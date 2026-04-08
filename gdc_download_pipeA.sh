#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# PipeA: GDC BAM downloader
# Usage: bash gdc_download_pipeA.sh <manifest> <token> <out_dir> [n_parallel]
#
# Reads UUIDs from a GDC manifest, downloads each BAM via gdc-client,
# validates, and writes a .done marker per sample.
# Safe to run from multiple nodes against the same out_dir (lock-based).
# ============================================================

manifest=$1
token=$2
out_dir=$3
n_parallel=${4:-4}

mkdir -p "$out_dir"/{.locks,.done,.err,.tmp}

lock_root="$out_dir/.locks"
done_root="$out_dir/.done"
err_root="$out_dir/.err"
tmp_root="$out_dir/.tmp"

CURRENT_LOCK=""
cleanup() {
  if [[ -n "${CURRENT_LOCK:-}" && -d "$CURRENT_LOCK" ]]; then
    rm -rf "$CURRENT_LOCK" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

# Extract UUIDs from manifest (skip header line starting with 'id')
mapfile -t UUIDS < <(grep -v '^id' "$manifest" | awk 'NF{print $1}')

echo "[INFO] PipeA starting — ${#UUIDS[@]} UUIDs from manifest"

for uuid in "${UUIDS[@]}"; do

  lock_path="$lock_root/${uuid}.lock"
  done_path="$done_root/${uuid}.done"
  err_path="$err_root/${uuid}.err"

  # Skip if already successfully downloaded
  if [[ -f "$done_path" ]]; then
    echo "[SKIP] $uuid (already done)"
    continue
  fi

  # Acquire distributed lock (atomic mkdir works across NFS)
  if mkdir "$lock_path" 2>/dev/null; then
    CURRENT_LOCK="$lock_path"
    {
      echo "stage: A_gdc_download"
      echo "host: $(hostname)"
      echo "pid: $$"
      echo "start: $(date -Is)"
      echo "uuid: $uuid"
      echo "output_dir: $out_dir/$uuid"
    } > "${lock_path}/meta"
  else
    echo "[LOCKED] $uuid (another worker downloading)"
    continue
  fi

  echo "[RUN] Downloading UUID: $uuid"

  # Build a single-entry mini-manifest for gdc-client
  mini_manifest="$tmp_root/${uuid}.manifest.$$"
  head -1 "$manifest" > "$mini_manifest"
  grep "^${uuid}" "$manifest" >> "$mini_manifest" || {
    echo "[ERROR] UUID $uuid not found in manifest"
    rm -f "$mini_manifest"
    rm -rf "$lock_path"; CURRENT_LOCK=""
    continue
  }

  dl_ok=false
  max_retries=3
  attempt=0

  while [[ $attempt -lt $max_retries ]]; do
    attempt=$(( attempt + 1 ))
    echo "[ATTEMPT $attempt/$max_retries] $uuid"

    # gdc-client downloads into out_dir/<uuid>/<filename>.bam
    if gdc-client download \
        -m "$mini_manifest" \
        -t "$token" \
        -n "$n_parallel" \
        -d "$out_dir"; then

      # Find the BAM that was placed under out_dir/<uuid>/
      bam_fp=$(find "$out_dir/$uuid" -maxdepth 1 -name "*.bam" | head -1)

      if [[ -z "$bam_fp" ]]; then
        echo "[WARN] No BAM found under $out_dir/$uuid (attempt $attempt)"
        continue
      fi

      # Validate: non-empty
      if [[ ! -s "$bam_fp" ]]; then
        echo "[WARN] BAM is empty: $bam_fp (attempt $attempt)"
        continue
      fi

      # Validate: BGZF EOF marker (last 28 bytes)
      eof_hex=$(tail -c 28 "$bam_fp" | xxd -p | tr -d '\n')
      expected_eof="1f8b08040000000000ff0600424302001b0003000000000000000000"
      if [[ "$eof_hex" != "$expected_eof" ]]; then
        echo "[WARN] BAM missing BGZF EOF marker: $bam_fp (attempt $attempt)"
        rm -rf "$out_dir/$uuid"
        continue
      fi

      dl_ok=true
      break
    else
      echo "[WARN] gdc-client failed for $uuid (attempt $attempt)"
      rm -rf "$out_dir/$uuid"
    fi
  done

  rm -f "$mini_manifest"

  if $dl_ok; then
    bam_fp=$(find "$out_dir/$uuid" -maxdepth 1 -name "*.bam" | head -1)
    bam_size=$(stat --format='%s' "$bam_fp")
    {
      cat "${lock_path}/meta"
      echo "finish: $(date -Is)"
      echo "status: success"
      echo "bam: $bam_fp"
      echo "size: $bam_size"
      echo "attempts: $attempt"
    } > "${done_path}.tmp.$$"
    mv -f "${done_path}.tmp.$$" "$done_path"
    rm -f "$err_path"
    rm -rf "$lock_path"; CURRENT_LOCK=""
    echo "[DONE] $uuid -> $bam_fp"
  else
    {
      cat "${lock_path}/meta" 2>/dev/null || true
      echo "finish: $(date -Is)"
      echo "status: error_after_${max_retries}_attempts"
    } > "${err_path}.tmp.$$"
    mv -f "${err_path}.tmp.$$" "$err_path"
    rm -rf "$lock_path"; CURRENT_LOCK=""
    echo "[ERROR] download failed after $max_retries attempts: $uuid"
  fi

done

echo "[INFO] PipeA complete."
