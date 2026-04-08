#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# PipeB: BAM downsampler — watches PipeA output, downsamples each BAM
# Usage: bash downsample_pipeB.sh <pipeA_out_dir> <out_dir> <downsample_script> [target_cov] [np]
#
# Polls pipeA_out_dir/.done/ for <uuid>.done markers.
# For each ready UUID: runs downsample_bam.py, writes .done marker.
# Deletes the full BAM after successful downsampling.
# Safe to run from multiple nodes (lock-based).
# ============================================================

pipeA_dir=$1           # PipeA out_dir
out_dir=$2             # PipeB out_dir (where .DS.bam files land)
downsample_script=$3   # path to downsample_bam.py
target_cov=${4:-10.0}  # target coverage
np=${5:-16}            # threads (passed to downsample script indirectly via samtools inside it)

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

process_one_uuid() {
  local uuid="$1"

  local lock_path="$lock_root/${uuid}.lock"
  local done_path="$done_root/${uuid}.done"
  local err_path="$err_root/${uuid}.err"

  # Skip if this UUID already downsampled
  if [[ -f "$done_path" ]]; then
    echo "[SKIP] $uuid (already downsampled)"
    return 0
  fi

  # Require PipeA .done marker before proceeding
  local a_done="$pipeA_dir/.done/${uuid}.done"
  if [[ ! -f "$a_done" ]]; then
    echo "[NOT READY] $uuid (waiting for PipeA .done)"
    return 0
  fi

  # Find the full BAM under pipeA_dir/<uuid>/
  local bam_fp
  bam_fp=$(find "$pipeA_dir/$uuid" -maxdepth 1 -name "*.bam" ! -name "*.DS.bam" | head -1)
  if [[ -z "$bam_fp" || ! -f "$bam_fp" ]]; then
    echo "[NOT READY] $uuid (BAM not found under $pipeA_dir/$uuid)"
    return 0
  fi

  # Acquire distributed lock
  if mkdir "$lock_path" 2>/dev/null; then
    CURRENT_LOCK="$lock_path"
    {
      echo "stage: B_downsample"
      echo "host: $(hostname)"
      echo "pid: $$"
      echo "start: $(date -Is)"
      echo "uuid: $uuid"
      echo "input: $bam_fp"
      echo "target_cov: $target_cov"
      echo "output_dir: $out_dir"
    } > "${lock_path}/meta"
  else
    echo "[LOCKED] $uuid (another worker downsampling)"
    return 0
  fi

  echo "[RUN] Downsampling $bam_fp -> ${out_dir}/ (target: ${target_cov}x)"

  # Index full BAM if missing
  if [[ ! -f "${bam_fp}.bai" ]]; then
    echo "[RUN] Indexing $bam_fp..."
    if ! samtools index "$bam_fp"; then
      echo "[ERROR] samtools index failed for $uuid"
      { cat "${lock_path}/meta" 2>/dev/null || true
        echo "finish: $(date -Is)"
        echo "status: error_index"
      } > "${err_path}.tmp.$$"
      mv -f "${err_path}.tmp.$$" "$err_path"
      rm -rf "$lock_path"; CURRENT_LOCK=""
      return 0
    fi
  fi

  # Run downsample — script writes <stem>.DS.bam into --downsample_dir
  if python "$downsample_script" \
      --bam "$bam_fp" \
      --final "$target_cov" \
      --downsample_dir "$out_dir"; then

    local stem
    stem=$(basename "$bam_fp" .bam)
    local ds_bam="$out_dir/${stem}.DS.bam"

    # The downsample script exits 0 but writes no DS.bam if already at/below target.
    # In that case, symlink/copy the original so PipeC always has a consistent path.
    if [[ ! -f "$ds_bam" ]]; then
      echo "[INFO] $uuid already at/below ${target_cov}x — linking original as DS.bam"
      cp "$bam_fp" "$ds_bam"
      samtools index "$ds_bam"
    fi

    {
      cat "${lock_path}/meta"
      echo "finish: $(date -Is)"
      echo "status: success"
      echo "ds_bam: $ds_bam"
    } > "${done_path}.tmp.$$"
    mv -f "${done_path}.tmp.$$" "$done_path"
    rm -f "$err_path"

    echo "[DEL] Downsampling succeeded — deleting full BAM: $bam_fp"
    rm -f "$bam_fp" "${bam_fp}.bai"

    rm -rf "$lock_path"; CURRENT_LOCK=""
    echo "[DONE] $uuid -> $ds_bam"

  else
    echo "[ERROR] downsample_bam.py failed for $uuid"
    {
      cat "${lock_path}/meta" 2>/dev/null || true
      echo "finish: $(date -Is)"
      echo "status: error_downsample"
    } > "${err_path}.tmp.$$"
    mv -f "${err_path}.tmp.$$" "$err_path"
    rm -rf "$lock_path"; CURRENT_LOCK=""
    return 0
  fi
}

# ── Poll loop ──────────────────────────────────────────────────────────────────
echo "[INFO] PipeB started — watching $pipeA_dir/.done/ ..."

while true; do
  shopt -s nullglob
  for done_marker in "$pipeA_dir/.done/"*.done; do
    uuid="$(basename "$done_marker" .done)"
    process_one_uuid "$uuid"
  done
  shopt -u nullglob
  sleep 5
done
