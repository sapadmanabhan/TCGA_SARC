#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# PipeC: AmpliconSuite runner — watches PipeB output, runs AA+AC per sample
# Usage: bash ampsuite_pipeC.sh <pipeB_out_dir> <out_dir> <ampsuite_script> [np]
#
# Polls pipeB_out_dir/.done/ for <uuid>.done markers.
# For each ready UUID: runs AmpliconSuite-pipeline.py on the .DS.bam.
# Writes .done marker on success.
# Safe to run from multiple nodes (lock-based).
# ============================================================

pipeB_dir=$1          # PipeB out_dir (contains .DS.bam files and .done/ markers)
out_dir=$2            # PipeC out_dir (AmpliconSuite per-sample output dirs land here)
ampsuite_script=$3    # path to AmpliconSuite-pipeline.py
np=${4:-24}           # threads

mkdir -p "$out_dir"/{.locks,.done,.err}

lock_root="$out_dir/.locks"
done_root="$out_dir/.done"
err_root="$out_dir/.err"

CURRENT_LOCK=""
cleanup() {
  if [[ -n "${CURRENT_LOCK:-}" && -d "$CURRENT_LOCK" ]]; then
    rm -rf "$CURRENT_LOCK" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

# ── Finish detection ───────────────────────────────────────────────────────────
# Checks whether AmpliconSuite completed successfully for a given sample.
# Adjust the grep pattern if your AA version writes a different finish string.
ampsuite_finished() {
  local sample_outdir="$1"
  local sample_name="$2"
  # Primary: look for a _summary.txt (produced by AC on completion)
  if ls "${sample_outdir}/${sample_name}"*"_summary.txt" 2>/dev/null | grep -q .; then
    return 0
  fi
  # Fallback: scan any .log for a finish string
  if grep -rql "Job finished\|Pipeline complete\|AmpliconSuite-pipeline complete" \
      "${sample_outdir}" 2>/dev/null; then
    return 0
  fi
  return 1
}

process_one_uuid() {
  local uuid="$1"

  local lock_path="$lock_root/${uuid}.lock"
  local done_path="$done_root/${uuid}.done"
  local err_path="$err_root/${uuid}.err"

  # Skip if AmpliconSuite already ran for this UUID
  if [[ -f "$done_path" ]]; then
    echo "[SKIP] $uuid (already done)"
    return 0
  fi

  # Require PipeB .done marker
  local b_done="$pipeB_dir/.done/${uuid}.done"
  if [[ ! -f "$b_done" ]]; then
    echo "[NOT READY] $uuid (waiting for PipeB .done)"
    return 0
  fi

  # Derive sample name and DS BAM path from PipeB done marker
  # PipeB records: "ds_bam: /path/to/<stem>.DS.bam"
  local ds_bam
  ds_bam=$(grep '^ds_bam:' "$b_done" | awk '{print $2}' | head -1)

  if [[ -z "$ds_bam" || ! -f "$ds_bam" ]]; then
    # Fallback: scan pipeB_dir directly
    ds_bam=$(find "$pipeB_dir" -maxdepth 1 -name "*.DS.bam" | head -1)
  fi

  if [[ -z "$ds_bam" || ! -f "$ds_bam" ]]; then
    echo "[NOT READY] $uuid (DS BAM not found — PipeB may not have finished writing)"
    return 0
  fi

  local sample_name
  sample_name=$(basename "$ds_bam" .DS.bam)   # e.g. TCGA-DX-A1KW-01A

  # Acquire distributed lock
  if mkdir "$lock_path" 2>/dev/null; then
    CURRENT_LOCK="$lock_path"
    {
      echo "stage: C_ampsuite"
      echo "host: $(hostname)"
      echo "pid: $$"
      echo "start: $(date -Is)"
      echo "uuid: $uuid"
      echo "sample_name: $sample_name"
      echo "input: $ds_bam"
      echo "output_dir: $out_dir/$sample_name"
      echo "threads: $np"
    } > "${lock_path}/meta"
  else
    echo "[LOCKED] $uuid (another worker running AmpliconSuite)"
    return 0
  fi

  local sample_outdir="$out_dir/$sample_name"
  mkdir -p "$sample_outdir"

  echo "[RUN] AmpliconSuite: $sample_name ($ds_bam)"

AS=/home/sapadmanabhan/AmpliconSuite-pipeline/AmpliconSuite-pipeline.py

  if python $AS \
	-s “${sample_name}" -t ${np} -o "${sample_outdir}" \
	--bam $ds_bam\
	--ref GRCh38 \
        --run_AA --run_AC \
	--cnvkit_dir /home/sapadmanabhan/miniconda3/envs/ampsuite/bin/ \
	&> "${sample_outdir}/job_out.txt"; then

    if ampsuite_finished "$sample_outdir" "$sample_name"; then
      {
        cat "${lock_path}/meta"
        echo "finish: $(date -Is)"
        echo "status: success"
        echo "sample_outdir: $sample_outdir"
      } > "${done_path}.tmp.$$"
      mv -f "${done_path}.tmp.$$" "$done_path"
      rm -f "$err_path"
      rm -rf "$lock_path"; CURRENT_LOCK=""
      echo "[DONE] $uuid ($sample_name)"
    else
      # Process returned 0 but completion markers are absent — treat as error
      echo "[ERROR] AmpliconSuite returned 0 but no completion marker found: $sample_name"
      {
        cat "${lock_path}/meta" 2>/dev/null || true
        echo "finish: $(date -Is)"
        echo "status: error_no_completion_marker"
      } > "${err_path}.tmp.$$"
      mv -f "${err_path}.tmp.$$" "$err_path"
      rm -rf "$lock_path"; CURRENT_LOCK=""
    fi

  else
    echo "[ERROR] AmpliconSuite failed for $uuid ($sample_name)"
    {
      cat "${lock_path}/meta" 2>/dev/null || true
      echo "finish: $(date -Is)"
      echo "status: error_ampsuite_nonzero"
    } > "${err_path}.tmp.$$"
    mv -f "${err_path}.tmp.$$" "$err_path"
    rm -rf "$lock_path"; CURRENT_LOCK=""
    return 0
  fi
}

# ── Poll loop ──────────────────────────────────────────────────────────────────
echo "[INFO] PipeC started — watching $pipeB_dir/.done/ ..."

while true; do
  shopt -s nullglob
  for done_marker in "$pipeB_dir/.done/"*.done; do
    uuid="$(basename "$done_marker" .done)"
    process_one_uuid "$uuid"
  done
  shopt -u nullglob
  sleep 5
done
