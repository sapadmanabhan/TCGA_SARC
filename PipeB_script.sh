#!/usr/bin/env bash
#SBATCH --job-name=PipeB_ds
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=32gb
#SBATCH --time=4:00:00
#SBATCH --partition=main
#SBATCH --output=/ribosome/projects/sapadmanabhan/TCGA_SARC/Outputs/Downsampled/.logs/pipeB_%j.out
#SBATCH --error=/ribosome/projects/sapadmanabhan/TCGA_SARC/Outputs/Downsampled/.logs/pipeB_%j.err
#SBATCH --mail-type=FAIL

# ============================================================
# PipeB_script.sh  —  SLURM worker: downsample one BAM to target coverage.
# Submitted by PipeB_watcher.sh once PipeA .done marker is present.
#
# Usage (direct):  bash PipeB_script.sh <uuid> <bam_fp> <out_dir> <downsample_script> [target_cov] [np]
# Usage (SLURM):   sbatch PipeB_script.sh <uuid> <bam_fp> <out_dir> <downsample_script> [target_cov] [np]
# ============================================================
set -euo pipefail

uuid=$1
bam_fp=$2
out_dir=$3
downsample_script=$4
target_cov=${5:-10.0}
np=${6:-16}

CONDA_SH=/home/sapadmanabhan/miniconda3/etc/profile.d/conda.sh
CONDA_ENV=ampsuite

source "${CONDA_SH}"
conda activate "${CONDA_ENV}"

mkdir -p "$out_dir"/{.locks,.done,.err,.tmp,.logs}

lock_root="$out_dir/.locks"
done_root="$out_dir/.done"
err_root="$out_dir/.err"

lock_path="$lock_root/${uuid}.lock"
done_path="$done_root/${uuid}.done"
err_path="$err_root/${uuid}.err"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] PipeB | $*"; }

# Already done? (e.g. job re-queued)
if [[ -f "$done_path" ]]; then
  log "[SKIP] $uuid already downsampled"
  exit 0
fi

# Acquire lock
if ! mkdir "$lock_path" 2>/dev/null; then
  log "[LOCKED] $uuid — another job is downsampling"
  exit 0
fi

cleanup() { [[ -d "$lock_path" ]] && rm -rf "$lock_path" 2>/dev/null || true; }
trap cleanup EXIT INT TERM

{
  echo "stage: B_downsample"
  echo "host: $(hostname)"
  echo "job: ${SLURM_JOB_ID:-local}"
  echo "pid: $$"
  echo "start: $(date -Is)"
  echo "uuid: $uuid"
  echo "input: $bam_fp"
  echo "target_cov: $target_cov"
  echo "output_dir: $out_dir"
} > "${lock_path}/meta"

# ── Index BAM if missing ────────────────────────────────────────────────────
if [[ ! -f "${bam_fp}.bai" ]]; then
  log "Indexing $bam_fp..."
  samtools index -@ "$np" "$bam_fp" || {
    log "[ERROR] samtools index failed for $uuid"
    { cat "${lock_path}/meta" 2>/dev/null || true
      echo "finish: $(date -Is)"
      echo "status: error_index"
    } > "${err_path}.tmp.$$"
    mv -f "${err_path}.tmp.$$" "$err_path"
    exit 1
  }
fi

# ── Downsample ────────────────────────────────────────────────────────────────
log "Downsampling $bam_fp -> $out_dir (target: ${target_cov}x)"

if python "$downsample_script" \
    --bam "$bam_fp" \
    --final "$target_cov" \
    --downsample_dir "$out_dir"; then

  stem=$(basename "$bam_fp" .bam)
  ds_bam="$out_dir/${stem}.DS.bam"

  # If script exited 0 but wrote no DS.bam the sample was already ≤ target — copy as-is
  if [[ ! -f "$ds_bam" ]]; then
    log "$uuid already at/below ${target_cov}x — linking original as DS.bam"
    cp "$bam_fp" "$ds_bam"
    samtools index -@ "$np" "$ds_bam"
  fi

  {
    cat "${lock_path}/meta"
    echo "finish: $(date -Is)"
    echo "status: success"
    echo "ds_bam: $ds_bam"
  } > "${done_path}.tmp.$$"
  mv -f "${done_path}.tmp.$$" "$done_path"
  rm -f "$err_path"

  log "[DEL] Removing full BAM: $bam_fp"
  rm -f "$bam_fp" "${bam_fp}.bai"

  log "[DONE] $uuid -> $ds_bam"
  exit 0

else
  log "[ERROR] downsample_bam.py failed for $uuid"
  {
    cat "${lock_path}/meta" 2>/dev/null || true
    echo "finish: $(date -Is)"
    echo "status: error_downsample"
  } > "${err_path}.tmp.$$"
  mv -f "${err_path}.tmp.$$" "$err_path"
  exit 1
fi
