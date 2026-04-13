#!/usr/bin/env bash
#SBATCH --job-name=PipeC_AA
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=24
#SBATCH --mem=120gb
#SBATCH --time=36:00:00
#SBATCH --partition=main
#SBATCH --output=/ribosome/projects/sapadmanabhan/TCGA_SARC/Outputs/AmpliconSuite/.logs/pipeC_%j.out
#SBATCH --error=/ribosome/projects/sapadmanabhan/TCGA_SARC/Outputs/AmpliconSuite/.logs/pipeC_%j.err
#SBATCH --mail-type=FAIL,END

# ============================================================
# PipeC_script.sh  —  SLURM worker: run AmpliconSuite AA + AC for one sample.
# Submitted by PipeC_watcher.sh once PipeB .done marker is present.
#
# NOTE: Ask Jens whether this can run on genomemaster instead.
#       If yes: remove the #SBATCH headers above and call directly.
#       If no:  submit via sbatch as-is (--cpus-per-task=24 recommended).
#               For a minimal SLURM slot, pass np=1 and use --cpus-per-task=1
#               but AA/AC will be slow.
#
# Usage (SLURM):  sbatch PipeC_script.sh <uuid> <ds_bam> <out_dir> [np]
# Usage (direct): bash   PipeC_script.sh <uuid> <ds_bam> <out_dir> [np]
# ============================================================
set -euo pipefail

uuid=$1
ds_bam=$2
out_dir=$3
np=${4:-24}

CONDA_SH=/home/sapadmanabhan/miniconda3/etc/profile.d/conda.sh
CONDA_ENV=ampsuite
AMPSUITE=/home/sapadmanabhan/AmpliconSuite-pipeline/AmpliconSuite-pipeline.py
CNVKIT_DIR=/home/sapadmanabhan/miniconda3/envs/ampsuite/bin/

source "${CONDA_SH}"
conda activate "${CONDA_ENV}"

mkdir -p "$out_dir"/{.locks,.done,.err,.logs}

lock_root="$out_dir/.locks"
done_root="$out_dir/.done"
err_root="$out_dir/.err"

lock_path="$lock_root/${uuid}.lock"
done_path="$done_root/${uuid}.done"
err_path="$err_root/${uuid}.err"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] PipeC | $*"; }

sample_name=$(basename "$ds_bam" .DS.bam)
sample_outdir="$out_dir/$sample_name"

# ── Completion helpers ─────────────────────────────────────────────────────────
ampsuite_finished() {
  local sdir="$1" sname="$2"
  ls "${sdir}/${sname}"*"_summary.txt" 2>/dev/null | grep -q . && return 0
  grep -rql "Job finished\|Pipeline complete\|AmpliconSuite-pipeline complete" \
    "${sdir}" 2>/dev/null && return 0
  return 1
}

# Already done?
if [[ -f "$done_path" ]]; then
  log "[SKIP] $uuid already complete"
  exit 0
fi

# Acquire lock
if ! mkdir "$lock_path" 2>/dev/null; then
  log "[LOCKED] $uuid — another job is running AmpliconSuite"
  exit 0
fi

cleanup() { [[ -d "$lock_path" ]] && rm -rf "$lock_path" 2>/dev/null || true; }
trap cleanup EXIT INT TERM

{
  echo "stage: C_ampsuite"
  echo "host: $(hostname)"
  echo "job: ${SLURM_JOB_ID:-local}"
  echo "pid: $$"
  echo "start: $(date -Is)"
  echo "uuid: $uuid"
  echo "sample_name: $sample_name"
  echo "input: $ds_bam"
  echo "output_dir: $sample_outdir"
  echo "threads: $np"
} > "${lock_path}/meta"

mkdir -p "$sample_outdir"

log "Running AmpliconSuite: $sample_name"

if python "$AMPSUITE" \
    -s "$sample_name" \
    -t "$np" \
    -o "$sample_outdir" \
    --bam "$ds_bam" \
    --ref GRCh38 \
    --run_AA --run_AC \
    --cnvkit_dir "$CNVKIT_DIR" \
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
    log "[DONE] $uuid ($sample_name) -> $sample_outdir"
    exit 0
  else
    log "[ERROR] AmpliconSuite returned 0 but no completion marker found: $sample_name"
    {
      cat "${lock_path}/meta" 2>/dev/null || true
      echo "finish: $(date -Is)"
      echo "status: error_no_completion_marker"
    } > "${err_path}.tmp.$$"
    mv -f "${err_path}.tmp.$$" "$err_path"
    exit 1
  fi

else
  log "[ERROR] AmpliconSuite failed for $uuid ($sample_name)"
  {
    cat "${lock_path}/meta" 2>/dev/null || true
    echo "finish: $(date -Is)"
    echo "status: error_ampsuite_nonzero"
  } > "${err_path}.tmp.$$"
  mv -f "${err_path}.tmp.$$" "$err_path"
  exit 1
fi
