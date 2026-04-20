#!/usr/bin/env bash
#SBATCH --job-name=PipeD_AAAC
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=64gb
#SBATCH --time=48:00:00
#SBATCH --partition=main
#SBATCH --output=/ribosome/projects/sapadmanabhan/TCGA_SARC/Outputs/AA_AC/.logs/pipeD_%j.out
#SBATCH --error=/ribosome/projects/sapadmanabhan/TCGA_SARC/Outputs/AA_AC/.logs/pipeD_%j.err
#SBATCH --mail-type=FAIL,END

# ============================================================
# PipeD_script.sh  —  SLURM worker: run AmpliconArchitect (AA) + AmpliconClassifier (AC)
# on a single sample's seeds + DS BAM produced by Pipe C.
#
# Per Jens' note: AA and AC are serial (1 thread), so this stage uses
# --cpus-per-task=1. Many samples run in parallel as separate SLURM jobs.
#
# Submitted by PipeD_watcher.sh once PipeC .done marker is present.
#
# Usage (SLURM):  sbatch PipeD_script.sh <uuid> <ds_bam> <seeds_bed> <sample_name> <pipeC_outdir> <out_dir>
# Usage (direct): bash   PipeD_script.sh <uuid> <ds_bam> <seeds_bed> <sample_name> <pipeC_outdir> <out_dir>
# ============================================================
set -euo pipefail

uuid=$1
ds_bam=$2
seeds_bed=$3
sample_name=$4
pipeC_outdir=$5   # AA_seeds lives under here from Pipe C
out_dir=$6

CONDA_SH=/home/sapadmanabhan/miniconda3/etc/profile.d/conda.sh
CONDA_ENV=ampsuite
AMPSUITE=/home/sapadmanabhan/AmpliconSuite-pipeline/AmpliconSuite-pipeline.py

source "${CONDA_SH}"
conda activate "${CONDA_ENV}"

mkdir -p "$out_dir"/{.locks,.done,.err,.logs}

lock_root="$out_dir/.locks"
done_root="$out_dir/.done"
err_root="$out_dir/.err"

lock_path="$lock_root/${uuid}.lock"
done_path="$done_root/${uuid}.done"
err_path="$err_root/${uuid}.err"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] PipeD | $*"; }

sample_outdir="$out_dir/$sample_name"

# ── Completion helpers ─────────────────────────────────────────────────────────
# AA writes *_summary.txt and *_graph.txt per amplicon; AC writes *_result_table.tsv
aa_ac_finished() {
  local sdir="$1" sname="$2"
  # Either AA found no amplicons (valid outcome) OR AC wrote its result table
  ls "${sdir}/${sname}"*"_result_table.tsv"    2>/dev/null | grep -q . && return 0
  ls "${sdir}/${sname}"*"_classification.tsv"  2>/dev/null | grep -q . && return 0
  # AA-only completion marker (some samples have no amplicons to classify)
  grep -rql "No amplicons found\|Pipeline complete\|AA complete" \
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
  log "[LOCKED] $uuid — another job is running AA+AC"
  exit 0
fi

cleanup() { [[ -d "$lock_path" ]] && rm -rf "$lock_path" 2>/dev/null || true; }
trap cleanup EXIT INT TERM

{
  echo "stage: D_aa_ac"
  echo "host: $(hostname)"
  echo "job: ${SLURM_JOB_ID:-local}"
  echo "pid: $$"
  echo "start: $(date -Is)"
  echo "uuid: $uuid"
  echo "sample_name: $sample_name"
  echo "input_bam: $ds_bam"
  echo "input_seeds: $seeds_bed"
  echo "pipeC_outdir: $pipeC_outdir"
  echo "output_dir: $sample_outdir"
} > "${lock_path}/meta"

mkdir -p "$sample_outdir"

# ── Guard: seeds file must exist & be non-empty ───────────────────────────────
if [[ ! -s "$seeds_bed" ]]; then
  log "[SKIP-NO-SEEDS] $uuid has empty/missing seeds ($seeds_bed) — nothing to amplify"
  {
    cat "${lock_path}/meta"
    echo "finish: $(date -Is)"
    echo "status: success_no_seeds"
    echo "sample_outdir: $sample_outdir"
    echo "note: PipeC produced no seeds; AA+AC skipped"
  } > "${done_path}.tmp.$$"
  mv -f "${done_path}.tmp.$$" "$done_path"
  rm -f "$err_path"
  exit 0
fi

log "Running AA + AC on $sample_name (1 thread; seeds: $seeds_bed)"

# Run AA + AC via the AmpliconSuite wrapper. --run_AA --run_AC with no --run_CNV,
# and passing the seeds file skips the CN step entirely — PipeC already did it.
if python "$AMPSUITE" \
    -s "$sample_name" \
    -t 1 \
    -o "$sample_outdir" \
    --bam "$ds_bam" \
    --bed "$seeds_bed" \
    --ref GRCh38 \
    --run_AA --run_AC \
    &> "${sample_outdir}/job_out.txt"; then

  if aa_ac_finished "$sample_outdir" "$sample_name"; then
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
    log "[ERROR] AA+AC returned 0 but no completion marker found: $sample_name"
    {
      cat "${lock_path}/meta" 2>/dev/null || true
      echo "finish: $(date -Is)"
      echo "status: error_no_completion_marker"
    } > "${err_path}.tmp.$$"
    mv -f "${err_path}.tmp.$$" "$err_path"
    exit 1
  fi

else
  log "[ERROR] AA+AC failed for $uuid ($sample_name)"
  {
    cat "${lock_path}/meta" 2>/dev/null || true
    echo "finish: $(date -Is)"
    echo "status: error_aa_ac_nonzero"
  } > "${err_path}.tmp.$$"
  mv -f "${err_path}.tmp.$$" "$err_path"
  exit 1
fi
