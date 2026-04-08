#!/bin/bash
#SBATCH -J TCGA_Batch
#SBATCH --mail-type=FAIL,END
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=24
#SBATCH --time=150:00:00   # 5 samples x ~25-30hrs each; tune once you have runtime data
#SBATCH --mem=120gb         # AA+AC can spike >60GB; 120GB is safer for sequential runs
#SBATCH --output=job_%j.out
#SBATCH --error=job_%j.err
#SBATCH --partition=main

# ─────────────────────────────────────────────────────────────────────────────
# USER CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
MANIFEST=/pedigree2/projects/TCGA_SARC/gdc_manifest.TCGA-SARC-WGS_tumor.2026-02-23.155715.txt
TOKEN=/ribosome/projects/sapadmanabhan/TCGA_SARC/Scripts/token.txt
N_PARALLEL=4                    # gdc-client parallel streams

# Root dirs — each pipe writes into its own subdirectory
BASE=/ribosome/projects/sapadmanabhan/TCGA_SARC/Outputs
PIPA_DIR="$BASE/Downloads"      # PipeA output (raw BAMs land here)
PIPB_DIR="$BASE/Downsampled"    # PipeB output (.DS.bam files land here)
PIPC_DIR="$BASE/Outputs"        # PipeC output (AmpliconSuite results per sample)

PIPEA_SCRIPT=/ribosome/projects/sapadmanabhan/TCGA_SARC/Scripts/gdc_download_pipeA.sh
PIPEB_SCRIPT=/ribosome/projects/sapadmanabhan/TCGA_SARC/Scripts/downsample_pipeB.sh
PIPEC_SCRIPT=/ribosome/projects/sapadmanabhan/TCGA_SARC/Scripts/ampsuite_pipeC.sh

DOWNSAMPLE_SCRIPT=/ribosome/projects/sapadmanabhan/TCGA_SARC/Scripts/Downsample.py
DOWNSAMPLE_TARGET=10.0

AMPSUITE=/home/sapadmanabhan/AmpliconSuite-pipeline/AmpliconSuite-pipeline.py

CONDA_SH=/home/sapadmanabhan/miniconda3/etc/profile.d/conda.sh
CONDA_ENV=ampsuite

NP=24
POLL_INTERVAL=30    # seconds between completion checks

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }
die() { log "ERROR: $*" >&2; exit 1; }

# ─────────────────────────────────────────────────────────────────────────────
# ENVIRONMENT
# ─────────────────────────────────────────────────────────────────────────────
log "Node: $(hostname) | Job: $SLURM_JOB_ID"

source "${CONDA_SH}" || die "Could not source conda at ${CONDA_SH}"
conda activate "${CONDA_ENV}" || die "Could not activate conda env: ${CONDA_ENV}"

mkdir -p "$PIPA_DIR" "$PIPB_DIR" "$PIPC_DIR"

# ─────────────────────────────────────────────────────────────────────────────
# UUIDS — passed as positional args by submit_all_batches.sh
# ─────────────────────────────────────────────────────────────────────────────
UUIDS=("$@")
[[ ${#UUIDS[@]} -gt 0 ]] || die "No UUIDs supplied. Pass UUIDs as positional arguments."
log "Batch size: ${#UUIDS[@]} | UUIDs: ${UUIDS[*]}"

# Build a mini-manifest for just this batch's UUIDs
MINI_MANIFEST="$PIPA_DIR/.tmp/mini_manifest_${SLURM_JOB_ID}.txt"
mkdir -p "$(dirname "$MINI_MANIFEST")"
head -1 "$MANIFEST" > "$MINI_MANIFEST"
for uuid in "${UUIDS[@]}"; do
  grep "^${uuid}" "$MANIFEST" >> "$MINI_MANIFEST" \
    || log "WARNING: UUID $uuid not found in manifest — it will be skipped by PipeA"
done

# ─────────────────────────────────────────────────────────────────────────────
# LAUNCH PIPES AS BACKGROUND PROCESSES
#
# PipeA runs once through all UUIDs and exits.
# PipeB and PipeC run in poll loops and are terminated once all UUIDs are done.
# ─────────────────────────────────────────────────────────────────────────────
log "=== Launching PipeA (GDC download) ==="
bash "$PIPEA_SCRIPT" "$MINI_MANIFEST" "$TOKEN" "$PIPA_DIR" "$N_PARALLEL" &
PIPEA_PID=$!

log "=== Launching PipeB (downsampler watcher) ==="
bash "$PIPEB_SCRIPT" "$PIPA_DIR" "$PIPB_DIR" "$DOWNSAMPLE_SCRIPT" "$DOWNSAMPLE_TARGET" "$NP" &
PIPEB_PID=$!

log "=== Launching PipeC (AmpliconSuite watcher) ==="
bash "$PIPEC_SCRIPT" "$PIPB_DIR" "$PIPC_DIR" "$AMPSUITE" "$NP" &
PIPEC_PID=$!

# Ensure all background pipes are killed if this script exits unexpectedly
cleanup_pipes() {
  log "Cleaning up background pipe processes..."
  kill "$PIPEA_PID" "$PIPEB_PID" "$PIPEC_PID" 2>/dev/null || true
}
trap cleanup_pipes EXIT INT TERM

# ─────────────────────────────────────────────────────────────────────────────
# WAIT: poll until every UUID has a PipeC .done or .err marker
# ─────────────────────────────────────────────────────────────────────────────
log "=== Waiting for all ${#UUIDS[@]} UUIDs to complete PipeC ==="

while true; do
  all_done=true
  n_done=0; n_err=0; n_pending=0

  for uuid in "${UUIDS[@]}"; do
    if [[ -f "$PIPC_DIR/.done/${uuid}.done" ]]; then
      n_done=$(( n_done + 1 ))
    elif [[ -f "$PIPC_DIR/.err/${uuid}.err" ]]; then
      n_err=$(( n_err + 1 ))
      log "WARNING: PipeC reported error for $uuid — check $PIPC_DIR/.err/${uuid}.err"
    else
      n_pending=$(( n_pending + 1 ))
      all_done=false
    fi
  done

  log "Status — done: $n_done | errors: $n_err | pending: $n_pending"

  $all_done && break

  sleep "$POLL_INTERVAL"
done

# ─────────────────────────────────────────────────────────────────────────────
# SHUTDOWN
# ─────────────────────────────────────────────────────────────────────────────
log "All UUIDs finished. Stopping pipe watchers."
kill "$PIPEA_PID" "$PIPEB_PID" "$PIPEC_PID" 2>/dev/null || true
wait "$PIPEA_PID" "$PIPEB_PID" "$PIPEC_PID" 2>/dev/null || true
trap - EXIT INT TERM

# Summary
n_success=0; n_fail=0
for uuid in "${UUIDS[@]}"; do
  [[ -f "$PIPC_DIR/.done/${uuid}.done" ]] && n_success=$(( n_success + 1 )) || n_fail=$(( n_fail + 1 ))
done

log "=== Batch complete — success: $n_success | failed: $n_fail ==="
[[ $n_fail -gt 0 ]] && exit 1 || exit 0
