#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# PipeB_watcher.sh  —  runs 24/7 on genomemaster; no SLURM slot consumed.
# Watches PipeA .done/ markers and submits a dedicated SLURM job
# (PipeB_script.sh) for each new UUID that is ready for downsampling.
#
# Usage: bash PipeB_watcher.sh [--dry-run]
# Or run persistently: nohup bash PipeB_watcher.sh >> /path/to/pipeB_watcher.log 2>&1 &
# ============================================================

# ── Configuration (edit here) ─────────────────────────────────────────────────
MANIFEST=/pedigree2/projects/TCGA_SARC/gdc_manifest.TCGA-SARC-WGS_tumor.2026-02-23.155715.txt
PIPA_DIR=/ribosome/projects/sapadmanabhan/TCGA_SARC/Outputs/Downloads
PIPB_DIR=/ribosome/projects/sapadmanabhan/TCGA_SARC/Outputs/Downsampled
PIPEB_SCRIPT=/ribosome/projects/sapadmanabhan/TCGA_SARC/Scripts/PipeB_script.sh
DOWNSAMPLE_SCRIPT=/ribosome/projects/sapadmanabhan/TCGA_SARC/Scripts/Downsample.py

DOWNSAMPLE_TARGET=10.0
NP=16
POLL_INTERVAL=30      # seconds between watcher cycles

# SLURM resource overrides (override the #SBATCH defaults in PipeB_script.sh)
SLURM_CPUS=16
SLURM_MEM=32gb
SLURM_TIME=4:00:00
SLURM_PARTITION=main
# ── End configuration ─────────────────────────────────────────────────────────

DRY_RUN=false
for arg in "$@"; do [[ "$arg" == "--dry-run" ]] && DRY_RUN=true; done

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] PipeB_watcher | $*"; }

mkdir -p "$PIPB_DIR"/{.locks,.done,.err,.tmp,.logs}

# Track which UUIDs we've already submitted (survives loop restarts within a run)
declare -A SUBMITTED

# Extract all UUIDs so we can report overall progress
mapfile -t ALL_UUIDS < <(grep -v '^id' "$MANIFEST" | awk 'NF{print $1}')
log "Loaded ${#ALL_UUIDS[@]} UUIDs from manifest."
$DRY_RUN && log "DRY-RUN mode — no jobs will be submitted."

# ── Main poll loop ─────────────────────────────────────────────────────────────
log "Watcher started — polling every ${POLL_INTERVAL}s."

while true; do
  n_submitted=0; n_done=0; n_err=0

  for uuid in "${ALL_UUIDS[@]}"; do
    a_done="$PIPA_DIR/.done/${uuid}.done"
    b_done="$PIPB_DIR/.done/${uuid}.done"
    b_err="$PIPB_DIR/.err/${uuid}.err"

    # PipeB already complete for this UUID
    if [[ -f "$b_done" ]]; then
      n_done=$(( n_done + 1 ))
      continue
    fi

    # PipeB permanently failed
    if [[ -f "$b_err" ]]; then
      n_err=$(( n_err + 1 ))
      continue
    fi

    # PipeA not finished yet
    [[ -f "$a_done" ]] || continue

    # Already submitted in this watcher session (don't double-submit)
    [[ -n "${SUBMITTED[$uuid]+set}" ]] && { n_submitted=$(( n_submitted + 1 )); continue; }

    # Resolve BAM path from PipeA .done marker
    bam_fp=$(grep '^bam:' "$a_done" | awk '{print $2}' | head -1)
    if [[ -z "$bam_fp" || ! -f "$bam_fp" ]]; then
      log "[WARN] $uuid: BAM path missing or file gone (PipeA .done: $a_done)"
      continue
    fi

    log "Submitting PipeB SLURM job for $uuid (BAM: $bam_fp)"

    if $DRY_RUN; then
      log "[DRY-RUN] Would sbatch: $PIPEB_SCRIPT $uuid $bam_fp $PIPB_DIR $DOWNSAMPLE_SCRIPT $DOWNSAMPLE_TARGET $NP"
      SUBMITTED[$uuid]=1
      n_submitted=$(( n_submitted + 1 ))
    else
      job_id=$(sbatch \
        --job-name="PipeB_${uuid:0:8}" \
        --cpus-per-task="$SLURM_CPUS" \
        --mem="$SLURM_MEM" \
        --time="$SLURM_TIME" \
        --partition="$SLURM_PARTITION" \
        --output="$PIPB_DIR/.logs/pipeB_${uuid:0:8}_%j.out" \
        --error="$PIPB_DIR/.logs/pipeB_${uuid:0:8}_%j.err" \
        "$PIPEB_SCRIPT" \
          "$uuid" \
          "$bam_fp" \
          "$PIPB_DIR" \
          "$DOWNSAMPLE_SCRIPT" \
          "$DOWNSAMPLE_TARGET" \
          "$NP" \
        | awk '{print $NF}')
      log "Submitted job $job_id for $uuid"
      SUBMITTED[$uuid]="$job_id"
      n_submitted=$(( n_submitted + 1 ))
    fi
  done

  log "Status — PipeB done: $n_done | errors: $n_err | in-flight/submitted: $n_submitted"

  # All accounted for
  if (( n_done + n_err == ${#ALL_UUIDS[@]} )); then
    log "All UUIDs finished (done=$n_done, errors=$n_err). Watcher exiting."
    exit 0
  fi

  sleep "$POLL_INTERVAL"
done
