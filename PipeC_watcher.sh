#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# PipeC_watcher.sh  —  runs 24/7 on genomemaster; no SLURM slot consumed.
# Watches PipeB .done/ markers and submits a SLURM job (PipeC_script.sh)
# for each sample ready for AmpliconSuite.
#
# *** Check with Jens first ***
# If AmpliconSuite can run directly on genomemaster, change the submit
# section below to call PipeC_script.sh directly (background process)
# instead of sbatch-ing it.  The GENOMEMASTER_MODE variable controls this.
#
# Usage: bash PipeC_watcher.sh [--dry-run] [--genomemaster]
# Or run persistently: nohup bash PipeC_watcher.sh >> /path/to/pipeC_watcher.log 2>&1 &
# ============================================================

# ── Configuration (edit here) ─────────────────────────────────────────────────
MANIFEST=/pedigree2/projects/TCGA_SARC/gdc_manifest.TCGA-SARC-WGS_tumor.2026-02-23.155715.txt
PIPB_DIR=/ribosome/projects/sapadmanabhan/TCGA_SARC/Outputs/Downsampled
PIPC_DIR=/ribosome/projects/sapadmanabhan/TCGA_SARC/Outputs/AmpliconSuite
PIPEC_SCRIPT=/ribosome/projects/sapadmanabhan/TCGA_SARC/Scripts/PipeC_script.sh

NP=24
POLL_INTERVAL=30      # seconds between watcher cycles

# SLURM resource overrides (if running via sbatch)
SLURM_CPUS=24
SLURM_MEM=120gb
SLURM_TIME=36:00:00
SLURM_PARTITION=main
# ── End configuration ─────────────────────────────────────────────────────────

DRY_RUN=false
GENOMEMASTER_MODE=false   # set true if Jens confirms genomemaster is OK for AA+AC

for arg in "$@"; do
  [[ "$arg" == "--dry-run"      ]] && DRY_RUN=true
  [[ "$arg" == "--genomemaster" ]] && GENOMEMASTER_MODE=true
done

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] PipeC_watcher | $*"; }

mkdir -p "$PIPC_DIR"/{.locks,.done,.err,.logs}

declare -A SUBMITTED

mapfile -t ALL_UUIDS < <(grep -v '^id' "$MANIFEST" | awk 'NF{print $1}')
log "Loaded ${#ALL_UUIDS[@]} UUIDs from manifest."
$DRY_RUN          && log "DRY-RUN mode — nothing will be submitted or run."
$GENOMEMASTER_MODE && log "GENOMEMASTER mode — PipeC_script.sh runs locally (no sbatch)."
! $GENOMEMASTER_MODE && log "SLURM mode — PipeC_script.sh will be submitted via sbatch."

# ── Main poll loop ─────────────────────────────────────────────────────────────
log "Watcher started — polling every ${POLL_INTERVAL}s."

while true; do
  n_submitted=0; n_done=0; n_err=0

  for uuid in "${ALL_UUIDS[@]}"; do
    b_done="$PIPB_DIR/.done/${uuid}.done"
    c_done="$PIPC_DIR/.done/${uuid}.done"
    c_err="$PIPC_DIR/.err/${uuid}.err"

    if [[ -f "$c_done" ]]; then
      n_done=$(( n_done + 1 ))
      continue
    fi

    if [[ -f "$c_err" ]]; then
      n_err=$(( n_err + 1 ))
      continue
    fi

    # PipeB not finished yet
    [[ -f "$b_done" ]] || continue

    # Already submitted/running
    [[ -n "${SUBMITTED[$uuid]+set}" ]] && { n_submitted=$(( n_submitted + 1 )); continue; }

    # Resolve DS BAM path from PipeB .done marker
    ds_bam=$(grep '^ds_bam:' "$b_done" | awk '{print $2}' | head -1)
    if [[ -z "$ds_bam" || ! -f "$ds_bam" ]]; then
      log "[WARN] $uuid: DS BAM path missing or file gone (PipeB .done: $b_done)"
      continue
    fi

    log "Submitting PipeC for $uuid (DS BAM: $ds_bam)"

    if $DRY_RUN; then
      log "[DRY-RUN] Would run: PipeC_script.sh $uuid $ds_bam $PIPC_DIR $NP"
      SUBMITTED[$uuid]=1
      n_submitted=$(( n_submitted + 1 ))

    elif $GENOMEMASTER_MODE; then
      # Run directly on genomemaster (confirm with Jens before enabling)
      bash "$PIPEC_SCRIPT" "$uuid" "$ds_bam" "$PIPC_DIR" "$NP" \
        >> "$PIPC_DIR/.logs/pipeC_${uuid:0:8}.log" 2>&1 &
      SUBMITTED[$uuid]="local_pid_$!"
      n_submitted=$(( n_submitted + 1 ))
      log "Launched locally (PID $!) for $uuid"

    else
      # Submit to SLURM
      job_id=$(sbatch \
        --job-name="PipeC_${uuid:0:8}" \
        --cpus-per-task="$SLURM_CPUS" \
        --mem="$SLURM_MEM" \
        --time="$SLURM_TIME" \
        --partition="$SLURM_PARTITION" \
        --output="$PIPC_DIR/.logs/pipeC_${uuid:0:8}_%j.out" \
        --error="$PIPC_DIR/.logs/pipeC_${uuid:0:8}_%j.err" \
        "$PIPEC_SCRIPT" \
          "$uuid" \
          "$ds_bam" \
          "$PIPC_DIR" \
          "$NP" \
        | awk '{print $NF}')
      log "Submitted job $job_id for $uuid"
      SUBMITTED[$uuid]="$job_id"
      n_submitted=$(( n_submitted + 1 ))
    fi
  done

  log "Status — PipeC done: $n_done | errors: $n_err | in-flight/submitted: $n_submitted"

  if (( n_done + n_err == ${#ALL_UUIDS[@]} )); then
    log "All UUIDs finished (done=$n_done, errors=$n_err). Watcher exiting."
    exit 0
  fi

  sleep "$POLL_INTERVAL"
done
