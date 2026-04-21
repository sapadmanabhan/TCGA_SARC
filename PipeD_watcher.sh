#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# PipeD_watcher.sh  —  runs 24/7 on genomemaster; no SLURM slot consumed.
# Watches PipeC .done/ markers and submits a SLURM job (PipeD_script.sh)
# for each sample ready for AA + AC. PipeD jobs are single-threaded, so
# many can run in parallel.
#
# Usage: bash PipeD_watcher.sh [--dry-run] [--genomemaster]
# Or run persistently: nohup bash PipeD_watcher.sh >> /path/to/pipeD_watcher.log 2>&1 &
# ============================================================

# ── Configuration (edit here) ─────────────────────────────────────────────────
MANIFEST=/pedigree2/projects/TCGA_SARC/gdc_manifest.TCGA-SARC-WGS_tumor.2026-02-23.155715.txt
PIPB_DIR=/ribosome/projects/sapadmanabhan/TCGA_SARC/Outputs/Downsampled
PIPC_DIR=/ribosome/projects/sapadmanabhan/TCGA_SARC/Outputs/AmpliconSuite
PIPD_DIR=/ribosome/projects/sapadmanabhan/TCGA_SARC/Outputs/AA_AC
PIPED_SCRIPT=/ribosome/projects/sapadmanabhan/TCGA_SARC/Pipeline/PipeD_script.sh

POLL_INTERVAL=30

# SLURM resource overrides (AA + AC are single-threaded — keep CPUs=1)
SLURM_CPUS=1
SLURM_MEM=64gb
SLURM_TIME=48:00:00
SLURM_PARTITION=main
# ── End configuration ─────────────────────────────────────────────────────────

DRY_RUN=false
GENOMEMASTER_MODE=false

for arg in "$@"; do
  [[ "$arg" == "--dry-run"      ]] && DRY_RUN=true
  [[ "$arg" == "--genomemaster" ]] && GENOMEMASTER_MODE=true
done

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] PipeD_watcher | $*"; }

mkdir -p "$PIPD_DIR"/{.locks,.done,.err,.logs}

declare -A SUBMITTED

mapfile -t ALL_UUIDS < <(grep -v '^id' "$MANIFEST" | awk 'NF{print $1}')
log "Loaded ${#ALL_UUIDS[@]} UUIDs from manifest."
$DRY_RUN           && log "DRY-RUN mode — nothing will be submitted or run."
$GENOMEMASTER_MODE && log "GENOMEMASTER mode — PipeD_script.sh runs locally (no sbatch)."
! $GENOMEMASTER_MODE && log "SLURM mode — PipeD_script.sh will be submitted via sbatch."

# ── Helper: resolve seeds BED from PipeC output ──────────────────────────────
# Pipe C emits CNV calls + seeds (typically <sample>_AA_CNV_SEEDS.bed).
# Falls back to any *AA_CNV_SEEDS.bed under the sample dir.
resolve_seeds() {
  local pipeC_sample_dir="$1" sname="$2"
  local seeds
  seeds=$(find "$pipeC_sample_dir" -maxdepth 3 \
            -name "${sname}*AA_CNV_SEEDS.bed" -type f 2>/dev/null | head -1)
  if [[ -z "$seeds" ]]; then
    seeds=$(find "$pipeC_sample_dir" -maxdepth 3 \
              -name "*AA_CNV_SEEDS.bed" -type f 2>/dev/null | head -1)
  fi
  echo "$seeds"
}

# ── Main poll loop ────────────────────────────────────────────────────────────
log "Watcher started — polling every ${POLL_INTERVAL}s."

while true; do
  n_submitted=0; n_done=0; n_err=0

  for uuid in "${ALL_UUIDS[@]}"; do
    c_done="$PIPC_DIR/.done/${uuid}.done"
    d_done="$PIPD_DIR/.done/${uuid}.done"
    d_err="$PIPD_DIR/.err/${uuid}.err"

    if [[ -f "$d_done" ]]; then
      n_done=$(( n_done + 1 ))
      continue
    fi

    if [[ -f "$d_err" ]]; then
      n_err=$(( n_err + 1 ))
      continue
    fi

    # PipeC not finished yet
    [[ -f "$c_done" ]] || continue

    # Already submitted/running
    [[ -n "${SUBMITTED[$uuid]+set}" ]] && { n_submitted=$(( n_submitted + 1 )); continue; }

    # Resolve DS BAM (from PipeB) and PipeC output dir + seeds
    b_done="$PIPB_DIR/.done/${uuid}.done"
    ds_bam=$(grep '^ds_bam:' "$b_done" 2>/dev/null | awk '{print $2}' | head -1)
    pipeC_sample_dir=$(grep '^sample_outdir:' "$c_done" | awk '{print $2}' | head -1)
    sample_name=$(grep '^sample_name:' "$c_done" | awk '{print $2}' | head -1)

    if [[ -z "$ds_bam" || ! -f "$ds_bam" ]]; then
      log "[WARN] $uuid: DS BAM missing (PipeB .done: $b_done)"
      continue
    fi
    if [[ -z "$sample_name" || -z "$pipeC_sample_dir" || ! -d "$pipeC_sample_dir" ]]; then
      log "[WARN] $uuid: PipeC output dir or sample_name missing (PipeC .done: $c_done)"
      continue
    fi

    seeds_bed=$(resolve_seeds "$pipeC_sample_dir" "$sample_name")
    if [[ -z "$seeds_bed" ]]; then
      log "[WARN] $uuid: could not find AA_CNV_SEEDS.bed under $pipeC_sample_dir"
      # Don't mark as error — Pipe C may legitimately produce no seeds.
      # We still submit PipeD with an empty seeds path so it records success_no_seeds.
      seeds_bed="${pipeC_sample_dir}/${sample_name}_AA_CNV_SEEDS.bed"
    fi

    log "Submitting PipeD for $uuid (sample: $sample_name, seeds: $seeds_bed)"

    if $DRY_RUN; then
      log "[DRY-RUN] Would run: PipeD_script.sh $uuid $ds_bam $seeds_bed $sample_name $pipeC_sample_dir $PIPD_DIR"
      SUBMITTED[$uuid]=1
      n_submitted=$(( n_submitted + 1 ))

    elif $GENOMEMASTER_MODE; then
      bash "$PIPED_SCRIPT" \
        "$uuid" "$ds_bam" "$seeds_bed" "$sample_name" "$pipeC_sample_dir" "$PIPD_DIR" \
        >> "$PIPD_DIR/.logs/pipeD_${uuid:0:8}.log" 2>&1 &
      SUBMITTED[$uuid]="local_pid_$!"
      n_submitted=$(( n_submitted + 1 ))
      log "Launched locally (PID $!) for $uuid"

    else
      job_id=$(sbatch \
        --job-name="PipeD_${uuid:0:8}" \
        --cpus-per-task="$SLURM_CPUS" \
        --mem="$SLURM_MEM" \
        --time="$SLURM_TIME" \
        --partition="$SLURM_PARTITION" \
        --output="$PIPD_DIR/.logs/pipeD_${uuid:0:8}_%j.out" \
        --error="$PIPD_DIR/.logs/pipeD_${uuid:0:8}_%j.err" \
        "$PIPED_SCRIPT" \
          "$uuid" \
          "$ds_bam" \
          "$seeds_bed" \
          "$sample_name" \
          "$pipeC_sample_dir" \
          "$PIPD_DIR" \
        | awk '{print $NF}')
      log "Submitted job $job_id for $uuid"
      SUBMITTED[$uuid]="$job_id"
      n_submitted=$(( n_submitted + 1 ))
    fi
  done

  log "Status — PipeD done: $n_done | errors: $n_err | in-flight/submitted: $n_submitted"

  if (( n_done + n_err == ${#ALL_UUIDS[@]} )); then
    log "All UUIDs finished (done=$n_done, errors=$n_err). Watcher exiting."
    exit 0
  fi

  sleep "$POLL_INTERVAL"
done
