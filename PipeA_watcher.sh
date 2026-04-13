#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# PipeA_watcher.sh  —  runs 24/7 on genomemaster; no SLURM slot consumed.
# Polls the manifest for pending UUIDs and calls PipeA_script.sh
# directly (download is I/O-bound, not CPU-bound).
#
# Cache management: only starts a new download when the number of
# BAM directories currently in PIPA_DIR is < MAX_CACHED_BAMS (default 5).
# This prevents filling the scratch disk.
#
# Usage: bash PipeA_watcher.sh [--dry-run]
# Or run persistently: nohup bash PipeA_watcher.sh >> /path/to/pipeA_watcher.log 2>&1 &
# ============================================================

# ── Configuration (edit here) ─────────────────────────────────────────────────
MANIFEST=/pedigree2/projects/TCGA_SARC/gdc_manifest.TCGA-SARC-WGS_tumor.2026-02-23.155715.txt
TOKEN=/ribosome/projects/sapadmanabhan/TCGA_SARC/Scripts/token.txt
PIPA_DIR=/ribosome/projects/sapadmanabhan/TCGA_SARC/Outputs/Downloads
PIPEA_SCRIPT=/ribosome/projects/sapadmanabhan/TCGA_SARC/Scripts/PipeA_script.sh

N_PARALLEL=4          # gdc-client parallel streams per download
MAX_CACHED_BAMS=5     # max BAMs sitting in PIPA_DIR before we pause downloading
POLL_INTERVAL=30      # seconds between watcher cycles
# ── End configuration ─────────────────────────────────────────────────────────

DRY_RUN=false
for arg in "$@"; do [[ "$arg" == "--dry-run" ]] && DRY_RUN=true; done

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] PipeA_watcher | $*"; }

mkdir -p "$PIPA_DIR"/{.locks,.done,.err,.tmp}

# Extract all UUIDs from manifest (skip header)
mapfile -t ALL_UUIDS < <(grep -v '^id' "$MANIFEST" | awk 'NF{print $1}')
log "Loaded ${#ALL_UUIDS[@]} UUIDs from manifest."
log "Cache cap: $MAX_CACHED_BAMS BAMs in $PIPA_DIR"
$DRY_RUN && log "DRY-RUN mode — downloads will not actually run."

# ── Helpers ────────────────────────────────────────────────────────────────────

# Count BAM files currently in the download dir (excludes .DS.bam to avoid
# double-counting if PipeB hasn't cleaned up yet).
count_cached_bams() {
  find "$PIPA_DIR" -maxdepth 2 -name "*.bam" ! -name "*.DS.bam" 2>/dev/null | wc -l
}

# Count actively running PipeA_script processes for this watcher's PIPA_DIR
count_active_downloads() {
  pgrep -fc "PipeA_script.sh.*${PIPA_DIR}" 2>/dev/null || true
}

# ── Main poll loop ─────────────────────────────────────────────────────────────
log "Watcher started — polling every ${POLL_INTERVAL}s."

while true; do
  n_done=0; n_err=0; n_pending=0

  for uuid in "${ALL_UUIDS[@]}"; do
    done_path="$PIPA_DIR/.done/${uuid}.done"
    err_path="$PIPA_DIR/.err/${uuid}.err"
    lock_path="$PIPA_DIR/.locks/${uuid}.lock"

    if [[ -f "$done_path" ]]; then
      n_done=$(( n_done + 1 ))
    elif [[ -f "$err_path" ]]; then
      n_err=$(( n_err + 1 ))
    else
      n_pending=$(( n_pending + 1 ))
    fi
  done

  log "Status — done: $n_done | errors: $n_err | pending: $n_pending"

  # All UUIDs accounted for — watcher can exit
  if (( n_done + n_err == ${#ALL_UUIDS[@]} )); then
    log "All UUIDs finished (done=$n_done, errors=$n_err). Watcher exiting."
    exit 0
  fi

  # Enforce cache cap: wait if there are already enough BAMs on disk
  cached=$(count_cached_bams)
  if (( cached >= MAX_CACHED_BAMS )); then
    log "Cache cap hit ($cached BAMs present, cap=$MAX_CACHED_BAMS) — waiting for PipeB to free space."
    sleep "$POLL_INTERVAL"
    continue
  fi

  # Launch downloads for pending UUIDs (up to available cache slots)
  slots=$(( MAX_CACHED_BAMS - cached ))
  launched=0

  for uuid in "${ALL_UUIDS[@]}"; do
    (( launched >= slots )) && break

    done_path="$PIPA_DIR/.done/${uuid}.done"
    err_path="$PIPA_DIR/.err/${uuid}.err"
    lock_path="$PIPA_DIR/.locks/${uuid}.lock"

    # Skip already-finished or already-locked UUIDs
    [[ -f "$done_path" ]] && continue
    [[ -f "$err_path"  ]] && continue
    [[ -d "$lock_path" ]] && continue   # another process already downloading

    log "Submitting download: $uuid (cache=$cached/$MAX_CACHED_BAMS)"

    if $DRY_RUN; then
      log "[DRY-RUN] Would run: bash $PIPEA_SCRIPT $MANIFEST $TOKEN $PIPA_DIR $uuid $N_PARALLEL"
    else
      # Run in background so the watcher loop stays responsive.
      # stdout/stderr are appended to a per-UUID log.
      bash "$PIPEA_SCRIPT" \
        "$MANIFEST" "$TOKEN" "$PIPA_DIR" "$uuid" "$N_PARALLEL" \
        >> "$PIPA_DIR/.err/${uuid}_download.log" 2>&1 &
    fi

    launched=$(( launched + 1 ))
    cached=$(( cached + 1 ))   # optimistic count to avoid over-launching
  done

  sleep "$POLL_INTERVAL"
done
