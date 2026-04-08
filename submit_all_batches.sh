#!/bin/bash
# submit_all_batches.sh
# ─────────────────────────────────────────────────────────────────────────────
# Reads a GDC manifest, splits all sample UUIDs into batches of BATCH_SIZE,
# and submits one SLURM job per batch via tcga_batch_5samples.sh.
#
# Usage:
#   bash submit_all_batches.sh
#   bash submit_all_batches.sh --dry-run    # print sbatch commands, don't submit
# ─────────────────────────────────────────────────────────────────────────────

# ── Configuration ─────────────────────────────────────────────────────────────
MANIFEST=/pedigree2/projects/TCGA_SARC/gdc_manifest.TCGA-SARC-WGS_tumor.2026-02-23.155715.txt             
BATCH_SCRIPT=/ribosome/projects/sapadmanabhan/TCGA_SARC/Scripts/tcga_batch_5samples.sh # path to the SLURM batch script
LOG_DIR=/ribosome/projects/sapadmanabhan/TCGA_SARC/Outputs # where job .out/.err go

BATCH_SIZE=5        # samples per job (change if needed)
DRY_RUN=false       # set to true or pass --dry-run to preview without submitting

# ── Parse flags ───────────────────────────────────────────────────────────────
for arg in "$@"; do
    [[ "$arg" == "--dry-run" ]] && DRY_RUN=true
done

# ── Sanity checks ─────────────────────────────────────────────────────────────
[[ -f "$MANIFEST" ]]     || { echo "ERROR: Manifest not found: $MANIFEST";     exit 1; }
[[ -f "$BATCH_SCRIPT" ]] || { echo "ERROR: Batch script not found: $BATCH_SCRIPT"; exit 1; }

mkdir -p "$LOG_DIR"

# ── Extract all UUIDs from manifest (skip header line starting with 'id') ─────
mapfile -t ALL_UUIDS < <(grep -v '^id' "$MANIFEST" | awk 'NF{print $1}')

TOTAL=${#ALL_UUIDS[@]}
if [[ $TOTAL -eq 0 ]]; then
    echo "ERROR: No UUIDs found in manifest: $MANIFEST"
    exit 1
fi

N_BATCHES=$(( (TOTAL + BATCH_SIZE - 1) / BATCH_SIZE ))   # ceiling division

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Manifest      : $MANIFEST"
echo "  Total samples : $TOTAL"
echo "  Batch size    : $BATCH_SIZE"
echo "  Jobs to submit: $N_BATCHES"
$DRY_RUN && echo "  Mode          : DRY RUN (no jobs submitted)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# ── Chunk and submit ──────────────────────────────────────────────────────────
SUBMITTED=0
BATCH_NUM=0
PREV_JOB_ID=""   # tracks the last submitted job ID for chaining

for (( i=0; i<TOTAL; i+=BATCH_SIZE )); do

    BATCH_NUM=$(( BATCH_NUM + 1 ))

    # Slice BATCH_SIZE UUIDs from the full array
    BATCH=("${ALL_UUIDS[@]:$i:$BATCH_SIZE}")
    BATCH_LABEL=$(printf "batch%03d" "$BATCH_NUM")

    CMD=(
        sbatch
        --job-name="TCGA_${BATCH_LABEL}"
        --output="${LOG_DIR}/${BATCH_LABEL}_%j.out"
        --error="${LOG_DIR}/${BATCH_LABEL}_%j.err"
    )

    # Chain: every job after the first waits for the previous to finish OK.
    # If the previous job fails, downstream jobs are automatically cancelled.
    if [[ -n "$PREV_JOB_ID" ]]; then
        CMD+=(--dependency=afterok:${PREV_JOB_ID})
    fi

    CMD+=(
        "$BATCH_SCRIPT"
        "${BATCH[@]}"   # UUIDs passed as positional args to the job script
    )

    echo ""
    echo "Batch ${BATCH_NUM}/${N_BATCHES}  [${#BATCH[@]} samples]"
    echo "  UUIDs      : ${BATCH[*]}"
    [[ -n "$PREV_JOB_ID" ]] && echo "  Depends on : job $PREV_JOB_ID"
    echo "  CMD        : ${CMD[*]}"

    if $DRY_RUN; then
        echo "  >>> DRY RUN — not submitted (would depend on job ${PREV_JOB_ID:-none})"
        PREV_JOB_ID="<dry_run_id_${BATCH_NUM}>"   # placeholder so chain logic prints correctly
    else
        OUTPUT=$("${CMD[@]}" 2>&1)
        STATUS=$?
        if [[ $STATUS -eq 0 ]]; then
            # sbatch prints "Submitted batch job <id>" — extract the numeric ID
            PREV_JOB_ID=$(echo "$OUTPUT" | awk '{print $NF}')
            echo "  >>> Submitted: $OUTPUT"
            SUBMITTED=$(( SUBMITTED + 1 ))
        else
            echo "  >>> ERROR submitting batch ${BATCH_NUM}: $OUTPUT"
            echo "  >>> Stopping submission to avoid broken dependency chain."
            break
        fi
        sleep 1
    fi

done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
if $DRY_RUN; then
    echo "  Dry run complete. $N_BATCHES jobs would be submitted."
else
    echo "  Done. $SUBMITTED / $N_BATCHES jobs submitted."
fi
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
