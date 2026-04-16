#!/bin/bash
# Full gap backfill using the same pipeline as resilient_sync (blocks + txs + logs)
# Usage: nohup bash scripts/run_full_gap_backfill.sh > logs/full_gap_backfill.log 2>&1 &

cd "$(dirname "$0")/.."

echo "[$(date)] === STEP 1: Main gap backfill (blocks 1,964,081 → 2,041,706) ==="
python3 -u scripts/backfill_gap_range.py --from-block 1964081 --to-block 2041706 --batch-size 25
echo "[$(date)] Main gap finished (exit code: $?)"

echo ""
echo "[$(date)] === STEP 2: Small gaps (6 × 50 blocks) ==="
for start_end in "1722799:1722848" "2695207:2695256" "2775707:2775756" "2775857:2775906" "2786507:2786556" "2795557:2795606"; do
    FROM_BLOCK="${start_end%%:*}"
    TO_BLOCK="${start_end##*:}"
    echo "[$(date)] Gap: blocks $FROM_BLOCK → $TO_BLOCK..."
    python3 -u scripts/backfill_gap_range.py --from-block "$FROM_BLOCK" --to-block "$TO_BLOCK" --batch-size 50
done
echo "[$(date)] Small gaps finished"

echo ""
echo "[$(date)] === ALL DONE ==="
