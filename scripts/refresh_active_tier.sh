#!/usr/bin/env bash
# Refresh the active-tier universe through the full ingest pipeline.
# Picks up Q1 2026 financials/segments/employees/transcripts/SEC/audit
# for each company. Runs serially to avoid FMP rate-limit contention.

set -uo pipefail
cd "$(dirname "$0")/.."

LOG_DIR="data/refresh_logs/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$LOG_DIR"

TICKERS=(
    AMAT AMD AMZN AVGO AXTI BE CAT CENX COHR CRWV DELL FN
    GEV GOOGL HUBB INTC LITE META MSFT MU NVDA ON PLTR RRX
    TER TSLA VRT
)

echo "Refreshing ${#TICKERS[@]} active-tier tickers, logs -> $LOG_DIR"
echo "Started: $(date)"
echo

success=0
failed=0
for i in "${!TICKERS[@]}"; do
    ticker="${TICKERS[$i]}"
    n=$((i + 1))
    log="$LOG_DIR/${ticker}.log"
    start=$(date +%s)
    echo "[$n/${#TICKERS[@]}] $ticker — start $(date +%H:%M:%S)..."
    if /Users/michael/Arrow/.venv/bin/python3 scripts/ingest_company.py "$ticker" >"$log" 2>&1; then
        elapsed=$(( $(date +%s) - start ))
        echo "  ✓ $ticker  ${elapsed}s"
        success=$((success + 1))
    else
        elapsed=$(( $(date +%s) - start ))
        echo "  ✗ $ticker  ${elapsed}s  (see $log)"
        failed=$((failed + 1))
    fi
done

echo
echo "Done: $success ok, $failed failed"
echo "Finished: $(date)"
