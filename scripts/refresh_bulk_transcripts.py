"""Incremental transcript refresh across the bulk-seed universe.

Reads the same manifest the bulk-seed produced and re-runs
ingest_transcripts() per ticker. ingest_transcripts() is naturally
missing-only at the (ticker, fiscal_year, fiscal_quarter) level — it
checks current artifacts.source_document_id and only fetches periods
that don't already have a row.

This catches new earnings calls that FMP has published since the
bulk-seed run completed.

Usage:
    uv run scripts/refresh_bulk_transcripts.py \
        --manifest data/universes/transcript_bulk_20260507.json
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import traceback
from datetime import UTC, datetime
from pathlib import Path

from dotenv import load_dotenv

REPO = Path(__file__).resolve().parent.parent
load_dotenv(REPO / ".env", override=True)

from arrow.agents.fmp_transcripts import ingest_transcripts
from arrow.db.connection import get_conn


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument(
        "--trace-out",
        type=Path,
        default=REPO / "data" / "universes" / "bulk_refresh_trace.jsonl",
    )
    args = parser.parse_args()

    if not args.manifest.exists():
        print(f"manifest not found: {args.manifest}", file=sys.stderr)
        return 1
    args.trace_out.parent.mkdir(parents=True, exist_ok=True)

    tickers = [t["ticker"] for t in json.loads(args.manifest.read_text())["tickers"]]
    if args.limit:
        tickers = tickers[: args.limit]
    print(f"Refreshing transcripts for {len(tickers)} tickers from {args.manifest.name}")

    new_total = 0
    started = datetime.now(UTC)
    with args.trace_out.open("a") as trace:
        with get_conn() as conn:
            for i, ticker in enumerate(tickers, 1):
                ticker = ticker.upper()
                t0 = time.monotonic()
                try:
                    counts = ingest_transcripts(
                        conn,
                        tickers=[ticker],
                        refresh=False,
                        actor="operator:refresh",
                        client=None,
                        allow_derived_anchor=True,
                    )
                    inserted = counts.get("artifacts_inserted", 0)
                    new_total += inserted
                    status = "ok"
                    err = None
                except Exception as e:
                    inserted = 0
                    counts = {}
                    status = "error"
                    err = repr(e)

                elapsed = time.monotonic() - t0
                rec = {
                    "ts": datetime.now(UTC).isoformat(),
                    "ticker": ticker,
                    "ordinal": i,
                    "status": status,
                    "artifacts_inserted": inserted,
                    "elapsed_sec": round(elapsed, 2),
                    "error": err,
                }
                trace.write(json.dumps(rec) + "\n")
                trace.flush()

                # Lightweight progress every 50 tickers
                if i % 50 == 0 or inserted > 0 or status == "error":
                    flag = "★" if inserted > 0 else ("⨯" if status == "error" else "·")
                    print(f"  [{i:5d}/{len(tickers)}] {flag} {ticker:8s}  +{inserted}  ({elapsed:.1f}s)"
                          + (f"  {err[:80]}" if err else ""))

    print()
    print(f"Done. New transcripts inserted: {new_total}")
    print(f"Total elapsed: {(datetime.now(UTC) - started).total_seconds()/60:.1f} min")
    print(f"Trace: {args.trace_out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
