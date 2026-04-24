"""Ingest FMP revenue segmentation for one or more tickers.

Usage:
    uv run scripts/ingest_segments.py NVDA [MSFT ...]

Companies must be seeded first. Segment rows land in `financial_facts` with:
    statement = 'segment'
    concept = 'revenue'
    dimension_type = product | geography
"""

from __future__ import annotations

import sys
from typing import Any

from arrow.agents.fmp_segments import backfill_fmp_segments
from arrow.db.connection import get_conn


def _print_success(tickers: list[str], counts: dict[str, Any]) -> None:
    print(f"Ingested FMP revenue segments for {', '.join(t.upper() for t in tickers)}:")
    print(f"  ingest_run_id:          {counts['ingest_run_id']}")
    print(f"  since_date:             {counts['since_date']}")
    print(f"  raw_responses written:  {counts['raw_responses']}")
    print(f"  rows processed:         {counts['rows_processed']}")
    print(f"  segments processed:     {counts['segments_processed']}")
    print(f"  facts written:          {counts['facts_written']}")
    print(f"  facts superseded:       {counts['facts_superseded']}")
    print()
    print("Status: PASS — revenue segment facts stored.")


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: ingest_segments.py TICKER [TICKER ...]", file=sys.stderr)
        return 2

    tickers = [ticker.upper() for ticker in sys.argv[1:]]
    with get_conn() as conn:
        counts = backfill_fmp_segments(conn, tickers)

    _print_success(tickers, counts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
