"""Fetch recent SEC filings/artifacts for seeded tickers.

Usage:
    uv run scripts/fetch_sec_filings.py NVDA [MSFT ...]
    uv run scripts/fetch_sec_filings.py --limit 3 NVDA
"""

from __future__ import annotations

import sys

from arrow.db.connection import get_conn
from arrow.ingest.sec.filings import ingest_recent_sec_filings


def main() -> int:
    args = sys.argv[1:]
    limit = 5
    if "--limit" in args:
        i = args.index("--limit")
        if i + 1 >= len(args):
            print("Usage: fetch_sec_filings.py [--limit N] TICKER [TICKER ...]", file=sys.stderr)
            return 2
        limit = int(args[i + 1])
        args = args[:i] + args[i + 2 :]

    if not args:
        print("Usage: fetch_sec_filings.py [--limit N] TICKER [TICKER ...]", file=sys.stderr)
        return 2

    with get_conn() as conn:
        counts = ingest_recent_sec_filings(conn, args, limit_per_ticker=limit)

    print(f"Fetched SEC filings for {', '.join(t.upper() for t in args)}:")
    print(f"  ingest_run_id:          {counts['ingest_run_id']}")
    print(f"  raw_responses written:  {counts['raw_responses']}")
    print(f"  filings seen:           {counts['filings_seen']}")
    print(f"  artifacts written:      {counts['artifacts_written']}")
    print(f"  artifacts existing:     {counts['artifacts_existing']}")
    for artifact_type, n in sorted(counts["artifacts_by_type"].items()):
        print(f"  {artifact_type}:                {n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
