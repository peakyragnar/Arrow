"""Ingest FMP earnings-call transcripts for one or more tickers.

Usage:
    uv run scripts/ingest_transcripts.py NVDA [MSFT ...]
    uv run scripts/ingest_transcripts.py --refresh NVDA
    uv run scripts/ingest_transcripts.py --limit 1 NVDA
"""

from __future__ import annotations

import argparse
from typing import Any

from arrow.agents.fmp_transcripts import ingest_transcripts
from arrow.db.connection import get_conn


def _print_success(tickers: list[str], counts: dict[str, Any]) -> None:
    print(f"Ingested FMP earnings-call transcripts for {', '.join(tickers)}:")
    print(f"  ingest_run_id:             {counts['ingest_run_id']}")
    print(f"  raw_responses written:     {counts['raw_responses']}")
    print(f"  transcript dates fetched:  {counts['transcript_dates_fetched']}")
    print(f"  transcript dates seen:     {counts['transcript_dates_seen']}")
    print(f"  transcripts requested:     {counts['transcripts_requested']}")
    print(f"  transcripts fetched:       {counts['transcripts_fetched']}")
    print(f"  transcripts missing:       {counts['transcripts_missing']}")
    print(f"  artifacts inserted:        {counts['artifacts_inserted']}")
    print(f"  artifacts existing:        {counts['artifacts_existing']}")
    print(f"  artifacts superseded:      {counts['artifacts_superseded']}")
    print(f"  text units inserted:       {counts['text_units_inserted']}")
    print(f"  text chunks inserted:      {counts['text_chunks_inserted']}")
    print()
    print("Status: PASS — transcript artifacts and text units stored.")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("tickers", nargs="+", metavar="TICKER")
    parser.add_argument("--refresh", action="store_true", help="re-fetch existing transcript periods")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="dev/test guard: max transcript periods to fetch per ticker",
    )
    parser.add_argument("--actor", default="operator", help="operator or agent identity")
    args = parser.parse_args()

    tickers = [ticker.upper() for ticker in args.tickers]
    with get_conn() as conn:
        counts = ingest_transcripts(
            conn,
            tickers,
            refresh=args.refresh,
            actor=args.actor,
            limit_per_ticker=args.limit,
        )

    _print_success(tickers, counts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
