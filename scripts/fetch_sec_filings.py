"""Fetch SEC filings/artifacts for seeded tickers.

Usage:
    uv run scripts/fetch_sec_filings.py NVDA [MSFT ...]
    uv run scripts/fetch_sec_filings.py --since 2019-01-01 NVDA
    uv run scripts/fetch_sec_filings.py --limit 3 NVDA

For 10-K / 10-Q filings, the default calendar cutoff is rounded by fiscal
year so the first included fiscal year is complete. Earnings 8-Ks stay on
the calendar filing-date cutoff.
"""

from __future__ import annotations

import sys

from arrow.db.connection import get_conn
from arrow.ingest.sec.filings import DEFAULT_QUAL_SINCE_DATE, ingest_sec_filings


def main() -> int:
    args = sys.argv[1:]
    limit: int | None = None
    since_date = DEFAULT_QUAL_SINCE_DATE
    until_date = None

    def _pop_date_flag(flag: str):
        nonlocal args
        if flag not in args:
            return None
        i = args.index(flag)
        if i + 1 >= len(args):
            print(
                "Usage: fetch_sec_filings.py [--since YYYY-MM-DD] [--until YYYY-MM-DD] [--limit N] TICKER [TICKER ...]\n"
                "Default SEC window is 5 fiscal years of 10-K / 10-Q primary filings.",
                file=sys.stderr,
            )
            return 2
        from datetime import date as _d

        try:
            y, m, d = args[i + 1].split("-")
            val = _d(int(y), int(m), int(d))
        except Exception as e:
            print(f"Invalid {flag} date: {e}", file=sys.stderr)
            return 2
        args = args[:i] + args[i + 2 :]
        return val

    parsed_since = _pop_date_flag("--since")
    if parsed_since == 2:
        return 2
    if parsed_since is not None:
        since_date = parsed_since

    parsed_until = _pop_date_flag("--until")
    if parsed_until == 2:
        return 2
    if parsed_until is not None:
        until_date = parsed_until

    if "--limit" in args:
        i = args.index("--limit")
        if i + 1 >= len(args):
            print(
                "Usage: fetch_sec_filings.py [--since YYYY-MM-DD] [--until YYYY-MM-DD] [--limit N] TICKER [TICKER ...]\n"
                "Default SEC window is 5 fiscal years of 10-K / 10-Q primary filings.",
                file=sys.stderr,
            )
            return 2
        limit = int(args[i + 1])
        args = args[:i] + args[i + 2 :]

    if not args:
        print(
            "Usage: fetch_sec_filings.py [--since YYYY-MM-DD] [--until YYYY-MM-DD] [--limit N] TICKER [TICKER ...]\n"
            "Default SEC window is 5 fiscal years of 10-K / 10-Q primary filings.",
            file=sys.stderr,
        )
        return 2

    with get_conn() as conn:
        counts = ingest_sec_filings(
            conn,
            args,
            since_date=since_date,
            until_date=until_date,
            limit_per_ticker=limit,
        )

    print(f"Fetched SEC filings for {', '.join(t.upper() for t in args)}:")
    print(f"  ingest_run_id:          {counts['ingest_run_id']}")
    print(f"  since_date:             {counts['since_date']}")
    print(f"  until_date:             {counts['until_date']}")
    if counts.get("min_fiscal_year_by_ticker"):
        fy = ", ".join(
            f"{ticker}=FY{year}"
            for ticker, year in sorted(counts["min_fiscal_year_by_ticker"].items())
        )
        print(f"  10-K/Q window start:    {fy}")
    if counts.get("max_fiscal_year_by_ticker"):
        fy = ", ".join(
            f"{ticker}=FY{year}"
            for ticker, year in sorted(counts["max_fiscal_year_by_ticker"].items())
        )
        print(f"  10-K/Q window end:      {fy}")
    print(f"  earnings_8k_only:       {counts['earnings_8k_only']}")
    print(f"  raw_responses written:  {counts['raw_responses']}")
    print(f"  filings seen:           {counts['filings_seen']}")
    print(f"  filing docs fetched:    {counts['documents_fetched']}")
    print(f"  artifacts written:      {counts['artifacts_written']}")
    print(f"  artifacts existing:     {counts['artifacts_existing']}")
    print(f"  sections written:       {counts['sections_written']}")
    for artifact_type, n in sorted(counts["artifacts_by_type"].items()):
        print(f"  {artifact_type}:                {n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
