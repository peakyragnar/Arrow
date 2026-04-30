"""Ingest FMP daily prices + market cap for one or more securities.

Usage:
    uv run scripts/ingest_prices.py NVDA [SPY ...]
    uv run scripts/ingest_prices.py --since 2018-01-01 NVDA
    uv run scripts/ingest_prices.py --since 2024-01-01 --until 2024-12-31 NVDA
    uv run scripts/ingest_prices.py --all          # all active securities

Idempotent: re-runs upsert on (security_id, date), so safe to schedule daily.
"""

from __future__ import annotations

import sys
from typing import Any

from arrow.agents.fmp_prices import DEFAULT_SINCE_DATE, backfill_fmp_prices
from arrow.db.connection import get_conn


def _usage() -> str:
    return (
        "Usage: ingest_prices.py [--since YYYY-MM-DD] [--until YYYY-MM-DD] "
        "(--all | TICKER [TICKER ...])"
    )


def _all_active_tickers(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT ticker FROM securities WHERE status='active' ORDER BY ticker"
        )
        return [r[0] for r in cur.fetchall()]


def _print_section(title: str, counts: dict[str, Any]) -> None:
    print(title)
    for key, value in counts.items():
        print(f"  {key}: {value}")
    print()


def main() -> int:
    args = sys.argv[1:]

    since_date: str | None = DEFAULT_SINCE_DATE
    until_date: str | None = None
    all_securities = False

    if "--since" in args:
        i = args.index("--since")
        if i + 1 >= len(args):
            print(_usage(), file=sys.stderr)
            return 2
        since_date = args[i + 1]
        args = args[:i] + args[i + 2 :]

    if "--until" in args:
        i = args.index("--until")
        if i + 1 >= len(args):
            print(_usage(), file=sys.stderr)
            return 2
        until_date = args[i + 1]
        args = args[:i] + args[i + 2 :]

    if "--all" in args:
        all_securities = True
        args = [a for a in args if a != "--all"]

    with get_conn() as conn:
        if all_securities:
            tickers = _all_active_tickers(conn)
            if not tickers:
                print("No active securities found.", file=sys.stderr)
                return 1
        else:
            if not args:
                print(_usage(), file=sys.stderr)
                return 2
            tickers = [t.upper() for t in args]

        print(f"Ingesting {len(tickers)} security/securities: {tickers}")
        print(f"  since={since_date}  until={until_date or '(today)'}")
        print()

        counts = backfill_fmp_prices(
            conn,
            tickers,
            since_date=since_date,
            until_date=until_date,
        )

    _print_section("FMP prices ingest results:", counts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
