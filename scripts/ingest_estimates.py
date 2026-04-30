"""Ingest FMP analyst estimates / price targets / earnings / grades for one or more securities.

Usage:
    uv run scripts/ingest_estimates.py NVDA [PLTR ...]
    uv run scripts/ingest_estimates.py --all          # all active common stock

Idempotent: re-runs delete-and-replace for analyst_estimates and
price_target_consensus, UPSERT for earnings_surprises, and dedup-on-
natural-key for the two event logs (analyst_grades, analyst_price_targets).
"""

from __future__ import annotations

import sys
from typing import Any

from arrow.agents.fmp_estimates import backfill_fmp_estimates
from arrow.db.connection import get_conn


def _usage() -> str:
    return "Usage: ingest_estimates.py (--all | TICKER [TICKER ...])"


def _all_active_common_stock_tickers(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT ticker FROM securities "
            "WHERE status = 'active' AND kind = 'common_stock' "
            "ORDER BY ticker"
        )
        return [r[0] for r in cur.fetchall()]


def _print_section(title: str, counts: dict[str, Any]) -> None:
    print(title)
    for key, value in counts.items():
        print(f"  {key}: {value}")
    print()


def main() -> int:
    args = sys.argv[1:]

    all_tickers = False
    if "--all" in args:
        all_tickers = True
        args = [a for a in args if a != "--all"]

    with get_conn() as conn:
        if all_tickers:
            tickers = _all_active_common_stock_tickers(conn)
            if not tickers:
                print("No active common-stock securities found.", file=sys.stderr)
                return 1
        else:
            if not args:
                print(_usage(), file=sys.stderr)
                return 2
            tickers = [t.upper() for t in args]

        print(f"Ingesting estimates for {len(tickers)} ticker(s): {tickers}")
        print()

        counts = backfill_fmp_estimates(conn, tickers)

    _print_section("FMP estimates ingest results:", counts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
