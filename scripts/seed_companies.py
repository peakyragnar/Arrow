"""Seed company rows from SEC.

Usage:
    uv run scripts/seed_companies.py NVDA [MSFT AAPL ...]
"""

from __future__ import annotations

import sys

from arrow.db.connection import get_conn
from arrow.ingest.sec.bootstrap import seed_companies


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: seed_companies.py TICKER [TICKER ...]", file=sys.stderr)
        return 2

    tickers = sys.argv[1:]
    with get_conn() as conn:
        seeded = seed_companies(conn, tickers)

    for s in seeded:
        print(
            f"Seeded {s.ticker} (cik={s.cik}, id={s.id}, fye={s.fiscal_year_end_md})"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
