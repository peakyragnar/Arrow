"""Ingest FMP historical-employee-count for one or more tickers.

Usage:
    uv run scripts/ingest_employees.py NVDA [MSFT AAPL ...]

Writes one `financial_facts` row per 10-K per ticker, with:
    statement = 'metrics'
    concept   = 'total_employees'
    period_type = 'annual'
    unit = 'employees'

Companies must be seeded first (scripts/seed_companies.py).

Feeds metric 18 (Revenue per Employee) in formulas.md. Quarterly consumers
join to the most recent `total_employees` row where
`employee_period_end <= quarter_end`.
"""

from __future__ import annotations

import sys
from typing import Any

from arrow.agents.fmp_employees import backfill_fmp_employees
from arrow.db.connection import get_conn


def _print_success(tickers: list[str], counts: dict[str, Any]) -> None:
    print(f"Ingested FMP employee counts for {', '.join(t.upper() for t in tickers)}:")
    print(f"  ingest_run_id:          {counts['ingest_run_id']}")
    print(f"  raw_responses written:  {counts['raw_responses']}")
    print(f"  rows processed:         {counts['rows_processed']}")
    print(f"  facts written:          {counts['facts_written']}")
    print(f"  facts superseded:       {counts['facts_superseded']}")
    print()
    print("Status: PASS — employee-count facts stored.")


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: ingest_employees.py TICKER [TICKER ...]", file=sys.stderr)
        return 2

    tickers = sys.argv[1:]
    with get_conn() as conn:
        counts = backfill_fmp_employees(conn, tickers)

    _print_success(tickers, counts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
