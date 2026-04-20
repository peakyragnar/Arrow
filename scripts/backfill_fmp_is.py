"""Backfill FMP income-statement history for one or more tickers.

Usage:
    uv run scripts/backfill_fmp_is.py NVDA [MSFT ...]

Companies must be seeded first (scripts/seed_companies.py). Orchestration
lives in arrow.agents.fmp_ingest.backfill_fmp_is; this script just parses
argv, opens the DB, and prints results.
"""

from __future__ import annotations

import sys

from arrow.agents.fmp_ingest import backfill_fmp_is
from arrow.db.connection import get_conn


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: backfill_fmp_is.py TICKER [TICKER ...]", file=sys.stderr)
        return 2

    tickers = sys.argv[1:]
    with get_conn() as conn:
        counts = backfill_fmp_is(conn, tickers)

    print(f"Backfilled IS for {', '.join(t.upper() for t in tickers)}:")
    print(f"  since_date:          {counts['since_date']}")
    print(f"  ingest_run_id:       {counts['ingest_run_id']}")
    print(f"  raw_responses:       {counts['raw_responses']}")
    print(f"  rows_processed:      {counts['rows_processed']}")
    print(f"  facts written:       {counts['financial_facts_written']}")
    print(f"  facts superseded:    {counts['financial_facts_superseded']}")
    print(f"  Layer-3 identities:  {counts['layer3_identities_checked']} checked")
    print(f"  XBRL cross-check:    {counts['xbrl_matched']}/{counts['xbrl_checked']} matched "
          f"(skipped: {counts['xbrl_skipped_q4']} Q4, "
          f"{counts['xbrl_skipped_split_sensitive']} split-sensitive, "
          f"{counts['xbrl_skipped_no_xbrl']} absent-from-XBRL)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
