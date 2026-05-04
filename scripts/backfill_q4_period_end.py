"""Snap Q4 quarterly period_end to FY annual period_end where they disagree.

For each (company, fiscal_year) where a Q4 quarterly fact's period_end
differs from the FY annual fact's period_end, update the Q4 quarterly
period_end to match. Both come from the same 10-K filing — FMP's
quarterly endpoint stamps Q4 with a calendar-month-end approximation
while the annual endpoint carries the actual fiscal year-end.

Idempotent. Re-running after the fix is a no-op.

The body of this script is now a library function in
``arrow.normalize.periods.canonicalize`` so it can be invoked from the
``ingest_company.py`` orchestrator. The CLI here is the operator entry.

Usage:
    uv run scripts/backfill_q4_period_end.py            # dry run
    uv run scripts/backfill_q4_period_end.py --apply    # write
"""

from __future__ import annotations

import argparse

from arrow.db.connection import get_conn
from arrow.normalize.periods.canonicalize import _q4_sql, canonicalize_q4_to_annual


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write the update; default is a dry-run preview.",
    )
    args = parser.parse_args()

    with get_conn() as conn:
        preview_sql, _, params = _q4_sql(tickers=None)
        with conn.cursor() as cur:
            cur.execute(preview_sql, params)
            preview = cur.fetchall()

        if not preview:
            print("No mismatched Q4 period_ends. Nothing to fix.")
            return

        print(f"{'TICKER':<8}{'FY':>5}{'STMT':<18}{'old_pe':<12}{'new_pe':<12}{'rows':>6}")
        total = 0
        for ticker, fy, stmt, old, new, n in preview:
            print(f"{ticker:<8}{fy:>5}{stmt:<18}{str(old):<12}{str(new):<12}{n:>6}")
            total += n
        print(f"\nTotal rows to update: {total}")

        if not args.apply:
            print("\nDry run. Pass --apply to write.")
            return

        result = canonicalize_q4_to_annual(
            conn, tickers=None, apply=True, actor="operator"
        )
        print(
            f"\nUpdated {result.rows_processed} rows. "
            f"ingest_run_id={result.ingest_run_id}"
        )


if __name__ == "__main__":
    main()
