"""Snap IS/BS/CF period_end to the trusted-endpoint date for the same fiscal period.

Phase 2 of the FMP date-stamping cleanup. Where IS/BS/CF rows are stamped
with a calendar-month-end approximation but employees/segments rows for
the same (company, fiscal_year, fiscal_quarter, period_type) carry the
real fiscal year-end (or quarter-end), snap IS/BS/CF to the trusted
date.

Examples (pre-fix):
  DELL FY2017 annual: IS/BS/CF at 2017-01-31, employees/segments at 2017-02-03.
  INTC FY2018 Q4: IS/BS/CF at 2018-12-31, segments at 2018-12-29.

Phase 1 (`backfill_q4_period_end.py`) handled Q4-quarter-vs-FY-annual
within the same statement. Phase 2 handles cross-endpoint splits: the
FY annual itself is sometimes wrong on the IS/BS/CF side, while
employees/segments are reliably stamped to the real fiscal date.

Idempotent. Skips fiscal periods where the trusted endpoints disagree
with each other (no clean canonical) — those surface as steward
findings instead.

The body of this script is now a library function in
``arrow.normalize.periods.canonicalize`` so it can be invoked from the
``ingest_company.py`` orchestrator. The CLI here is the operator entry.

Usage:
    uv run scripts/backfill_cross_endpoint_period_end.py            # dry run
    uv run scripts/backfill_cross_endpoint_period_end.py --apply    # write
"""

from __future__ import annotations

import argparse

from arrow.db.connection import get_conn
from arrow.normalize.periods.canonicalize import (
    _cross_endpoint_sql,
    canonicalize_cross_endpoint,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write the update; default is a dry-run preview.",
    )
    args = parser.parse_args()

    with get_conn() as conn:
        # Print the per-group preview table for operator visibility.
        preview_sql, _, params = _cross_endpoint_sql(tickers=None)
        with conn.cursor() as cur:
            cur.execute(preview_sql, params)
            preview = cur.fetchall()

        if not preview:
            print("No cross-endpoint period_end mismatches. Nothing to fix.")
            return

        print(
            f"{'TICKER':<8}{'FY':>5}{'FQ':>4}{'pt':<8}"
            f"{'STMT':<18}{'EV':<14}{'old_pe':<12}{'new_pe':<12}{'rows':>6}"
        )
        total = 0
        for ticker, fy, fq, pt, stmt, ev, old, new, n in preview:
            q = str(fq) if fq is not None else "-"
            print(
                f"{ticker:<8}{fy:>5}{q:>4}{pt:<8}{stmt:<18}{ev:<14}"
                f"{str(old):<12}{str(new):<12}{n:>6}"
            )
            total += n
        print(f"\nTotal rows to update: {total}")

        if not args.apply:
            print("\nDry run. Pass --apply to write.")
            return

        result = canonicalize_cross_endpoint(
            conn, tickers=None, apply=True, actor="operator"
        )
        print(
            f"\nUpdated {result.rows_processed} rows. "
            f"ingest_run_id={result.ingest_run_id}"
        )


if __name__ == "__main__":
    main()
