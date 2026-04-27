"""Snap Q4 quarterly period_end to FY annual period_end where they disagree.

For each (company, fiscal_year) where a Q4 quarterly fact's period_end
differs from the FY annual fact's period_end, update the Q4 quarterly
period_end to match. Both come from the same 10-K filing — FMP's
quarterly endpoint stamps Q4 with a calendar-month-end approximation
while the annual endpoint carries the actual fiscal year-end.

Idempotent. Re-running after the fix is a no-op.

Usage:
    uv run scripts/backfill_q4_period_end.py            # dry run
    uv run scripts/backfill_q4_period_end.py --apply    # write
"""

from __future__ import annotations

import argparse

from arrow.db.connection import get_conn


# Phase 1: align IS/BS/CF Q4 quarter rows to the SAME-statement FY annual row.
# Cross-endpoint date splits (employees/segments vs IS/BS/CF) are out of scope
# here — they produce duplicate v_metrics_fy rows for old years, but don't
# affect the current dashboard view, and deserve a separate backfill.
TARGET_VERSIONS = ("fmp-is-v1", "fmp-bs-v1", "fmp-cf-v1")


PREVIEW_SQL = """
WITH annual_pe AS (
  SELECT company_id, fiscal_year, statement, extraction_version,
         period_end AS fy_pe
  FROM financial_facts
  WHERE period_type = 'annual'
    AND superseded_at IS NULL
    AND dimension_type IS NULL
    AND extraction_version = ANY(%s)
  GROUP BY company_id, fiscal_year, statement, extraction_version, period_end
)
SELECT c.ticker, q.fiscal_year, q.statement,
       q.period_end AS old_pe, a.fy_pe AS new_pe,
       COUNT(*) AS rows
FROM financial_facts q
JOIN annual_pe a
  ON a.company_id = q.company_id
 AND a.fiscal_year = q.fiscal_year
 AND a.statement = q.statement
 AND a.extraction_version = q.extraction_version
JOIN companies c ON c.id = q.company_id
WHERE q.period_type = 'quarter'
  AND q.fiscal_quarter = 4
  AND q.superseded_at IS NULL
  AND q.dimension_type IS NULL
  AND q.extraction_version = ANY(%s)
  AND q.period_end <> a.fy_pe
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3;
"""


UPDATE_SQL = """
WITH annual_pe AS (
  SELECT company_id, fiscal_year, statement, extraction_version,
         period_end AS fy_pe
  FROM financial_facts
  WHERE period_type = 'annual'
    AND superseded_at IS NULL
    AND dimension_type IS NULL
    AND extraction_version = ANY(%s)
  GROUP BY company_id, fiscal_year, statement, extraction_version, period_end
),
mismatched AS (
  SELECT q.id, a.fy_pe AS new_pe
  FROM financial_facts q
  JOIN annual_pe a
    ON a.company_id = q.company_id
   AND a.fiscal_year = q.fiscal_year
   AND a.statement = q.statement
   AND a.extraction_version = q.extraction_version
  WHERE q.period_type = 'quarter'
    AND q.fiscal_quarter = 4
    AND q.superseded_at IS NULL
    AND q.dimension_type IS NULL
    AND q.extraction_version = ANY(%s)
    AND q.period_end <> a.fy_pe
)
UPDATE financial_facts ff
SET period_end = m.new_pe
FROM mismatched m
WHERE ff.id = m.id
RETURNING ff.id;
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write the update; default is a dry-run preview.",
    )
    args = parser.parse_args()

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(PREVIEW_SQL, (list(TARGET_VERSIONS), list(TARGET_VERSIONS)))
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

        cur.execute(UPDATE_SQL, (list(TARGET_VERSIONS), list(TARGET_VERSIONS)))
        updated = cur.rowcount
        conn.commit()
        print(f"\nUpdated {updated} rows.")


if __name__ == "__main__":
    main()
