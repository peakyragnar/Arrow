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

Trusted endpoints: ``fmp-employees-v1``, ``fmp-segments-v1``.
Targets to update: ``fmp-is-v1``, ``fmp-bs-v1``, ``fmp-cf-v1``.

Idempotent. Skips fiscal periods where the trusted endpoints disagree
with each other (no clean canonical) — those surface as steward
findings instead.

Usage:
    uv run scripts/backfill_cross_endpoint_period_end.py            # dry run
    uv run scripts/backfill_cross_endpoint_period_end.py --apply    # write
"""

from __future__ import annotations

import argparse

from arrow.db.connection import get_conn


TRUSTED_VERSIONS = ("fmp-employees-v1", "fmp-segments-v1")
TARGET_VERSIONS = ("fmp-is-v1", "fmp-bs-v1", "fmp-cf-v1")


PREVIEW_SQL = """
WITH trusted_dates AS (
  SELECT company_id, fiscal_year, fiscal_quarter, period_type,
         period_end
  FROM financial_facts
  WHERE superseded_at IS NULL
    AND extraction_version = ANY(%s)
  GROUP BY company_id, fiscal_year, fiscal_quarter, period_type, period_end
),
canonical AS (
  -- Only canonicalize when trusted endpoints unambiguously agree on one date.
  SELECT company_id, fiscal_year, fiscal_quarter, period_type,
         MIN(period_end) AS canon_pe
  FROM trusted_dates
  GROUP BY company_id, fiscal_year, fiscal_quarter, period_type
  HAVING COUNT(DISTINCT period_end) = 1
)
SELECT c.ticker, ff.fiscal_year, ff.fiscal_quarter, ff.period_type,
       ff.statement, ff.extraction_version,
       ff.period_end AS old_pe, can.canon_pe AS new_pe,
       COUNT(*) AS rows
FROM financial_facts ff
JOIN canonical can
  ON can.company_id = ff.company_id
 AND can.fiscal_year = ff.fiscal_year
 AND COALESCE(can.fiscal_quarter, -1) = COALESCE(ff.fiscal_quarter, -1)
 AND can.period_type = ff.period_type
JOIN companies c ON c.id = ff.company_id
WHERE ff.superseded_at IS NULL
  AND ff.dimension_type IS NULL
  AND ff.extraction_version = ANY(%s)
  AND ff.period_end <> can.canon_pe
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
ORDER BY 1, 2, 3, 4, 5;
"""


UPDATE_SQL = """
WITH trusted_dates AS (
  SELECT company_id, fiscal_year, fiscal_quarter, period_type,
         period_end
  FROM financial_facts
  WHERE superseded_at IS NULL
    AND extraction_version = ANY(%s)
  GROUP BY company_id, fiscal_year, fiscal_quarter, period_type, period_end
),
canonical AS (
  SELECT company_id, fiscal_year, fiscal_quarter, period_type,
         MIN(period_end) AS canon_pe
  FROM trusted_dates
  GROUP BY company_id, fiscal_year, fiscal_quarter, period_type
  HAVING COUNT(DISTINCT period_end) = 1
),
mismatched AS (
  SELECT ff.id, can.canon_pe AS new_pe
  FROM financial_facts ff
  JOIN canonical can
    ON can.company_id = ff.company_id
   AND can.fiscal_year = ff.fiscal_year
   AND COALESCE(can.fiscal_quarter, -1) = COALESCE(ff.fiscal_quarter, -1)
   AND can.period_type = ff.period_type
  WHERE ff.superseded_at IS NULL
    AND ff.dimension_type IS NULL
    AND ff.extraction_version = ANY(%s)
    AND ff.period_end <> can.canon_pe
)
UPDATE financial_facts ff
SET period_end            = m.new_pe,
    calendar_year         = EXTRACT(YEAR FROM m.new_pe)::int,
    calendar_quarter      = EXTRACT(QUARTER FROM m.new_pe)::int,
    calendar_period_label = 'CY' || EXTRACT(YEAR FROM m.new_pe)::int
                            || ' Q' || EXTRACT(QUARTER FROM m.new_pe)::int
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
        cur.execute(PREVIEW_SQL, (list(TRUSTED_VERSIONS), list(TARGET_VERSIONS)))
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

        cur.execute(UPDATE_SQL, (list(TRUSTED_VERSIONS), list(TARGET_VERSIONS)))
        updated = cur.rowcount
        conn.commit()
        print(f"\nUpdated {updated} rows.")


if __name__ == "__main__":
    main()
