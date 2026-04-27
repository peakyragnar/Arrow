"""Supersede a corrupted quarterly IS row with values derived from
annual − (sum of the other three fiscal quarters).

Use when FMP's quarterly endpoint shipped a fabricated quarterly row
whose values disagree wildly with the annual filing. Concretely: the
quarter row for one fiscal year fails the Q1+Q2+Q3+Q4 = annual
reconciliation across most flow concepts (revenue, COGS, gross_profit,
rd, sga, etc.).

The flagged quarter is most often Q4 (FMP often derives Q4 = annual −
prior quarters with one input on the wrong basis), but the same
fabrication pattern can land in any quarter. Use ``--fiscal-quarter``
to target the outlier quarter; default is 4.

Empirical case (2026-04-27): DELL FY2022 Q4 IS — every flow concept
fails the reconciliation. Public DELL Q4 FY22 revenue was ~$28B; FMP
shipped $22.2B. R&D came back as -$1.63B. The full row is corrupted,
likely from FMP mishandling DELL's VMWare spinoff which completed in
Q3 FY22.

The fix supersedes each affected ``fmp-is-v1`` row with reason
``fmp_corrupt_value`` and inserts a replacement row at extraction
version ``arrow-derived-q4-v1`` carrying ``annual − (sum of the
other three quarters)`` for each summable flow concept whose
other-quarter sum is non-zero. Concepts that are reported only at the
annual level for the filer (``general_and_admin_expense``,
``selling_and_marketing_expense`` for some filers) are skipped — their
FMP quarterly placeholder of 0 stays. Per-share / weighted-average
concepts (eps_*, shares_*) are not derived from a sum.

Idempotent: if the current target-quarter row is already at extraction
version ``arrow-derived-q4-v1``, the script reports no work and exits.

Usage:
    uv run scripts/correct_corrupted_q4_is.py --ticker DELL --fiscal-year 2022
    uv run scripts/correct_corrupted_q4_is.py --ticker DELL --fiscal-year 2022 --apply
    uv run scripts/correct_corrupted_q4_is.py --ticker INTC --fiscal-year 2025 --fiscal-quarter 2 --apply
"""

from __future__ import annotations

import argparse
from decimal import Decimal

import psycopg

from arrow.db.connection import get_conn
from arrow.ingest.common.runs import close_succeeded, open_run
from arrow.normalize.periods.derive import (
    derive_calendar_period,
    derive_fiscal_period,
)


DERIVED_EXTRACTION_VERSION = "arrow-derived-q4-v1"
SOURCE_EXTRACTION_VERSION = "fmp-is-v1"
SUPERSESSION_REASON = "fmp_corrupt_value"

#: Concepts that are summable across the four fiscal quarters. Excludes
#: per-share and weighted-average concepts (eps_*, shares_*) which
#: don't sum.
SUMMABLE_FLOW_CONCEPTS = (
    "revenue",
    "cogs",
    "gross_profit",
    "rd",
    "sga",
    "general_and_admin_expense",
    "selling_and_marketing_expense",
    "total_opex",
    "operating_income",
    "ebt_incl_unusual",
    "net_income",
    "net_income_attributable_to_parent",
    "minority_interest",
    "tax",
    "interest_expense",
    "interest_income",
    "continuing_ops_after_tax",
    "discontinued_ops",
    "sbc",
)


def fetch_company(conn: psycopg.Connection, ticker: str) -> tuple[int, str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, fiscal_year_end_md FROM companies WHERE ticker = %s",
            (ticker.upper(),),
        )
        row = cur.fetchone()
        if row is None:
            raise SystemExit(f"company not found: {ticker}")
    return row[0], row[1]


def fetch_annual_and_priors(
    conn: psycopg.Connection,
    *,
    company_id: int,
    fiscal_year: int,
    target_quarter: int,
) -> tuple[dict[str, Decimal], dict[str, Decimal], int | None]:
    """Return (annual_by_concept, sum_other_quarters_by_concept,
    annual_raw_response_id). The "other quarters" are the three quarters
    that are NOT the target — these are assumed correct, and the target
    quarter's value is derived as ``annual − sum(other quarters)``."""
    annual: dict[str, Decimal] = {}
    annual_raw_response_id: int | None = None
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT concept, value, source_raw_response_id
            FROM financial_facts
            WHERE company_id = %s
              AND fiscal_year = %s
              AND period_type = 'annual'
              AND statement = 'income_statement'
              AND extraction_version = %s
              AND superseded_at IS NULL
              AND dimension_type IS NULL
            """,
            (company_id, fiscal_year, SOURCE_EXTRACTION_VERSION),
        )
        for concept, value, raw_id in cur.fetchall():
            annual[concept] = value
            annual_raw_response_id = raw_id  # all annual rows share one raw payload

    other_quarters = [q for q in (1, 2, 3, 4) if q != target_quarter]
    sums: dict[str, Decimal] = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT concept, COALESCE(SUM(value), 0)
            FROM financial_facts
            WHERE company_id = %s
              AND fiscal_year = %s
              AND period_type = 'quarter'
              AND fiscal_quarter = ANY(%s)
              AND statement = 'income_statement'
              AND extraction_version = %s
              AND superseded_at IS NULL
              AND dimension_type IS NULL
            GROUP BY concept
            """,
            (company_id, fiscal_year, other_quarters, SOURCE_EXTRACTION_VERSION),
        )
        for concept, total in cur.fetchall():
            sums[concept] = total

    return annual, sums, annual_raw_response_id


def fetch_current_target_q_rows(
    conn: psycopg.Connection,
    *,
    company_id: int,
    fiscal_year: int,
    target_quarter: int,
) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, concept, value, period_end, period_type,
                   fiscal_period_label, calendar_year, calendar_quarter,
                   calendar_period_label, published_at, source_raw_response_id,
                   unit, extraction_version
            FROM financial_facts
            WHERE company_id = %s
              AND fiscal_year = %s
              AND fiscal_quarter = %s
              AND period_type = 'quarter'
              AND statement = 'income_statement'
              AND superseded_at IS NULL
              AND dimension_type IS NULL
            ORDER BY concept
            """,
            (company_id, fiscal_year, target_quarter),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ticker", required=True)
    parser.add_argument("--fiscal-year", type=int, required=True)
    parser.add_argument("--fiscal-quarter", type=int, default=4, choices=(1, 2, 3, 4),
                        help="Quarter to derive (default 4).")
    parser.add_argument("--apply", action="store_true",
                        help="Write the corrections; default is a dry-run preview.")
    args = parser.parse_args()

    ticker = args.ticker.upper()
    fiscal_year = args.fiscal_year
    target_q = args.fiscal_quarter
    other_q_label = "+".join(f"Q{q}" for q in (1, 2, 3, 4) if q != target_q)

    with get_conn() as conn:
        company_id, fye_md = fetch_company(conn, ticker)
        annual, sums, annual_raw = fetch_annual_and_priors(
            conn, company_id=company_id, fiscal_year=fiscal_year, target_quarter=target_q,
        )
        if not annual:
            print(f"No annual IS rows found for {ticker} FY{fiscal_year}.")
            return

        current_q = fetch_current_target_q_rows(
            conn, company_id=company_id, fiscal_year=fiscal_year, target_quarter=target_q,
        )
        if not current_q:
            print(f"No current Q{target_q} IS rows found for {ticker} FY{fiscal_year}.")
            return

        already_derived = [r for r in current_q if r["extraction_version"] == DERIVED_EXTRACTION_VERSION]
        if already_derived:
            print(
                f"{ticker} FY{fiscal_year} Q{target_q} already at "
                f"{DERIVED_EXTRACTION_VERSION} ({len(already_derived)} rows). "
                "Nothing to do."
            )
            return

        # Build the corrected row plan
        plan: list[dict] = []
        for row in current_q:
            concept = row["concept"]
            if concept not in SUMMABLE_FLOW_CONCEPTS:
                continue
            other_sum = sums.get(concept, Decimal(0))
            if other_sum == 0:
                # Concept not actively reported quarterly for this filer
                continue
            annual_val = annual.get(concept)
            if annual_val is None:
                continue
            derived = Decimal(annual_val) - Decimal(other_sum)
            plan.append({
                "old_id": row["id"],
                "concept": concept,
                "old_value": row["value"],
                "annual": annual_val,
                "other_sum": other_sum,
                "derived": derived,
                "period_end": row["period_end"],
                "fiscal_period_label": row["fiscal_period_label"],
                "calendar_year": row["calendar_year"],
                "calendar_quarter": row["calendar_quarter"],
                "calendar_period_label": row["calendar_period_label"],
                "published_at": row["published_at"],
                "source_raw_response_id": annual_raw,
                "unit": row["unit"],
            })

        if not plan:
            print(f"No correctable concepts found (no {other_q_label} sums to derive from).")
            return

        print(f"{ticker} FY{fiscal_year} Q{target_q} IS corrections:\n")
        print(
            f"{'concept':<35}{'old_value':>20}{'annual':>20}"
            f"{'sum_'+other_q_label:>20}{'derived_q'+str(target_q):>20}"
        )
        for p in plan:
            print(
                f"{p['concept']:<35}"
                f"{float(p['old_value']):>20,.0f}"
                f"{float(p['annual']):>20,.0f}"
                f"{float(p['other_sum']):>20,.0f}"
                f"{float(p['derived']):>20,.0f}"
            )

        if not args.apply:
            print("\nDry run. Pass --apply to write.")
            return

        run_id = open_run(
            conn,
            run_kind="manual",
            vendor="arrow",
            ticker_scope=[ticker],
        )
        with conn.transaction(), conn.cursor() as cur:
            for p in plan:
                # Supersede the corrupt row
                cur.execute(
                    """
                    UPDATE financial_facts
                    SET superseded_at = now(),
                        supersession_reason = %s
                    WHERE id = %s AND superseded_at IS NULL
                    """,
                    (SUPERSESSION_REASON, p["old_id"]),
                )
                # Insert the derived row
                cur.execute(
                    """
                    INSERT INTO financial_facts (
                        ingest_run_id, company_id, statement, concept, value, unit,
                        fiscal_year, fiscal_quarter, fiscal_period_label,
                        period_end, period_type,
                        calendar_year, calendar_quarter, calendar_period_label,
                        published_at, source_raw_response_id,
                        extraction_version, supersedes_fact_id, supersession_reason
                    )
                    VALUES (%s, %s, 'income_statement', %s, %s, %s,
                            %s, %s, %s, %s, 'quarter',
                            %s, %s, %s, %s, %s,
                            %s, %s, %s)
                    """,
                    (
                        run_id, company_id, p["concept"], p["derived"], p["unit"],
                        fiscal_year, target_q, p["fiscal_period_label"],
                        p["period_end"],
                        p["calendar_year"], p["calendar_quarter"], p["calendar_period_label"],
                        p["published_at"], p["source_raw_response_id"],
                        DERIVED_EXTRACTION_VERSION, p["old_id"], SUPERSESSION_REASON,
                    ),
                )
        close_succeeded(
            conn,
            run_id,
            counts={
                "is_facts_written": len(plan),
                "is_facts_superseded": len(plan),
                "ticker": ticker,
                "fiscal_year": fiscal_year,
                "fiscal_quarter": target_q,
            },
        )
        print(f"\nWrote {len(plan)} derived rows; superseded {len(plan)} corrupt rows. Run id={run_id}.")


if __name__ == "__main__":
    main()
