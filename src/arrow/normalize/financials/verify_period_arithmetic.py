"""Layer 3 verification — period arithmetic.

For every fiscal year where we have all four quarters plus an annual
row, check that `Q1 + Q2 + Q3 + Q4 ≈ FY` for each flow bucket.

Mismatches catch bugs that Layer 1 can't:
  - Mis-labeled quarter (e.g., two filings both tagged Q3)
  - Missing quarter (FMP silently returned 3 instead of 4)
  - Concept mis-mapping (same FMP field mapped to two canonical buckets)
  - Restatement drift (quarter values from pre-restatement, FY from post-)

Per verification.md Layer 3.

Balance-sheet stocks are NOT checked (snapshot semantics — never summed).
Per-share and share-count buckets are NOT checked (not additive across
quarters). Only USD-magnitude flow buckets get the identity check.

Same tolerance as Layer 1 (verify_is): max($1M, 0.1% of larger abs).
HARD BLOCK on failure.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal

import psycopg

from arrow.normalize.financials.verify_is import TOLERANCE_ABSOLUTE, TOLERANCE_PCT

# Flow buckets eligible for the Q1+Q2+Q3+Q4 ≈ FY identity.
# Excludes BS stocks (not in IS anyway), per-share, and share-counts.
_FLOW_BUCKETS = {
    "revenue", "cogs", "gross_profit",
    "rd", "sga", "total_opex", "operating_income",
    "interest_expense", "interest_income",
    "ebt_incl_unusual", "tax",
    "continuing_ops_after_tax", "discontinued_ops", "net_income",
}


@dataclass(frozen=True)
class PeriodArithmeticFailure:
    company_id: int
    concept: str
    fiscal_year: int
    quarters_sum: Decimal
    annual: Decimal
    delta: Decimal
    tolerance: Decimal


def _within_tolerance(a: Decimal, b: Decimal) -> tuple[bool, Decimal, Decimal]:
    delta = abs(a - b)
    threshold = max(
        TOLERANCE_ABSOLUTE,
        max(abs(a), abs(b)) * TOLERANCE_PCT,
    )
    return delta <= threshold, delta, threshold


def verify_period_arithmetic(
    conn: psycopg.Connection,
    *,
    company_id: int,
    extraction_version: str,
) -> list[PeriodArithmeticFailure]:
    """Check Q1+Q2+Q3+Q4 ≈ FY for every (concept, fiscal_year) with all five.

    Reads from financial_facts (current rows only, matching extraction_version).
    Returns list of failures (empty = all checkable identities passed).
    Fiscal years missing any of the five values are skipped (not failed).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT concept, fiscal_year, fiscal_quarter, period_type, value
            FROM financial_facts
            WHERE company_id = %s
              AND extraction_version = %s
              AND superseded_at IS NULL
              AND statement = 'income_statement'
              AND concept = ANY(%s);
            """,
            (company_id, extraction_version, list(_FLOW_BUCKETS)),
        )
        rows = cur.fetchall()

    # (concept, fiscal_year) -> {"Q1": val, "Q2": val, "Q3": val, "Q4": val, "FY": val}
    shape: dict[tuple[str, int], dict[str, Decimal]] = defaultdict(dict)
    for concept, fy, fq, period_type, value in rows:
        key = (concept, fy)
        if period_type == "annual":
            shape[key]["FY"] = value
        elif period_type == "quarter" and fq is not None:
            shape[key][f"Q{fq}"] = value

    failures: list[PeriodArithmeticFailure] = []
    for (concept, fy), components in shape.items():
        required = {"Q1", "Q2", "Q3", "Q4", "FY"}
        if not required.issubset(components.keys()):
            continue  # incomplete set — skip, not fail
        quarters_sum = (
            components["Q1"] + components["Q2"] + components["Q3"] + components["Q4"]
        )
        annual = components["FY"]
        ok, delta, threshold = _within_tolerance(quarters_sum, annual)
        if not ok:
            failures.append(
                PeriodArithmeticFailure(
                    company_id=company_id,
                    concept=concept,
                    fiscal_year=fy,
                    quarters_sum=quarters_sum,
                    annual=annual,
                    delta=delta,
                    tolerance=threshold,
                )
            )

    return failures
