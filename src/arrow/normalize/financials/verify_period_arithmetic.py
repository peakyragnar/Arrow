"""Layer 3 verification — period arithmetic.

SIDE RAIL — not wired into default FMP ingest.

Called only by `arrow.agents.amendment_detect.detect_and_apply_amendments`
(the audit/amendment side rail) and its tests. `backfill_fmp_statements`
runs Layer 1 only; period-arithmetic checking happens when the amendment
agent is invoked separately, per ADR-0010.

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

Tolerance: max($2.5M, 0.1% of larger abs) — wider than Layer 1 because
the identity compounds five independently-rounded values.

Failure behavior: this function RETURNS a list of failures; it does not
raise. The amendment agent decides whether to resolve (via XBRL
supersession inside a savepoint) or to write a `data_quality_flags` row
of type `layer3_q_sum_vs_fy`. Never hard-blocks.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal

import psycopg

# Layer 3 tolerance is wider than Layer 1 because the identity sums FIVE
# independently-rounded values (four quarterly discretes + the filer's
# own reported annual). Each value is typically rounded to the nearest
# $1M at filing time (±$0.5M noise), so the max expected rounding drift
# on the identity is 5 × $0.5M = $2.5M. We saw this empirically on
# NVDA FY2021 SG&A: SEC's XBRL itself reports Q1=$293M, Q2=$627M,
# Q3=$515M, Q4_derived=$503M summing to $1,938M, vs reported FY=$1,940M —
# a $2M delta that's entirely filer rounding (not an FMP or extraction
# bug). The wider floor absorbs this without missing genuine issues
# (anything beyond $2.5M is well above filer-rounding noise).
LAYER3_TOLERANCE_ABSOLUTE = Decimal("2500000")  # $2.5M
LAYER3_TOLERANCE_PCT = Decimal("0.001")         # 0.1% of larger abs (same as L1)

# Flow buckets eligible for the Q1+Q2+Q3+Q4 ≈ FY identity.
# IS: excludes per-share + share-counts (not additive).
# CF: excludes cash_begin/cash_end (snapshots, not flows) and the raw
# roll-forward components that are themselves subtotals but would
# double-count if we summed each subtotal AND each detail.
_IS_FLOW_BUCKETS = {
    "revenue", "cogs", "gross_profit",
    "rd", "sga", "total_opex", "operating_income",
    "interest_expense", "interest_income",
    "ebt_incl_unusual", "tax",
    "continuing_ops_after_tax", "discontinued_ops", "net_income",
}

# CF Layer 3: subtotals only. The detail-level line items within
# CFO/CFI/CFF are empirically RECLASSIFIED between the 10-Q and 10-K
# (e.g., NVDA moves items between change_accounts_payable and
# change_other_working_capital; classifies something as
# long_term_debt_issuance in the 10-K that was in other_financing in
# the 10-Qs). This is documented filing practice — see concepts.md
# § 7.1 on reclassification detection. The SUBTOTALS (CFO, CFI, CFF,
# net_change_in_cash) DO tie across Q1+Q2+Q3+Q4 = FY because they're
# normalized by FMP at the filing level. We enforce the identity on
# subtotals only; details are trusted per FMP's single-filing
# consistency + the subtotal tie above them.
_CF_FLOW_BUCKETS = {
    "cfo", "cfi", "cff", "net_change_in_cash",
}

# Backward-compat name for the IS set (used by older callers / tests).
_FLOW_BUCKETS = _IS_FLOW_BUCKETS


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
        LAYER3_TOLERANCE_ABSOLUTE,
        max(abs(a), abs(b)) * LAYER3_TOLERANCE_PCT,
    )
    return delta <= threshold, delta, threshold


def verify_period_arithmetic(
    conn: psycopg.Connection,
    *,
    company_id: int,
    extraction_version: str,
    statement: str = "income_statement",
) -> list[PeriodArithmeticFailure]:
    """Check Q1+Q2+Q3+Q4 ≈ FY for every (concept, fiscal_year) with all five.

    Reads from financial_facts (current rows only, matching extraction_version
    and statement). Returns list of failures (empty = all checkable
    identities passed). Fiscal years missing any of the five values are
    skipped (not failed).

    statement: 'income_statement' (default) or 'cash_flow'. Balance-sheet
    stocks are exempt from this check per concepts.md / verification.md.
    """
    if statement == "income_statement":
        flow_buckets = _IS_FLOW_BUCKETS
    elif statement == "cash_flow":
        flow_buckets = _CF_FLOW_BUCKETS
    else:
        raise ValueError(
            f"verify_period_arithmetic only defined for "
            f"'income_statement' or 'cash_flow', got {statement!r}"
        )

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT concept, fiscal_year, fiscal_quarter, period_type, value
            FROM financial_facts
            WHERE company_id = %s
              AND extraction_version = %s
              AND superseded_at IS NULL
              AND statement = %s
              AND concept = ANY(%s);
            """,
            (company_id, extraction_version, statement, list(flow_buckets)),
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
