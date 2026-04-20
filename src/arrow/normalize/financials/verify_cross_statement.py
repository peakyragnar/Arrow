"""Layer 2 — cross-statement tie verification.

Once IS, BS, and CF are all loaded for the same (company, period_end,
period_type), these invariants must hold. Per verification.md § 3.1
with pragmatic adjustments based on live-data findings:

    (1) bs.total_assets == bs.total_liabilities_and_equity
        ← already enforced by Layer 1 BS balance identity.

    (2) cf.net_income_start ≈ is.net_income      (±$1M filing rounding)

    (3)-(5) CASH ROLL-FORWARD TIES: DEFERRED pending restricted-cash
        mapping. Under ASC 230 (post-2018), CF's cash_end_of_period
        includes "cash, cash equivalents, AND restricted cash" — but
        FMP's balance-sheet endpoint doesn't separately expose
        restricted cash. So the tie `cf.cash_end == bs.cash_and_equiv`
        fails by the restricted-cash delta on filers that have any
        (~$99M for NVDA FY2024 Q2; varies by filer). The correct tie
        is `cf.cash_end == bs.cash_and_equiv + bs.restricted_cash`,
        which we can enforce only once restricted cash is mapped from
        SEC XBRL direct (Build Order ~step 19). Until then, trust is
        provided by: Layer 3 CF subtotal period arithmetic + Layer 5
        CFO/CFI/CFF anchor match against XBRL, which together catch
        CF errors without requiring the BS↔CF cash tie.

Tolerance for tie (2): max($1M, 0.1% of larger abs) — identical to
Layer 1 filing rounding. Empirically NVDA shows 1-2 periods per decade
with $1M CF.netIncome vs IS.netIncome drift, which is filing-level
rounding rather than a data integrity issue.

HARD BLOCK on any failure.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

import psycopg

from arrow.normalize.financials.verify_is import TOLERANCE_ABSOLUTE, TOLERANCE_PCT


@dataclass(frozen=True)
class CrossStatementFailure:
    tie: str
    period_end: date
    period_type: str
    fiscal_year: int
    fiscal_quarter: int | None
    lhs_value: Decimal
    rhs_value: Decimal
    delta: Decimal
    tolerance: Decimal


def _within(lhs: Decimal, rhs: Decimal, tolerance: Decimal) -> tuple[bool, Decimal]:
    delta = abs(lhs - rhs)
    return delta <= tolerance, delta


def _lookup_fact(
    conn: psycopg.Connection,
    *,
    company_id: int,
    concept: str,
    period_end: date,
    period_type: str,
    extraction_version: str,
) -> Decimal | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT value FROM financial_facts
            WHERE company_id = %s AND concept = %s
              AND period_end = %s AND period_type = %s
              AND extraction_version = %s
              AND superseded_at IS NULL
            LIMIT 1;
            """,
            (company_id, concept, period_end, period_type, extraction_version),
        )
        row = cur.fetchone()
    return row[0] if row else None


def verify_cross_statement_ties(
    conn: psycopg.Connection,
    *,
    company_id: int,
    is_extraction_version: str,
    bs_extraction_version: str,  # accepted for API stability; unused until restricted-cash lands
    cf_extraction_version: str,
) -> list[CrossStatementFailure]:
    """For each (period_end, period_type) where we have CF data, verify
    the cross-statement ties. Returns list of failures (empty = clean)."""
    failures: list[CrossStatementFailure] = []

    # Pull every CF period for this company. We anchor on CF because
    # all four ties involve a CF value.
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT period_end, period_type, fiscal_year, fiscal_quarter
            FROM financial_facts
            WHERE company_id = %s
              AND statement = 'cash_flow'
              AND extraction_version = %s
              AND superseded_at IS NULL
            ORDER BY period_end, period_type;
            """,
            (company_id, cf_extraction_version),
        )
        cf_periods = cur.fetchall()

    for period_end, period_type, fy, fq in cf_periods:
        is_ni = _lookup_fact(
            conn, company_id=company_id, concept="net_income",
            period_end=period_end, period_type=period_type,
            extraction_version=is_extraction_version,
        )
        cf_ni_start = _lookup_fact(
            conn, company_id=company_id, concept="net_income_start",
            period_end=period_end, period_type=period_type,
            extraction_version=cf_extraction_version,
        )

        # Tie (2): cf.net_income_start ≈ is.net_income
        # Tolerance = Layer 1 (±$1M filing rounding). Two independently-
        # rounded reports of the same filer-level number can drift by the
        # typical ±$0.5M rounding × 2 = ±$1M.
        if is_ni is not None and cf_ni_start is not None:
            threshold = max(
                TOLERANCE_ABSOLUTE,
                max(abs(cf_ni_start), abs(is_ni)) * TOLERANCE_PCT,
            )
            ok, delta = _within(cf_ni_start, is_ni, threshold)
            if not ok:
                failures.append(CrossStatementFailure(
                    tie="cf.net_income_start ≈ is.net_income",
                    period_end=period_end, period_type=period_type,
                    fiscal_year=fy, fiscal_quarter=fq,
                    lhs_value=cf_ni_start, rhs_value=is_ni,
                    delta=delta, tolerance=threshold,
                ))

        # Ties (3)/(4)/(5) — cash roll-forward — are DEFERRED pending
        # restricted-cash mapping. See module docstring.
        continue

    return failures
