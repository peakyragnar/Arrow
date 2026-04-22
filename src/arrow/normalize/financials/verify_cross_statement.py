"""Layer 2 — cross-statement tie verification.

SCAFFOLD — currently has no caller in mainline ingest or the audit side
rail. Per ADR-0010, default `backfill_fmp_statements` runs Layer 1 only.
This module exists as the spec/implementation to be re-wired if Layer 2
is reactivated as a side-rail audit layer; when it is, failures should
soft-flag (write `data_quality_flags` of type `layer2_cross_statement`),
not hard-block.

Once IS, BS, and CF are all loaded for the same (company, period_end,
period_type), these invariants must hold. Per verification.md § 3.1,
with restricted-cash sourced from SEC XBRL to make ASC 230 work:

    (1) bs.total_assets == bs.total_liabilities_and_equity
        ← already enforced by Layer 1 BS balance identity.

    (2) cf.net_income_start ≈ is.net_income              (±$1M filing rounding)

    (3) cf.cash_end_of_period ≈ bs.cash_and_equivalents[t] + xbrl.restricted_cash[t]
                                                         (±$1M filing rounding)

    (4) cf.cash_begin_of_period ≈ bs.cash_and_equivalents[t-1] + xbrl.restricted_cash[t-1]
                                                         (±$1M filing rounding)

    (5) cf.net_change_in_cash ≈ (bs.cash[t] + restricted[t]) − (bs.cash[t-1] + restricted[t-1])
                                                         (±$1M filing rounding)

ASC 230 restricted-cash handling: the CF's "cash, cash equivalents, and
restricted cash" definition (post-2018 amendments) differs from BS's
cash_and_equivalents. FMP doesn't expose restricted cash on its
balance-sheet endpoint. When Layer 2 runs as a side rail, it relies on
the SEC XBRL companyfacts payload the Layer 5 audit fetches — this
module does not fetch anything itself; the caller passes `companyfacts`.
Tags consulted, in order: RestrictedCashCurrent + RestrictedCashNoncurrent
(sum), then RestrictedCashAndCashEquivalentsAtCarryingValue (combined),
then 0 (filer doesn't report any restricted cash).

"Prior period" lookup: for a given (company, period_end), find the
largest period_end strictly less than it in the same company's BS rows.
For the first period in our validated window, there's no prior BS in
our data — ties (4) and (5) are SKIPPED (not failed) for that period.

Tolerance: ±$1M absolute or 0.1% of larger, same as Layer 1 —
two-independently-rounded-reports-of-the-same-filer-number can drift
that much in practice even on "same stored number" ties.

Failure behavior: this function RETURNS a list of failures; it does not
raise. When this layer is re-wired, the caller should write
`data_quality_flags` rows, not roll back ingest.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

import psycopg

from arrow.normalize.financials.verify_is import TOLERANCE_ABSOLUTE, TOLERANCE_PCT

# XBRL tags consulted for restricted cash at a given BS snapshot date.
# Tried in order; first match wins.
_RESTRICTED_CASH_TAGS_CURRENT = ("RestrictedCashCurrent",)
_RESTRICTED_CASH_TAGS_NONCURRENT = ("RestrictedCashNoncurrent",)
_RESTRICTED_CASH_COMBINED_TAGS = (
    "RestrictedCashAndCashEquivalents",
    "RestrictedCashAndCashEquivalentsAtCarryingValue",
)


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


def _filing_tolerance(lhs: Decimal, rhs: Decimal) -> Decimal:
    return max(
        TOLERANCE_ABSOLUTE,
        max(abs(lhs), abs(rhs)) * TOLERANCE_PCT,
    )


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


def _lookup_prior_bs_cash(
    conn: psycopg.Connection,
    *,
    company_id: int,
    period_end: date,
    period_type: str,
    extraction_version: str,
) -> tuple[date, Decimal] | None:
    """Find the most recent BS cash snapshot for this company strictly
    before `period_end`, matching the same period_type."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT period_end, value FROM financial_facts
            WHERE company_id = %s
              AND concept = 'cash_and_equivalents'
              AND period_type = %s
              AND extraction_version = %s
              AND superseded_at IS NULL
              AND period_end < %s
            ORDER BY period_end DESC
            LIMIT 1;
            """,
            (company_id, period_type, extraction_version, period_end),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return row[0], row[1]


def _xbrl_instant_value_at(
    us_gaap: dict[str, Any], tag: str, end: date,
) -> Decimal | None:
    """Return the latest-filed instant-type XBRL value for `tag` at `end`."""
    entries = us_gaap.get(tag, {}).get("units", {}).get("USD", [])
    target = end.isoformat()
    candidates = [
        e for e in entries
        if e.get("end") == target and not e.get("start")
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda e: e.get("filed", ""), reverse=True)
    return Decimal(str(candidates[0]["val"]))


def _xbrl_restricted_cash_at(
    companyfacts: dict[str, Any] | None, end: date,
) -> Decimal:
    """Pull filer's restricted cash from SEC XBRL at the given BS date.

    Tries separate Current + Noncurrent first; falls back to the combined
    tag; returns 0 if the filer doesn't report any restricted cash.
    """
    if not companyfacts:
        return Decimal("0")
    us_gaap = companyfacts.get("facts", {}).get("us-gaap", {})

    total = Decimal("0")
    found_any = False
    for tag in _RESTRICTED_CASH_TAGS_CURRENT + _RESTRICTED_CASH_TAGS_NONCURRENT:
        v = _xbrl_instant_value_at(us_gaap, tag, end)
        if v is not None:
            total += v
            found_any = True
    if found_any:
        return total

    for tag in _RESTRICTED_CASH_COMBINED_TAGS:
        v = _xbrl_instant_value_at(us_gaap, tag, end)
        if v is not None:
            return v

    return Decimal("0")


def verify_cross_statement_ties(
    conn: psycopg.Connection,
    *,
    company_id: int,
    is_extraction_version: str,
    bs_extraction_version: str,
    cf_extraction_version: str,
    companyfacts: dict[str, Any] | None = None,
) -> list[CrossStatementFailure]:
    """Layer 2 cross-statement verification.

    `companyfacts` should be the SEC XBRL payload already fetched for
    Layer 5 reconciliation. It's used here to look up restricted cash
    at BS snapshot dates (FMP doesn't expose it). If `companyfacts` is
    None, cash ties assume restricted=0; for filers with non-zero
    restricted cash this will fail the tie, which is the correct
    surfacing signal.
    """
    failures: list[CrossStatementFailure] = []

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
        bs_cash = _lookup_fact(
            conn, company_id=company_id, concept="cash_and_equivalents",
            period_end=period_end, period_type=period_type,
            extraction_version=bs_extraction_version,
        )
        cf_cash_end = _lookup_fact(
            conn, company_id=company_id, concept="cash_end_of_period",
            period_end=period_end, period_type=period_type,
            extraction_version=cf_extraction_version,
        )
        cf_cash_begin = _lookup_fact(
            conn, company_id=company_id, concept="cash_begin_of_period",
            period_end=period_end, period_type=period_type,
            extraction_version=cf_extraction_version,
        )
        cf_net_change = _lookup_fact(
            conn, company_id=company_id, concept="net_change_in_cash",
            period_end=period_end, period_type=period_type,
            extraction_version=cf_extraction_version,
        )

        # Tie (2): cf.net_income_start ≈ is.net_income
        if is_ni is not None and cf_ni_start is not None:
            threshold = _filing_tolerance(cf_ni_start, is_ni)
            ok, delta = _within(cf_ni_start, is_ni, threshold)
            if not ok:
                failures.append(CrossStatementFailure(
                    tie="cf.net_income_start ≈ is.net_income",
                    period_end=period_end, period_type=period_type,
                    fiscal_year=fy, fiscal_quarter=fq,
                    lhs_value=cf_ni_start, rhs_value=is_ni,
                    delta=delta, tolerance=threshold,
                ))

        # ASC 230 full cash at this BS date (cash + restricted)
        restricted_cash_t = _xbrl_restricted_cash_at(companyfacts, period_end)

        # Tie (3): cf.cash_end_of_period ≈ bs.cash + xbrl_restricted
        if bs_cash is not None and cf_cash_end is not None:
            rhs = bs_cash + restricted_cash_t
            threshold = _filing_tolerance(cf_cash_end, rhs)
            ok, delta = _within(cf_cash_end, rhs, threshold)
            if not ok:
                failures.append(CrossStatementFailure(
                    tie="cf.cash_end_of_period ≈ bs.cash + xbrl.restricted_cash[t]",
                    period_end=period_end, period_type=period_type,
                    fiscal_year=fy, fiscal_quarter=fq,
                    lhs_value=cf_cash_end, rhs_value=rhs,
                    delta=delta, tolerance=threshold,
                ))

        # Ties (4) and (5) need prior BS cash.
        prior = _lookup_prior_bs_cash(
            conn, company_id=company_id, period_end=period_end,
            period_type=period_type, extraction_version=bs_extraction_version,
        )
        if prior is None:
            continue
        prior_period_end, prior_bs_cash = prior
        restricted_cash_prior = _xbrl_restricted_cash_at(companyfacts, prior_period_end)

        # Tie (4): cf.cash_begin_of_period ≈ bs.cash[t-1] + xbrl.restricted_cash[t-1]
        if cf_cash_begin is not None:
            rhs = prior_bs_cash + restricted_cash_prior
            threshold = _filing_tolerance(cf_cash_begin, rhs)
            ok, delta = _within(cf_cash_begin, rhs, threshold)
            if not ok:
                failures.append(CrossStatementFailure(
                    tie=f"cf.cash_begin_of_period ≈ bs.cash[{prior_period_end}] + xbrl.restricted_cash[{prior_period_end}]",
                    period_end=period_end, period_type=period_type,
                    fiscal_year=fy, fiscal_quarter=fq,
                    lhs_value=cf_cash_begin, rhs_value=rhs,
                    delta=delta, tolerance=threshold,
                ))

        # Tie (5): cf.net_change_in_cash ≈ (cash+restricted)[t] − (cash+restricted)[t-1]
        if bs_cash is not None and cf_net_change is not None:
            expected_change = (bs_cash + restricted_cash_t) - (prior_bs_cash + restricted_cash_prior)
            threshold = _filing_tolerance(cf_net_change, expected_change)
            ok, delta = _within(cf_net_change, expected_change, threshold)
            if not ok:
                failures.append(CrossStatementFailure(
                    tie=f"cf.net_change_in_cash ≈ (bs.cash+restricted)[t] − (bs.cash+restricted)[{prior_period_end}]",
                    period_end=period_end, period_type=period_type,
                    fiscal_year=fy, fiscal_quarter=fq,
                    lhs_value=cf_net_change, rhs_value=expected_change,
                    delta=delta, tolerance=threshold,
                ))

    return failures
