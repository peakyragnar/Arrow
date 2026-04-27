"""Balance-sheet Layer 1 verification.

Two classes:

- SOFT subtotal-component drift: FMP's reported subtotal disagrees with the
  sum of FMP's own normalized component fields inside the same shipped row.
  Load the row verbatim; write a flag for analyst review.
- HARD balance identity: liabilities/equity identities that a valid balance
  sheet cannot violate. Abort ingest if these fail.

Absent-component handling differs from IS. On the balance sheet many
canonical buckets are legitimately absent for a given filer (e.g., no
preferred stock, no discontinued ops, no restricted cash), so "skip if
any component missing" would skip most ties. Instead, we treat any
mapped-but-missing component as **0** in the sum — consistent with
concepts.md § 8 "If a detail component is NULL and audited-absent,
treat as zero in the subtotal." When the sum then doesn't tie, that's
a genuine discrepancy (either our mapping is incomplete or FMP's
subtotal is internally inconsistent).

Tolerance: max($1M, 0.1% of larger abs) — same as Layer 1 IS, same
rationale (single-line ±$0.5M filing rounding × ~2-3 values in each
tie).
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from arrow.normalize.financials.verify_is import TOLERANCE_ABSOLUTE, TOLERANCE_PCT


@dataclass(frozen=True)
class TieFailure:
    tie: str
    filer: Decimal
    computed: Decimal
    delta: Decimal
    tolerance: Decimal


def _within_tolerance(filer: Decimal, computed: Decimal) -> tuple[bool, Decimal, Decimal]:
    delta = abs(filer - computed)
    threshold = max(
        TOLERANCE_ABSOLUTE,
        max(abs(filer), abs(computed)) * TOLERANCE_PCT,
    )
    return delta <= threshold, delta, threshold


def _val(values: dict[str, Decimal], concept: str) -> Decimal:
    """Treat absent mapped components as zero (see module docstring)."""
    return values.get(concept, Decimal("0"))


# Each tie: (name, subtotal_name, [(component, sign)])
# Computed value = sum(components × signs); tie passes if
# computed ≈ values[subtotal_name] within tolerance.
# FMP-SOURCED TIES (Layer 1 when FMP is the source):
# concepts.md § 5 describes the FULL economic vocabulary — every GAAP
# balance-sheet line analysts might query. Several of those concepts
# (restricted_cash_current, income_taxes_receivable_current,
# short_term_borrowings, current_portion_leases_finance, ROU_operating,
# equity_method_investments, long_term_leases_finance) are NOT separately
# exposed by FMP — FMP bundles them into higher-level aggregates we
# already sum here. Including them in these tie formulas would cause
# strict-coverage failures on every filer without adding integrity value,
# since the bundled aggregate already reflects them.
#
# The ties below reflect what's actually verifiable against FMP's data
# model. When SEC XBRL direct ingest lands (Build Order step 19), a
# parallel set of ties with the full concept set becomes appropriate.
_BS_SOFT_TIES: list[tuple[str, str, list[tuple[str, int]]]] = [
    (
        "total_current_assets == cash + sti + AR + other_receivables + inventory + prepaid + other_current_assets",
        "total_current_assets",
        [
            # FMP bundles restricted_cash_current into cash_and_equivalents
            # (observed: DELL FY26 Q2 has RestrictedCashCurrent=$146M reported
            # in XBRL but folded into FMP's cash, +$146M delta in tie — see
            # also LYB per deterministic-flow notes). Similarly, FMP folds
            # income_taxes_receivable_current into otherCurrentAssets.
            ("cash_and_equivalents", +1),
            ("short_term_investments", +1),
            ("accounts_receivable", +1),
            ("other_receivables", +1),
            ("inventory", +1),
            ("prepaid_expenses", +1),
            ("other_current_assets", +1),
        ],
    ),
    (
        "total_assets == total_current_assets + net_ppe + LT_investments + goodwill + intangibles + DTA_noncurrent + other_noncurrent",
        "total_assets",
        [
            # FMP doesn't separately expose right_of_use_assets_operating or
            # equity_method_investments. ROU assets are typically folded into
            # propertyPlantEquipmentNet or otherNonCurrentAssets. Equity-method
            # investments are folded into longTermInvestments or otherNonCurrent.
            ("total_current_assets", +1),
            ("net_ppe", +1),
            ("long_term_investments", +1),
            ("goodwill", +1),
            ("other_intangibles", +1),
            ("deferred_tax_assets_noncurrent", +1),
            ("other_noncurrent_assets", +1),
        ],
    ),
    (
        "total_current_liabilities == AP + accrued + current_lt_debt + lease_current_op + deferred_rev_current + other_current",
        "total_current_liabilities",
        [
            # FMP bundles short_term_borrowings into shortTermDebt → which we
            # map to current_portion_lt_debt (single bucket). FMP doesn't split
            # finance vs operating lease current portions. And
            # income_taxes_payable_current is a detail of accounts_payable
            # (see fmp_bs_mapper.py:67 bundling accountPayables + otherPayables).
            ("accounts_payable", +1),
            ("accrued_expenses", +1),
            ("current_portion_lt_debt", +1),
            ("current_portion_leases_operating", +1),
            ("deferred_revenue_current", +1),
            ("other_current_liabilities", +1),
        ],
    ),
    (
        "total_liabilities == total_current_liab + LT_debt + LT_lease_op + deferred_rev_noncurrent + DTL + other_noncurrent_liab",
        "total_liabilities",
        [
            # FMP doesn't split finance vs operating leases in the noncurrent
            # portion either. The single operating-lease bucket is sufficient.
            ("total_current_liabilities", +1),
            ("long_term_debt", +1),
            ("long_term_leases_operating", +1),
            ("deferred_revenue_noncurrent", +1),
            ("deferred_tax_liability_noncurrent", +1),
            ("other_noncurrent_liabilities", +1),
        ],
    ),
    # FMP's three reported subtotals should be self-consistent:
    # totalLiabilitiesAndTotalEquity == totalLiabilities + totalEquity.
    # Empirically not always true. MU FY2020 Q3 returns
    # totalLiabilitiesAndTotalEquity = totalLiabilities + totalStockholdersEquity
    # (i.e., FMP populated the field as if it excludes NCI, which contradicts
    # Arrow's naming convention that totalEquity includes NCI). The delta equals
    # the noncontrolling_interest value exactly — IM Flash JV with Intel.
    # Treat as vendor-data drift (soft flag, route to steward) rather than
    # blocking ingest, because the real balance identity below
    # (total_assets == total_liabilities_and_equity) still holds and is the
    # accounting check that matters.
    (
        "total_liabilities_and_equity == total_liabilities + total_equity",
        "total_liabilities_and_equity",
        [
            ("total_liabilities", +1),
            ("total_equity", +1),
        ],
    ),
    # total_equity component sum. NOTE ON TREASURY SIGN:
    # concepts.md § 5.5 describes treasury_stock as "positive magnitude —
    # subtracted in the formula." fmp_mapping.md § 5.2 CLAIMED FMP stores
    # it as positive magnitude. **Empirically that's false**: FMP returns
    # `treasuryStock` as SIGNED NEGATIVE (e.g., NVDA FY2022 Q3 returns
    # -12,038,000,000), and the filer-reported totalEquity balances only
    # when treasury is ADDED (i.e., included with its FMP-returned sign).
    # We store the FMP value as-is and add here — FMP's convention wins
    # per the "FMP is canonical" principle. Concepts.md + fmp_mapping.md
    # updated accordingly.
    (
        "total_equity == preferred + common_stock + APIC + treasury + retained_earnings + AOCI + other_equity + NCI",
        "total_equity",
        [
            ("preferred_stock", +1),
            ("common_stock", +1),
            ("additional_paid_in_capital", +1),
            ("treasury_stock", +1),  # FMP stores signed (negative for buybacks)
            ("retained_earnings", +1),
            ("accumulated_other_comprehensive_income", +1),
            ("other_equity", +1),  # FMP reconciliation plug; see concepts.md § 5.5
            ("noncontrolling_interest", +1),
        ],
    ),
]

_BS_HARD_TIES: list[tuple[str, str, list[tuple[str, int]]]] = [
    # THE BALANCE — the identity that gives the balance sheet its name.
    # The vendor-consistency tie (TLE == TL + TE) lives in _BS_SOFT_TIES
    # because FMP's three reported subtotals are not always internally
    # consistent (see the comment block above tie #6 in _BS_SOFT_TIES).
    (
        "total_assets == total_liabilities_and_equity",
        "total_assets",
        [("total_liabilities_and_equity", +1)],
    ),
]


def _verify_ties(
    ties: list[tuple[str, str, list[tuple[str, int]]]],
    values_by_concept: dict[str, Decimal],
) -> list[TieFailure]:
    failures: list[TieFailure] = []
    for name, subtotal, components in ties:
        if subtotal not in values_by_concept:
            continue
        filer = values_by_concept[subtotal]
        computed = sum(
            (_val(values_by_concept, c) * s for c, s in components),
            start=Decimal("0"),
        )
        ok, delta, threshold = _within_tolerance(filer, computed)
        if not ok:
            failures.append(
                TieFailure(
                    tie=name,
                    filer=filer,
                    computed=computed,
                    delta=delta,
                    tolerance=threshold,
                )
            )
    return failures


def verify_bs_ties(values_by_concept: dict[str, Decimal]) -> list[TieFailure]:
    """Return every failing BS tie, hard + soft.

    Retained for audit callers and tests that want the full picture.
    """
    return verify_bs_soft_ties(values_by_concept) + verify_bs_hard_ties(values_by_concept)


def verify_bs_soft_ties(values_by_concept: dict[str, Decimal]) -> list[TieFailure]:
    """Return failing BS subtotal-component ties.

    If the filer-reported subtotal itself is absent, the tie is skipped —
    there's nothing reported to validate. Component buckets absent from the
    FMP mapping contribute zero, because FMP legitimately bundles a number of
    concepts into broader aggregates (see fmp_mapping.md § 5.4).
    """
    return _verify_ties(_BS_SOFT_TIES, values_by_concept)


def verify_bs_hard_ties(values_by_concept: dict[str, Decimal]) -> list[TieFailure]:
    """Return failing BS balance-identity ties."""
    return _verify_ties(_BS_HARD_TIES, values_by_concept)
