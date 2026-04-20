"""Balance-sheet subtotal-tie verification (verification.md Layer 1, § 2.2)
plus the load-bearing BS invariant: total_assets == total_liabilities + total_equity.

HARD BLOCK. The load aborts the ingest run if any tie fails.

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
_BS_TIES: list[tuple[str, str, list[tuple[str, int]]]] = [
    (
        "total_current_assets == cash + sti + restricted_cash + AR + other_receivables + inventory + prepaid + itax_receivable + other_current_assets",
        "total_current_assets",
        [
            ("cash_and_equivalents", +1),
            ("short_term_investments", +1),
            ("restricted_cash_current", +1),
            ("accounts_receivable", +1),
            ("other_receivables", +1),
            ("inventory", +1),
            ("prepaid_expenses", +1),
            ("income_taxes_receivable_current", +1),
            ("other_current_assets", +1),
        ],
    ),
    (
        "total_assets == total_current_assets + net_ppe + ROU_operating + LT_investments + equity_method + goodwill + intangibles + DTA_noncurrent + other_noncurrent",
        "total_assets",
        [
            ("total_current_assets", +1),
            ("net_ppe", +1),
            ("right_of_use_assets_operating", +1),
            ("long_term_investments", +1),
            ("equity_method_investments", +1),
            ("goodwill", +1),
            ("other_intangibles", +1),
            ("deferred_tax_assets_noncurrent", +1),
            ("other_noncurrent_assets", +1),
        ],
    ),
    (
        "total_current_liabilities == AP + accrued + current_lt_debt + ST_borrowings + lease_current_op + lease_current_fin + itax_payable + deferred_rev_current + other_current",
        "total_current_liabilities",
        [
            ("accounts_payable", +1),
            ("accrued_expenses", +1),
            ("current_portion_lt_debt", +1),
            ("short_term_borrowings", +1),
            ("current_portion_leases_operating", +1),
            ("current_portion_leases_finance", +1),
            ("income_taxes_payable_current", +1),
            ("deferred_revenue_current", +1),
            ("other_current_liabilities", +1),
        ],
    ),
    (
        "total_liabilities == total_current_liab + LT_debt + LT_lease_op + LT_lease_fin + deferred_rev_noncurrent + DTL + other_noncurrent_liab",
        "total_liabilities",
        [
            ("total_current_liabilities", +1),
            ("long_term_debt", +1),
            ("long_term_leases_operating", +1),
            ("long_term_leases_finance", +1),
            ("deferred_revenue_noncurrent", +1),
            ("deferred_tax_liability_noncurrent", +1),
            ("other_noncurrent_liabilities", +1),
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
        "total_equity == preferred + common_stock + APIC + treasury + retained_earnings + AOCI + NCI",
        "total_equity",
        [
            ("preferred_stock", +1),
            ("common_stock", +1),
            ("additional_paid_in_capital", +1),
            ("treasury_stock", +1),  # FMP stores signed (negative for buybacks)
            ("retained_earnings", +1),
            ("accumulated_other_comprehensive_income", +1),
            ("noncontrolling_interest", +1),
        ],
    ),
    (
        "total_liabilities_and_equity == total_liabilities + total_equity",
        "total_liabilities_and_equity",
        [
            ("total_liabilities", +1),
            ("total_equity", +1),
        ],
    ),
    # THE BALANCE — the identity that gives the balance sheet its name.
    (
        "total_assets == total_liabilities_and_equity",
        "total_assets",
        [("total_liabilities_and_equity", +1)],
    ),
]


def verify_bs_ties(values_by_concept: dict[str, Decimal]) -> list[TieFailure]:
    """Return the list of ties that failed (empty = all passed)."""
    failures: list[TieFailure] = []
    for name, subtotal, components in _BS_TIES:
        if subtotal not in values_by_concept:
            continue  # can't check if the reported subtotal itself is absent
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
