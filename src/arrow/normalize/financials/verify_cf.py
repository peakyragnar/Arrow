"""Cash-flow subtotal-tie verification (verification.md § 2.3).

HARD BLOCK. Ingest aborts on mismatch.

All CF buckets are stored with CASH-IMPACT SIGN per concepts.md § 2.2.
Subtotals are straight sums of their detail components (no per-item sign
inversion in formulas).

Ties checked:
    cfo  == net_income_start + all non-cash adjustments + all working-capital changes
    cfi  == sum of all investing components
    cff  == sum of all financing components
    net_change_in_cash == cfo + cfi + cff + fx + misc

Plus the cash roll-forward inside the CF itself:
    net_change_in_cash == cash_end_of_period - cash_begin_of_period

Absent-component handling: treat mapped-but-absent buckets as 0 (same as
Layer 1 BS). The CF has many optional buckets — most filers don't report
divestitures or special_dividends — and the subtotals should still tie.

Tolerance: max($1M, 0.1% of larger abs). Same as Layer 1 IS/BS.
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
    return values.get(concept, Decimal("0"))


# FMP-SOURCED TIES (see verify_bs.py header comment for rationale).
# concepts.md § 6 describes the full CF vocabulary. Several concepts
# (gain_on_sale_assets_cf, gain_on_sale_investments_cf, asset_writedown,
# change_deferred_revenue, change_income_taxes, divestitures,
# loans_originated, loans_collected, short_term_debt_repayment,
# long_term_debt_repayment, special_dividends_paid, misc_cf_adjustments)
# are NOT separately exposed by FMP — FMP bundles them into
# otherNonCashItems, otherWorkingCapital, otherInvestingActivities,
# net debt issuance figures, or commonDividendsPaid. The ties below
# reflect FMP's data model. SEC XBRL direct ingest (future) would use
# ties with the full component set.
_CF_TIES: list[tuple[str, str, list[tuple[str, int]]]] = [
    (
        "cfo == net_income_start + non-cash adjustments + working capital changes",
        "cfo",
        [
            ("net_income_start", +1),
            ("dna_cf", +1),
            ("sbc", +1),
            ("deferred_income_tax", +1),
            # FMP lumps gain_on_sale_*, asset_writedown into otherNonCashItems.
            ("other_noncash", +1),
            ("change_accounts_receivable", +1),
            ("change_inventory", +1),
            ("change_accounts_payable", +1),
            # FMP lumps change_deferred_revenue + change_income_taxes into
            # otherWorkingCapital.
            ("change_other_working_capital", +1),
        ],
    ),
    (
        "cfi == capex + acquisitions + investments + other_investing",
        "cfi",
        [
            ("capital_expenditures", +1),
            ("acquisitions", +1),
            # FMP lumps divestitures, loans_originated, loans_collected into
            # otherInvestingActivities.
            ("purchases_of_investments", +1),
            ("sales_of_investments", +1),
            ("other_investing", +1),
        ],
    ),
    (
        "cff == debt issuance + stock + dividends + other_financing",
        "cff",
        [
            # FMP exposes NET debt issuance (gross issuance - repayment) as
            # shortTermNetDebtIssuance and longTermNetDebtIssuance. No separate
            # repayment fields — so short_term_debt_repayment and
            # long_term_debt_repayment buckets stay unpopulated. Per
            # fmp_cf_mapper.py:65-69.
            ("short_term_debt_issuance", +1),
            ("long_term_debt_issuance", +1),
            # stock_issuance bundle includes commonStockIssuance +
            # netPreferredStockIssuance (see fmp_cf_mapper.py).
            ("stock_issuance", +1),
            ("stock_repurchase", +1),
            ("common_dividends_paid", +1),
            ("preferred_dividends_paid", +1),
            # special_dividends_paid not separately exposed by FMP.
            ("other_financing", +1),
        ],
    ),
    (
        "net_change_in_cash == cfo + cfi + cff + fx",
        "net_change_in_cash",
        [
            # misc_cf_adjustments not separately exposed by FMP; would be in fx
            # or a reconciling item when it exists.
            ("cfo", +1),
            ("cfi", +1),
            ("cff", +1),
            ("fx_effect_on_cash", +1),
        ],
    ),
    (
        "net_change_in_cash == cash_end_of_period - cash_begin_of_period",
        "net_change_in_cash",
        [
            ("cash_end_of_period", +1),
            ("cash_begin_of_period", -1),
        ],
    ),
]


def verify_cf_ties(values_by_concept: dict[str, Decimal]) -> list[TieFailure]:
    """Return the list of ties that failed (empty = all passed).

    If the filer-reported subtotal itself is absent, the tie is skipped.
    Component buckets absent from the FMP mapping contribute zero — many CF
    concepts are legitimately bundled into umbrella fields such as
    `other_noncash`, `other_investing`, or net debt issuance.
    """
    failures: list[TieFailure] = []
    for name, subtotal, components in _CF_TIES:
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
