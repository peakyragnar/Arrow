"""Cash-flow subtotal-tie verification (verification.md § 2.3).

The CF ties split into two classes by what they actually prove:

HARD ties — filer-level integrity. Failure means the cash-flow statement
itself is broken. Block ingest.
  - net_change_in_cash == cfo + cfi + cff + fx
  - net_change_in_cash == cash_end_of_period - cash_begin_of_period

SOFT ties — vendor bucketing consistency. Failure means FMP's reported
subtotal and FMP's reported components disagree inside a single row
(FMP's own normalization dropped or misbucketed some item). The filer's
own 10-Q is typically internally consistent; the defect lives in FMP.
Do not block — write a `data_quality_flags` row so the analyst can
review and `accept_as_is`, and keep loading.
  - cfo == sum of non-cash adjustments + working capital changes
  - cfi == sum of investing components
  - cff == sum of financing components

All CF buckets are stored with CASH-IMPACT SIGN per concepts.md § 2.2.
Subtotals are straight sums of their detail components (no per-item sign
inversion in formulas).

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


# ---------------------------------------------------------------------------
# Tie definitions. Tuple: (name, subtotal concept, list of (component, sign)).
# ---------------------------------------------------------------------------

# SOFT ties — vendor-bucketing consistency. These test whether FMP's reported
# subtotal agrees with the sum of FMP's own component fields, or whether the
# three section subtotals sum to the reported net change in cash. When they
# disagree, FMP has shipped a self-inconsistent row. The filer's 10-Q is
# typically fine (the cash roll-forward ties); the defect is FMP's
# decomposition or Q4-derivation. Write a flag, don't block.
#
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
_CF_SOFT_TIES: list[tuple[str, str, list[tuple[str, int]]]] = [
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
        # This tie is SOFT rather than HARD because the failure mode is
        # vendor-decomposition/Q4-derivation, not filer integrity. A real
        # filer-level CF inconsistency would also break the HARD cash
        # roll-forward tie (cash_end - cash_begin == net_change_in_cash),
        # which remains HARD. Empirical: AMD FY2017 Q4 and similar Q4
        # rows that FMP derives as FY − 9M can show a section-sum ≠
        # net-change mismatch while cash_end - cash_begin ties perfectly.
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
]

# HARD ties — filer-level integrity. A cash-flow statement that fails
# this is literally impossible for the filer to ship: the cash
# roll-forward is definitional. Block ingest on failure.
_CF_HARD_TIES: list[tuple[str, str, list[tuple[str, int]]]] = [
    (
        "net_change_in_cash == cash_end_of_period - cash_begin_of_period",
        "net_change_in_cash",
        [
            ("cash_end_of_period", +1),
            ("cash_begin_of_period", -1),
        ],
    ),
]


def _check_ties(
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


def verify_cf_hard_ties(values_by_concept: dict[str, Decimal]) -> list[TieFailure]:
    """Return failures among HARD ties (filer integrity). Empty = all passed.

    Hard-tie failure should hard-block the caller's transaction.
    """
    return _check_ties(_CF_HARD_TIES, values_by_concept)


def verify_cf_soft_ties(values_by_concept: dict[str, Decimal]) -> list[TieFailure]:
    """Return failures among SOFT ties (vendor bucketing). Empty = all passed.

    Soft-tie failure should NOT hard-block. Caller should write a
    `data_quality_flags` row per failing tie and keep loading the row.
    The fact values are loaded verbatim from FMP; the flag records that
    FMP's subtotal and FMP's component fields disagree inside the shipped
    row. Analyst reviews via `scripts/review_flags.py` and accepts/rejects.
    """
    return _check_ties(_CF_SOFT_TIES, values_by_concept)


def verify_cf_ties(values_by_concept: dict[str, Decimal]) -> list[TieFailure]:
    """Return ALL tie failures (hard + soft) as a combined list.

    Preserved for callers that want the complete diagnostic set regardless
    of hard/soft distinction — notably `arrow.agents.amendment_detect`
    (post-supersession re-verify) and `scripts/sweep_fmp_coverage.py`.
    Mainline ingest does NOT use this; it calls verify_cf_hard_ties and
    verify_cf_soft_ties separately so it can branch on hard vs soft.
    """
    return (
        _check_ties(_CF_HARD_TIES, values_by_concept)
        + _check_ties(_CF_SOFT_TIES, values_by_concept)
    )
