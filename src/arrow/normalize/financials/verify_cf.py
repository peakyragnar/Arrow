"""Cash-flow subtotal-tie verification (verification.md § 2.3).

When FMP is the source, every CF tie is a vendor-consistency check, not
a filer-integrity check. The filer's actual 10-Q is always internally
consistent; FMP's normalized row can break any tie by sourcing the three
values (begin / end / netChange / section subtotals) from different
positions in its data model, or by mis-deriving Q4 as FY−9M.

Empirically, MU FY2017 Q4 (Q4-derived netChange off by $3.94B from the
cash-position delta) and MU FY2022 Q3 (chained begin/end values that
don't match the prior-period close) demonstrate the cash roll-forward
breaks just like the section-sum tie. Both are FMP normalization defects,
not filer issues.

All CF ties are SOFT in the FMP-source path: write a
`data_quality_flags` row per failing tie and keep loading. A genuinely
hard CF integrity check would require SEC XBRL direct ingest where
filer-level identities could be enforced. Until then, the cash
roll-forward and section-sum ties both route through the steward.

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
        # Failure mode is vendor-decomposition/Q4-derivation, not filer
        # integrity. Empirical: AMD FY2017 Q4 and similar Q4 rows that
        # FMP derives as FY − 9M show section-sum ≠ net-change mismatches.
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
        # Cash roll-forward is the definitional CF identity for the FILER's
        # 10-Q, but FMP's row can break it because its three values come
        # from different sources. Empirical (audited 60 MU quarters):
        # FY2017 Q4 off by $3.94B (Q4 = FY − 9M derivation gone wrong),
        # FY2022 Q3 off by $4M (chained begin/end values that don't
        # match prior-period close — Q2 ends 9,224M, Q3 begins 9,116M).
        # Treat as vendor consistency; route through steward.
        "net_change_in_cash == cash_end_of_period - cash_begin_of_period",
        "net_change_in_cash",
        [
            ("cash_end_of_period", +1),
            ("cash_begin_of_period", -1),
        ],
    ),
]

# HARD ties — currently empty in the FMP-source path. A genuine filer-
# integrity CF check would require SEC XBRL direct ingest where the
# values come from a single internally-consistent source. Kept as a
# stable export so callers (load.py, amendment_detect.py) and tests
# don't need a conditional branch.
_CF_HARD_TIES: list[tuple[str, str, list[tuple[str, int]]]] = []


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
