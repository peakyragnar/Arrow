"""FMP cash-flow row → canonical CF buckets.

Per docs/reference/fmp_mapping.md § 5.3 and concepts.md § 6.

Sign convention: all CF buckets are stored with CASH-IMPACT SIGN
(concepts.md § 2.2). FMP's convention matches ours empirically — no
sign transform. Cash out → negative, cash in → positive. CF subtotals
are straight sums; no per-item sign inversion in formulas.

Bundling where FMP is coarser than concepts.md:
  - FMP reports net debt issuance (net of issuance - repayment). We
    map the nets to the *_issuance buckets and leave the *_repayment
    buckets unpopulated — the CFF subtotal tie still holds because the
    sum includes the net, which is what the filer reported.

Buckets FMP doesn't expose (gain_on_sale_assets_cf,
gain_on_sale_investments_cf, asset_writedown, change_deferred_revenue,
change_income_taxes, divestitures, loans_*, special_dividends_paid,
misc_cf_adjustments) are not mapped here. They're either (a) rolled
into FMP's otherNonCashItems / otherInvestingActivities /
otherFinancingActivities / otherWorkingCapital buckets, or (b) simply
absent for filers that don't report them. Either way, they contribute
as 0 in the CF subtotal ties.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class MappedFact:
    concept: str
    value: Decimal
    unit: str


# (canonical_concept, [fmp_fields_to_sum], unit).
_CF_BUCKETS: list[tuple[str, list[str], str]] = [
    # --- Start of CF reconciliation ---
    ("net_income_start",               ["netIncome"],                          "USD"),
    # --- CFO non-cash adjustments ---
    ("dna_cf",                         ["depreciationAndAmortization"],        "USD"),
    ("sbc",                            ["stockBasedCompensation"],             "USD"),
    ("deferred_income_tax",            ["deferredIncomeTax"],                  "USD"),
    ("other_noncash",                  ["otherNonCashItems"],                  "USD"),
    # --- CFO working capital ---
    ("change_accounts_receivable",     ["accountsReceivables"],                "USD"),
    ("change_inventory",               ["inventory"],                          "USD"),
    ("change_accounts_payable",        ["accountsPayables"],                   "USD"),
    ("change_other_working_capital",   ["otherWorkingCapital"],                "USD"),
    ("cfo",                            ["netCashProvidedByOperatingActivities"], "USD"),
    # --- CFI ---
    # FMP exposes capex as investmentsInPropertyPlantAndEquipment with an
    # alias capitalExpenditure (identical value). Prefer the more explicit name.
    ("capital_expenditures",           ["investmentsInPropertyPlantAndEquipment"], "USD"),
    ("acquisitions",                   ["acquisitionsNet"],                    "USD"),
    ("purchases_of_investments",       ["purchasesOfInvestments"],             "USD"),
    ("sales_of_investments",           ["salesMaturitiesOfInvestments"],       "USD"),
    ("other_investing",                ["otherInvestingActivities"],           "USD"),
    ("cfi",                            ["netCashProvidedByInvestingActivities"], "USD"),
    # --- CFF ---
    # Bundling: FMP reports NET debt issuance (issuance - repayment). We
    # map those nets into the *_issuance canonical buckets so the CFF
    # subtotal tie holds; the *_repayment buckets stay unpopulated from
    # FMP. SEC XBRL direct ingest (future) can split gross issuance vs
    # repayment when needed.
    ("short_term_debt_issuance",       ["shortTermNetDebtIssuance"],           "USD"),
    ("long_term_debt_issuance",        ["longTermNetDebtIssuance"],            "USD"),
    # Bundle common + preferred issuance into stock_issuance. FMP exposes
    # preferred as a NET figure (netPreferredStockIssuance) rather than
    # splitting gross issuance vs repurchase — acceptable because preferred
    # stock activity is rare; when present (e.g., TDG FY2022 FY = $132M),
    # we count it here to keep the CFF subtotal tie clean. If a future
    # analyst needs the preferred/common split, SEC XBRL direct ingest has
    # the gross concepts separately.
    ("stock_issuance",                 ["commonStockIssuance", "netPreferredStockIssuance"], "USD"),
    ("stock_repurchase",               ["commonStockRepurchased"],             "USD"),
    ("common_dividends_paid",          ["commonDividendsPaid"],                "USD"),
    ("preferred_dividends_paid",       ["preferredDividendsPaid"],             "USD"),
    ("other_financing",                ["otherFinancingActivities"],           "USD"),
    ("cff",                            ["netCashProvidedByFinancingActivities"], "USD"),
    # --- FX / misc / roll-forward ---
    ("fx_effect_on_cash",              ["effectOfForexChangesOnCash"],         "USD"),
    ("net_change_in_cash",             ["netChangeInCash"],                    "USD"),
    ("cash_begin_of_period",           ["cashAtBeginningOfPeriod"],            "USD"),
    ("cash_end_of_period",             ["cashAtEndOfPeriod"],                  "USD"),
    # --- Supplemental disclosures (below the CF sections; not in tie formulas) ---
    # Needed by metric 21 (Unlevered FCF): CFO + interest_paid × (1 − tax_rate) − capex.
    # FMP's `interestPaid` is the same number filers disclose in the CF
    # supplemental footnote. Stored as cash-impact sign (FMP returns it as
    # a positive cash-out value; we preserve FMP's sign since the formula
    # expects a positive magnitude for interest paid).
    ("cash_paid_for_interest",         ["interestPaid"],                       "USD"),
]


def map_cash_flow_row(row: dict[str, Any]) -> list[MappedFact]:
    """Translate one FMP CF JSON row into canonical CF buckets.

    A bucket is emitted for any source field that is present (including 0).
    All-missing source fields mean the bucket is not emitted.
    """
    out: list[MappedFact] = []
    for concept, fmp_fields, unit in _CF_BUCKETS:
        values = [row.get(f) for f in fmp_fields]
        if all(v is None for v in values):
            continue
        total = sum(
            (Decimal(str(v)) for v in values if v is not None),
            start=Decimal("0"),
        )
        out.append(MappedFact(concept=concept, value=total, unit=unit))
    return out
