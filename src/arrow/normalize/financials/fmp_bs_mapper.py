"""FMP balance-sheet row → canonical BS buckets.

Per docs/reference/fmp_mapping.md § 5.2. Maps FMP's balance-sheet JSON
fields into our canonical bucket names + stored signs per concepts.md § 5.

All BS buckets are USD magnitudes (positive-valued by convention, with
accumulated_depreciation as the one exception stored as negative — which
FMP doesn't expose on this endpoint anyway).

Some FMP fields have higher granularity than our canonical buckets; we
bundle them in where appropriate (e.g. accountPayables + otherPayables →
accounts_payable) so the total-current-liabilities tie holds. Documented
below on each bundle.

Buckets FMP doesn't expose on this endpoint (restricted_cash_current,
gross_ppe, accumulated_depreciation, right_of_use_assets_operating,
equity_method_investments, income_taxes_receivable_current,
common_stock_and_apic combined form) are not mapped here — they're
either filled by SEC XBRL direct ingest later (Build Order step 19) or
derived at query time.
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


# (canonical_concept, [fmp_fields_to_sum], unit)
#
# Multiple fmp_fields allow bundling where FMP has more granularity than
# our canonical buckets. Missing FMP fields (returns None) contribute
# nothing; if ALL fields for a bucket are missing, the bucket is not
# emitted. A bucket-level 0 from FMP is preserved (not dropped) because
# that's a real filer-level "zero".
_BS_BUCKETS: list[tuple[str, list[str], str]] = [
    # --- Current assets ---
    ("cash_and_equivalents",           ["cashAndCashEquivalents"],         "USD"),
    ("short_term_investments",         ["shortTermInvestments"],           "USD"),
    ("accounts_receivable",            ["accountsReceivables"],            "USD"),
    ("inventory",                      ["inventory"],                      "USD"),
    ("prepaid_expenses",               ["prepaids"],                       "USD"),
    ("other_current_assets",           ["otherCurrentAssets"],             "USD"),
    ("total_current_assets",           ["totalCurrentAssets"],             "USD"),
    # --- Noncurrent assets ---
    ("net_ppe",                        ["propertyPlantEquipmentNet"],      "USD"),
    ("long_term_investments",          ["longTermInvestments"],            "USD"),
    ("goodwill",                       ["goodwill"],                       "USD"),
    ("other_intangibles",              ["intangibleAssets"],               "USD"),
    ("deferred_tax_assets_noncurrent", ["taxAssets"],                      "USD"),
    ("other_noncurrent_assets",        ["otherNonCurrentAssets"],          "USD"),
    ("total_assets",                   ["totalAssets"],                    "USD"),
    # --- Current liabilities ---
    # Bundle: FMP splits payables into trade (`accountPayables`) + non-trade
    # (`otherPayables`). Concepts.md has one `accounts_payable` bucket; we
    # sum both into it so the current-liabilities tie holds. See open-items
    # in concepts.md § 12 — an `other_payables` canonical bucket could be
    # added later if the split becomes analytically interesting.
    ("accounts_payable",               ["accountPayables", "otherPayables"], "USD"),
    ("accrued_expenses",               ["accruedExpenses"],                "USD"),
    # FMP bundles current-portion-of-LT-debt + short-term-borrowings into
    # one `shortTermDebt` field (fmp_mapping.md § 5.2). We map to
    # current_portion_lt_debt; short_term_borrowings canonical bucket stays
    # unpopulated from FMP.
    ("current_portion_lt_debt",        ["shortTermDebt"],                  "USD"),
    # FMP's `capitalLeaseObligationsCurrent` is a stale-terminology field
    # that actually covers operating lease current portion per ASC 842
    # (see fmp_mapping.md § 5.2 note). Map to operating-lease bucket.
    ("current_portion_leases_operating", ["capitalLeaseObligationsCurrent"], "USD"),
    ("deferred_revenue_current",       ["deferredRevenue"],                "USD"),
    ("other_current_liabilities",      ["otherCurrentLiabilities"],        "USD"),
    ("total_current_liabilities",      ["totalCurrentLiabilities"],        "USD"),
    # --- Noncurrent liabilities ---
    ("long_term_debt",                 ["longTermDebt"],                   "USD"),
    ("long_term_leases_operating",     ["capitalLeaseObligationsNonCurrent"], "USD"),
    ("deferred_revenue_noncurrent",    ["deferredRevenueNonCurrent"],      "USD"),
    ("deferred_tax_liability_noncurrent", ["deferredTaxLiabilitiesNonCurrent"], "USD"),
    ("other_noncurrent_liabilities",   ["otherNonCurrentLiabilities"],     "USD"),
    ("total_liabilities",              ["totalLiabilities"],               "USD"),
    # --- Equity ---
    ("preferred_stock",                ["preferredStock"],                 "USD"),
    ("common_stock",                   ["commonStock"],                    "USD"),
    ("additional_paid_in_capital",     ["additionalPaidInCapital"],        "USD"),
    ("retained_earnings",              ["retainedEarnings"],               "USD"),
    ("treasury_stock",                 ["treasuryStock"],                  "USD"),
    ("accumulated_other_comprehensive_income",
                                       ["accumulatedOtherComprehensiveIncomeLoss"], "USD"),
    ("noncontrolling_interest",        ["minorityInterest"],               "USD"),
    ("total_equity",                   ["totalEquity"],                    "USD"),
    ("total_liabilities_and_equity",   ["totalLiabilitiesAndTotalEquity"], "USD"),
]


def map_balance_sheet_row(row: dict[str, Any]) -> list[MappedFact]:
    """Translate one FMP BS JSON row into canonical BS buckets.

    Each bucket's value is the sum of its FMP source field(s). A bucket
    is skipped (no row emitted) only if ALL its source fields are
    missing/None — a bucket with any mapped field reporting 0 still
    emits a 0-valued fact, because 0 is a meaningful filer-reported
    value on the balance sheet (e.g., NVDA's preferred_stock or
    treasury_stock).
    """
    out: list[MappedFact] = []
    for concept, fmp_fields, unit in _BS_BUCKETS:
        values = [row.get(f) for f in fmp_fields]
        if all(v is None for v in values):
            continue
        total = sum(
            (Decimal(str(v)) for v in values if v is not None),
            start=Decimal("0"),
        )
        out.append(MappedFact(concept=concept, value=total, unit=unit))
    return out
