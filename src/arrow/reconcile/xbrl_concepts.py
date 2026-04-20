"""Canonical bucket -> XBRL tag(s) mapping.

Derived from docs/reference/fmp_mapping.md § 5.1. Each canonical bucket
may correspond to more than one XBRL tag because filers use different
us-gaap concepts for the same semantic item (e.g., `Revenues` vs
`RevenueFromContractWithCustomerExcludingAssessedTax` — NVDA switched
between these post-ASC 606 adoption). The reconciler tries tags in order
and uses the first one that has a matching fact for the target period.

Only IS concepts are mapped here (matches Slice 2a scope). BS and CF
concepts will be added when those mappers land.

Per-share and share-count buckets get their own unit key in XBRL
(`USD/shares` and `shares` respectively).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class XBRLConceptMapping:
    canonical: str
    xbrl_tags: tuple[str, ...]
    unit: str  # 'USD', 'USD/shares', 'shares'


_IS_MAPPINGS: tuple[XBRLConceptMapping, ...] = (
    XBRLConceptMapping(
        canonical="revenue",
        xbrl_tags=(
            "Revenues",
            "RevenueFromContractWithCustomerExcludingAssessedTax",
        ),
        unit="USD",
    ),
    XBRLConceptMapping("cogs", ("CostOfRevenue", "CostOfGoodsAndServicesSold"), "USD"),
    XBRLConceptMapping("gross_profit", ("GrossProfit",), "USD"),
    XBRLConceptMapping(
        "rd", ("ResearchAndDevelopmentExpense",), "USD",
    ),
    XBRLConceptMapping(
        "sga",
        ("SellingGeneralAndAdministrativeExpense",),
        "USD",
    ),
    XBRLConceptMapping(
        "total_opex",
        ("OperatingExpenses", "CostsAndExpenses"),
        "USD",
    ),
    XBRLConceptMapping("operating_income", ("OperatingIncomeLoss",), "USD"),
    XBRLConceptMapping("interest_expense", ("InterestExpense",), "USD"),
    XBRLConceptMapping(
        "interest_income",
        ("InvestmentIncomeInterest", "InterestIncomeOperating"),
        "USD",
    ),
    XBRLConceptMapping(
        "ebt_incl_unusual",
        (
            "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
            "IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments",
        ),
        "USD",
    ),
    XBRLConceptMapping("tax", ("IncomeTaxExpenseBenefit",), "USD"),
    XBRLConceptMapping(
        "continuing_ops_after_tax", ("IncomeLossFromContinuingOperations",), "USD"
    ),
    XBRLConceptMapping(
        "discontinued_ops",
        ("IncomeLossFromDiscontinuedOperationsNetOfTax",),
        "USD",
    ),
    XBRLConceptMapping("net_income", ("NetIncomeLoss",), "USD"),
    XBRLConceptMapping("eps_basic", ("EarningsPerShareBasic",), "USD/shares"),
    XBRLConceptMapping("eps_diluted", ("EarningsPerShareDiluted",), "USD/shares"),
    XBRLConceptMapping(
        "shares_basic_weighted_avg",
        ("WeightedAverageNumberOfSharesOutstandingBasic",),
        "shares",
    ),
    XBRLConceptMapping(
        "shares_diluted_weighted_avg",
        ("WeightedAverageNumberOfDilutedSharesOutstanding",),
        "shares",
    ),
)


_BS_MAPPINGS: tuple[XBRLConceptMapping, ...] = (
    XBRLConceptMapping("cash_and_equivalents", ("CashAndCashEquivalentsAtCarryingValue",), "USD"),
    XBRLConceptMapping("total_assets", ("Assets",), "USD"),
    XBRLConceptMapping("total_liabilities", ("Liabilities",), "USD"),
    XBRLConceptMapping(
        "total_equity",
        (
            "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
            "StockholdersEquity",
        ),
        "USD",
    ),
    XBRLConceptMapping(
        "total_liabilities_and_equity",
        ("LiabilitiesAndStockholdersEquity",),
        "USD",
    ),
)

_CF_MAPPINGS: tuple[XBRLConceptMapping, ...] = (
    XBRLConceptMapping("cfo", ("NetCashProvidedByUsedInOperatingActivities",), "USD"),
    XBRLConceptMapping("cfi", ("NetCashProvidedByUsedInInvestingActivities",), "USD"),
    XBRLConceptMapping("cff", ("NetCashProvidedByUsedInFinancingActivities",), "USD"),
    XBRLConceptMapping(
        "capital_expenditures",
        (
            "PaymentsToAcquirePropertyPlantAndEquipment",
            "PaymentsForCapitalImprovements",
            "PaymentsToAcquireProductiveAssets",
        ),
        "USD",
    ),
)

_BY_CANONICAL: dict[str, XBRLConceptMapping] = {
    m.canonical: m for m in (*_IS_MAPPINGS, *_BS_MAPPINGS, *_CF_MAPPINGS)
}


def mapping_for(canonical: str) -> XBRLConceptMapping | None:
    """Return the XBRL mapping for a canonical bucket, or None if unmapped."""
    return _BY_CANONICAL.get(canonical)


def all_is_mappings() -> tuple[XBRLConceptMapping, ...]:
    return _IS_MAPPINGS


def all_bs_mappings() -> tuple[XBRLConceptMapping, ...]:
    return _BS_MAPPINGS


def all_cf_mappings() -> tuple[XBRLConceptMapping, ...]:
    return _CF_MAPPINGS
