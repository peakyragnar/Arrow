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
    # Interest expense: filers tag this variously depending on whether
    # interest is part of operations (banks etc. — out of scope for our
    # non-financial universe) or below the operating line. Non-financial
    # filers typically use `InterestExpense` OR `InterestExpenseNonoperating`
    # (DELL uses the latter). `InterestExpenseDebt` is a subset (interest
    # on debt specifically). All three are acceptable sources for our
    # canonical `interest_expense`.
    XBRLConceptMapping(
        "interest_expense",
        (
            "InterestExpense",
            "InterestExpenseNonoperating",
            "InterestExpenseDebt",
        ),
        "USD",
    ),
    # Interest income: similar pattern. Filers variously use
    # InvestmentIncomeInterest / InterestIncomeOperating / InterestIncomeNonoperating.
    # `InvestmentIncomeInterestAndDividend` is the combined concept some filers
    # use (DELL, others) — technically conflates interest + dividend income, but
    # for non-financial filers dividend income is typically negligible (<1% of
    # total investment income), so using it as fallback is empirically safe.
    # If a filer has meaningful dividend income, Layer 5 XBRL anchor would flag it.
    XBRLConceptMapping(
        "interest_income",
        (
            "InvestmentIncomeInterest",
            "InterestIncomeOperating",
            "InterestIncomeNonoperating",
            "InvestmentIncomeInterestAndDividend",
        ),
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
        "continuing_ops_after_tax",
        (
            # Many filers (e.g., DELL with NCI) don't report
            # us-gaap:IncomeLossFromContinuingOperations separately — they
            # report us-gaap:ProfitLoss (consolidated pre-NCI) which IS
            # continuing-ops-after-tax when discontinued_ops is zero. We
            # fall back to ProfitLoss/NetIncomeLoss to match the filer's
            # actual reporting. For filers WITH real discontinued_ops,
            # IncomeLossFromContinuingOperations is the primary tag.
            "IncomeLossFromContinuingOperations",
            "ProfitLoss",
            "NetIncomeLoss",
        ),
        "USD",
    ),
    XBRLConceptMapping(
        "discontinued_ops",
        ("IncomeLossFromDiscontinuedOperationsNetOfTax",),
        "USD",
    ),
    # Net-income chain (concepts.md § 4.6):
    #   net_income                        = PRE-NCI consolidated = XBRL ProfitLoss
    #   net_income_attributable_to_parent = POST-NCI parent      = XBRL NetIncomeLoss
    #   minority_interest                 = NCI's share          = XBRL NetIncomeLossAttributableToNoncontrollingInterest
    #
    # Non-NCI filers (e.g., NVDA) typically only publish NetIncomeLoss (no
    # ProfitLoss fact at all). For those filers, parent == consolidated, so
    # NetIncomeLoss is the correct fallback for the pre-NCI `net_income`
    # anchor. For NCI filers (e.g., DELL), ProfitLoss is primary so we
    # compare pre-NCI to pre-NCI.
    XBRLConceptMapping("net_income", ("ProfitLoss", "NetIncomeLoss"), "USD"),
    XBRLConceptMapping(
        "net_income_attributable_to_parent", ("NetIncomeLoss",), "USD",
    ),
    XBRLConceptMapping(
        "minority_interest",
        ("NetIncomeLossAttributableToNoncontrollingInterest",),
        "USD",
    ),
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
    # CF `net_income_start` = pre-NCI consolidated, same as IS `net_income`.
    # Maps to ProfitLoss (primary) with NetIncomeLoss fallback for non-NCI
    # filers. When the amendment agent supersedes IS.net_income it MUST
    # also supersede CF.net_income_start to preserve Layer 2.
    XBRLConceptMapping("net_income_start", ("ProfitLoss", "NetIncomeLoss"), "USD"),
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
