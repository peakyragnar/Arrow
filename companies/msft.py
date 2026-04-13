"""
Microsoft (MSFT) specific extraction overrides.

Known quirks:
- Acquisitions: MSFT uses a custom extension concept
  AcquisitionsNetOfCashAcquiredAndPurchasesOfIntangibleAndOtherAssets
  (namespace microsoft.com) instead of standard US-GAAP PaymentsToAcquire*
  concepts. This bundles acquisitions with intangible asset purchases.
- Short-term debt: MSFT balance sheet presents "Short-term debt" (CommercialPaper)
  and "Current portion of long-term debt" (LongTermDebtCurrent) as separate line
  items. Both are debt and must be summed for short_term_debt_q.
"""


def get_components(base_components: dict) -> dict:
    """Override concept mappings for MSFT."""
    components = dict(base_components)

    components["acquisitions_q"] = {
        "concepts": ["AcquisitionsNetOfCashAcquiredAndPurchasesOfIntangibleAndOtherAssets"],
        "type": "flow", "statement": "cf",
        "negate": True,
    }

    components["short_term_debt_q"] = {
        "concepts": ["CommercialPaper", "LongTermDebtCurrent"],
        "type": "stock", "default": 0,
        "sum_concepts": True,
    }

    return components
