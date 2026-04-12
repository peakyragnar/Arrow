"""
Microsoft (MSFT) specific extraction overrides.

Known quirks:
- Acquisitions: MSFT uses a custom extension concept
  AcquisitionsNetOfCashAcquiredAndPurchasesOfIntangibleAndOtherAssets
  (namespace microsoft.com) instead of standard US-GAAP PaymentsToAcquire*
  concepts. This bundles acquisitions with intangible asset purchases.
"""


def get_components(base_components: dict) -> dict:
    """Override concept mappings for MSFT."""
    components = dict(base_components)

    components["acquisitions_q"] = {
        "concepts": ["AcquisitionsNetOfCashAcquiredAndPurchasesOfIntangibleAndOtherAssets"],
        "type": "flow", "statement": "cf",
        "negate": True,
    }

    return components
