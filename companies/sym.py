"""
Symbotic (SYM) specific extraction overrides.

Known quirks:
- Acquisitions are split across two CF line items: PaymentsToAcquireBusinessesGross
  (business acquisitions) and PaymentsToAcquireInterestInJointVenture (JV investments).
  Must be summed to match the total investing outflow for acquisitions.
- D&A is split across three CF line items in 10-Ks: DepreciationDepletionAndAmortization,
  OperatingLeaseRightOfUseAssetAmortizationExpense, and RestructuringOfLeasesAmortization
  (sym custom concept). 10-Qs bundle these into a single DDA figure. Summing all three
  ensures Q4 derivation (annual - 9mo) captures the full D&A.
"""


def get_components(base_components: dict) -> dict:
    """Override concept mappings for SYM."""
    components = dict(base_components)

    components["acquisitions_q"] = {
        "concepts": ["PaymentsToAcquireBusinessesGross",
                      "PaymentsToAcquireInterestInJointVenture"],
        "type": "flow", "statement": "cf", "negate": True,
        "sum_concepts": True,
    }

    components["dna_q"] = {
        "concepts": ["DepreciationDepletionAndAmortization",
                      "OperatingLeaseRightOfUseAssetAmortizationExpense"],
        "type": "flow", "statement": "cf",
        "sum_concepts": True,
    }

    return components
