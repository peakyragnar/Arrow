"""
Palo Alto Networks-specific extraction overrides.

Known quirks:
- D&A: PANW breaks out amortization of deferred contract costs, debt issuance
  costs, and investment premiums as separate CF reconciliation line items.
  These must be summed with DepreciationDepletionAndAmortization.
  (Dell, by contrast, rolls these into "Other, net" — same XBRL concept
  but double-counts if summed. This is company-specific, not master.)
- FY2025 Q4 10-K (2025-07-31) tags InventoryNet ($113.4M) for the first and only
  time across all filings. PANW is a software/services company with no inventory.
  This appears to be an XBRL tagging artifact in one 10-K. If future filings
  consistently report inventory, revisit this override.
"""


def get_components(base_components: dict) -> dict:
    """Override concept mappings for PANW."""
    components = dict(base_components)

    # PANW reports D&A as separate CF line items that must be summed:
    # - DepreciationDepletionAndAmortization (property/equipment)
    # - CapitalizedContractCostAmortization (deferred contract costs)
    # - AmortizationOfFinancingCostsAndDiscounts (debt issuance costs)
    # - AccretionAmortizationOfDiscountsAndPremiumsInvestments (investment premiums,
    #   negated: positive XBRL = CF reduction, opposite of D&A add-backs)
    components["dna_q"] = {
        "concepts": ["DepreciationDepletionAndAmortization",
                      "DepreciationAndAmortization",
                      "CapitalizedContractCostAmortization",
                      "AmortizationOfFinancingCostsAndDiscounts",
                      "AccretionAmortizationOfDiscountsAndPremiumsInvestments"],
        "type": "flow", "statement": "cf",
        "sum_concepts": True,
        "negate_in_sum": ["AccretionAmortizationOfDiscountsAndPremiumsInvestments"],
    }

    return components


def post_process(record: dict, extractions: list) -> dict:
    # Zero out inventory for FY2025 Q4 — XBRL tagging artifact (see docstring)
    if record["fiscal_year"] == 2025 and record["fiscal_period"] == "Q4":
        record["inventory_q"] = 0

    return record
