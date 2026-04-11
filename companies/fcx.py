"""
Freeport-McMoRan (FCX) specific extraction overrides.

Known quirks:
- FY2024 10-K (accession 0000831259-25-000006, period end 2024-12-31):
  DocumentFiscalYearFocus is incorrectly tagged as "2023" instead of "2024".
  Fixed by fix_dei() correcting the fiscal year based on DocumentPeriodEndDate.
- Inventory is split across three mining-specific XBRL concepts that must be
  summed: Product (finished goods), InventoryRawMaterialsAndSuppliesNetOfReserves
  (raw materials), InventoryMillandStockpilesonLeachPadsCurrent (in-process ore).
- Interest expense: FCX does not tag InterestExpense. Only InterestIncomeExpenseNet
  is available (already negative in XBRL, no negate needed).
"""


def fix_dei(dei: dict, meta: dict) -> dict:
    """Fix incorrect DEI fiscal year on FCX FY2024 10-K."""
    if meta["accession"] == "0000831259-25-000006":
        dei["DocumentFiscalYearFocus"] = "2024"
    return dei


def get_components(base_components: dict) -> dict:
    """Override concept mappings for FCX."""
    components = dict(base_components)

    components["interest_expense_q"] = {
        "concepts": ["InterestExpense", "InterestExpenseNonoperating",
                      "InterestExpenseDebt", "InterestIncomeExpenseNet"],
        "type": "flow", "statement": "is",
    }

    components["inventory_q"] = {
        "concepts": ["Product",
                      "InventoryRawMaterialsAndSuppliesNetOfReserves",
                      "InventoryMillandStockpilesonLeachPadsCurrent"],
        "type": "stock", "statement": "bs",
        "sum_concepts": True,
    }

    return components
