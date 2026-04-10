"""
Palo Alto Networks-specific extraction overrides.

Known quirks:
- FY2025 Q4 10-K (2025-07-31) tags InventoryNet ($113.4M) for the first and only
  time across all filings. PANW is a software/services company with no inventory.
  This appears to be an XBRL tagging artifact in one 10-K. If future filings
  consistently report inventory, revisit this override.
"""


def post_process(record: dict, extractions: list) -> dict:
    # Zero out inventory for FY2025 Q4 — XBRL tagging artifact (see docstring)
    if record["fiscal_year"] == 2025 and record["fiscal_period"] == "Q4":
        record["inventory_q"] = 0

    return record
