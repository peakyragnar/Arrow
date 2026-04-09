"""
NVIDIA-specific extraction overrides.

Known quirks:
- CapEx uses PaymentsToAcquireProductiveAssets (not PaymentsToAcquirePropertyPlantAndEquipment)
- FY2026 Q4 has two acquisition lines that must be summed (regular + Groq via
  PaymentsToAcquireBusinessTwoNetOfCashAcquired)
- Fiscal year ends in late January (FY2024 ends Jan 28, 2024)
- 10:1 stock split effective June 2024 — historical share counts are restated
- SBC in early filings may come from ShareBasedCompensation (CF) or
  AllocatedShareBasedCompensationExpense (IS); the CF addback is canonical
"""

import json
from datetime import datetime


def get_components(base_components: dict) -> dict:
    """Override concept mappings for NVIDIA."""
    components = dict(base_components)

    # NVIDIA uses PaymentsToAcquireProductiveAssets for CapEx
    components["capex_q"] = {
        "concepts": ["PaymentsToAcquireProductiveAssets",
                      "PaymentsToAcquirePropertyPlantAndEquipment"],
        "type": "flow", "statement": "cf", "negate": True,
    }

    return components


def post_process(record: dict, facts: dict, quarter: dict, all_quarters: list) -> dict:
    """Post-process extracted record for NVIDIA-specific fixes."""

    # Fix acquisitions for FY2026 Q4: sum regular + Groq acquisition line
    if record["fiscal_year"] == 2026 and record["fiscal_period"] == "Q4":
        _fix_fy2026_q4_acquisitions(record, facts, quarter, all_quarters)

    return record


def _fix_fy2026_q4_acquisitions(record, facts, quarter, all_quarters):
    """
    FY2026 has a second acquisition concept (PaymentsToAcquireBusinessTwoNetOfCashAcquired)
    for the Groq acquisition. Sum both lines.

    This concept only has an FY entry (no Q1-Q3 entries) because the acquisition
    happened entirely in Q4. We extract the FY value directly as the Q4 value.
    """
    from extract import get_concept_entries

    entries = get_concept_entries(facts, "PaymentsToAcquireBusinessTwoNetOfCashAcquired")
    if not entries:
        return

    fy_start = quarter.get("fy_start")
    period_end = quarter.get("period_end")
    if not fy_start or not period_end:
        return

    # Find the FY entry (start=fy_start, end=Q4_end)
    fy_val = None
    for e in entries:
        if e.get("form") not in ("10-Q", "10-K"):
            continue
        if e.get("start") == fy_start and e["end"] == period_end:
            fy_val = e["val"]
            break

    if fy_val is not None and fy_val != 0 and record.get("acquisitions_q") is not None:
        # Negate to match golden convention (XBRL positive -> golden negative)
        record["acquisitions_q"] += -fy_val
