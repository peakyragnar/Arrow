"""
NVIDIA-specific extraction overrides.

Known quirks:
- CapEx uses PaymentsToAcquireProductiveAssets (not PaymentsToAcquirePropertyPlantAndEquipment)
- FY2026 Q4 has two acquisition lines that must be summed (regular + Groq via
  PaymentsToAcquireBusinessTwoNetOfCashAcquired)
- Fiscal year ends in late January (FY2024 ends Jan 28, 2024)
- 10:1 stock split effective June 2024 — historical share counts are restated
"""


def get_components(base_components: dict) -> dict:
    """Override concept mappings for NVIDIA."""
    components = dict(base_components)

    # NVIDIA uses different CapEx concepts across fiscal years:
    # FY2024: PurchasesOfPropertyAndEquipmentAndIntangibleAssets
    # FY2025+: PaymentsToAcquireProductiveAssets
    components["capex_q"] = {
        "concepts": ["PaymentsToAcquireProductiveAssets",
                      "PurchasesOfPropertyAndEquipmentAndIntangibleAssets",
                      "PaymentsToAcquirePropertyPlantAndEquipment"],
        "type": "flow", "statement": "cf", "negate": True,
    }

    # FY2026 10-K switched from MarketableSecuritiesCurrent to a combined concept
    components["short_term_investments_q"] = {
        "concepts": ["MarketableSecuritiesCurrent",
                      "MarketableSecuritiesAndEquitySecuritiesFVNI",
                      "ShortTermInvestments"],
        "type": "stock",
    }

    return components


def post_process(record: dict, extractions: list) -> dict:
    """Post-process extracted record for NVIDIA-specific fixes."""

    # Fix acquisitions for FY2026 Q4: sum regular + Groq acquisition line
    if record["fiscal_year"] == 2026 and record["fiscal_period"] == "Q4":
        _fix_fy2026_q4_acquisitions(record, extractions)

    return record


def _fix_fy2026_q4_acquisitions(record, extractions):
    """
    FY2026 has a second acquisition concept (PaymentsToAcquireBusinessTwoNetOfCashAcquired)
    for the Groq acquisition. The XBRL only has an FY entry for this concept,
    meaning the full amount is in Q4.
    """
    # Find the FY2026 Q4 (10-K) extraction
    fy2026_10k = None
    for ext in extractions:
        if ext.get("fiscal_year") == 2026 and ext.get("fiscal_period") == "Q4":
            fy2026_10k = ext
            break

    if not fy2026_10k:
        return

    # Check if the second acquisition concept has a value
    from extract import parse_xbrl, classify_contexts, DATA_DIR
    import os

    filing_dir = os.path.join(DATA_DIR, "NVDA", fy2026_10k["accession"])
    meta_path = os.path.join(filing_dir, "filing_meta.json")

    import json
    with open(meta_path) as f:
        meta = json.load(f)

    xbrl_path = os.path.join(filing_dir, meta["xbrl_filename"])
    contexts, facts, nsmap = parse_xbrl(xbrl_path)
    classified = classify_contexts(contexts, meta["report_date"])

    # Get the FY value for PaymentsToAcquireBusinessTwoNetOfCashAcquired
    concept = "PaymentsToAcquireBusinessTwoNetOfCashAcquired"
    if concept not in facts:
        return

    fy_ctx = classified.get("current_fy")
    if not fy_ctx:
        return

    groq_fy = None
    for cref, val in facts[concept]:
        if cref == fy_ctx:
            groq_fy = val
            break

    if groq_fy and record.get("acquisitions_q") is not None:
        # Get the 9M YTD for Groq (likely 0 since it's Q4-only)
        q3_ctx = classified.get("current_ytd_9m")
        groq_9m = 0
        if q3_ctx:
            for cref, val in facts[concept]:
                if cref == q3_ctx:
                    groq_9m = val
                    break

        groq_q4 = groq_fy - groq_9m
        if groq_q4 > 0:
            # Negate to match convention (XBRL positive -> golden negative)
            record["acquisitions_q"] += -groq_q4
