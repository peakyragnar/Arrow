"""
Master extraction script: parses locally-stored XBRL filings and extracts
quarterly financial components.

Requires filings to be downloaded first via fetch.py.

Usage:
    python3 extract.py --ticker NVDA
    python3 extract.py --ticker NVDA --fy-start 2024 --fy-end 2026
"""

import argparse
import json
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from importlib import import_module

DATA_DIR = "data/filings"

# ── Component definitions ──────────────────────────────────────────────────────
# type: "stock" = balance sheet, "flow" = income/cash flow, "per_period" = discrete only
# statement: "is" = income statement (has discrete quarterly), "cf" = cash flow (YTD only)
# negate: True if golden convention is opposite sign from XBRL

COMPONENTS = {
    # Income Statement (flow, discrete quarterly available)
    "revenue_q": {
        "concepts": ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax"],
        "type": "flow", "statement": "is",
    },
    "cogs_q": {
        "concepts": ["CostOfRevenue", "CostOfGoodsAndServicesSold"],
        "type": "flow", "statement": "is",
    },
    "operating_income_q": {
        "concepts": ["OperatingIncomeLoss"],
        "type": "flow", "statement": "is",
    },
    "rd_expense_q": {
        "concepts": ["ResearchAndDevelopmentExpense"],
        "type": "flow", "statement": "is",
    },
    "income_tax_expense_q": {
        "concepts": ["IncomeTaxExpenseBenefit"],
        "type": "flow", "statement": "is",
    },
    "pretax_income_q": {
        "concepts": [
            "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
            "IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments",
        ],
        "type": "flow", "statement": "is",
    },
    "net_income_q": {
        "concepts": ["ProfitLoss", "NetIncomeLoss"],
        "type": "flow", "statement": "is",
    },
    "interest_expense_q": {
        "concepts": ["InterestExpense", "InterestExpenseNonoperating", "InterestExpenseDebt"],
        "type": "flow", "statement": "is", "negate": True,
    },

    # Balance Sheet (stock, instant values)
    "equity_q": {
        "concepts": ["StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
                      "StockholdersEquity"],
        "type": "stock",
    },
    "short_term_debt_q": {
        "concepts": ["DebtCurrent", "LongTermDebtCurrent", "ShortTermBorrowings", "ConvertibleDebtCurrent",
                      "LongTermDebtAndCapitalLeaseObligationsCurrent"],
        "type": "stock", "default": 0,
    },
    "long_term_debt_q": {
        "concepts": ["LongTermDebtNoncurrent", "ConvertibleDebtNoncurrent",
                      "LongTermDebtAndCapitalLeaseObligations"],
        "type": "stock",
    },
    "operating_lease_liabilities_q": {
        "concepts": ["OperatingLeaseLiabilityCurrent", "OperatingLeaseLiabilityNoncurrent"],
        "type": "stock",
        "sum_concepts": True,
    },
    "cash_q": {
        "concepts": ["CashAndCashEquivalentsAtCarryingValue"],
        "type": "stock",
    },
    "short_term_investments_q": {
        "concepts": ["MarketableSecuritiesCurrent", "ShortTermInvestments",
                      "AvailableForSaleSecuritiesDebtSecuritiesCurrent",
                      "HeldToMaturitySecuritiesCurrent"],
        "type": "stock",
    },
    "accounts_receivable_q": {
        "concepts": ["AccountsReceivableNetCurrent"],
        "type": "stock",
    },
    "inventory_q": {
        "concepts": ["InventoryNet", "MaterialsSuppliesAndOther"],
        "type": "stock",
    },
    "accounts_payable_q": {
        "concepts": ["AccountsPayableCurrent"],
        "type": "stock",
    },
    "total_assets_q": {
        "concepts": ["Assets"],
        "type": "stock",
    },

    # Cash Flow Statement (flow, YTD only — no discrete quarterly in XBRL)
    "cfo_q": {
        "concepts": ["NetCashProvidedByUsedInOperatingActivities"],
        "type": "flow", "statement": "cf",
    },
    "capex_q": {
        "concepts": ["PaymentsToAcquirePropertyPlantAndEquipment",
                      "PaymentsToAcquireProductiveAssets"],
        "type": "flow", "statement": "cf", "negate": True,
    },
    "dna_q": {
        "concepts": ["DepreciationDepletionAndAmortization",
                      "DepreciationAndAmortization", "Depreciation"],
        "type": "flow", "statement": "cf",
    },
    "acquisitions_q": {
        "concepts": ["PaymentsToAcquireBusinessesNetOfCashAcquired"],
        "type": "flow", "statement": "cf", "negate": True,
    },
    "sbc_q": {
        "concepts": ["ShareBasedCompensation",
                      "AllocatedShareBasedCompensationExpense"],
        "type": "flow", "statement": "cf",
    },

    # Per-period (use discrete quarterly entry, not YTD)
    "diluted_shares_q": {
        "concepts": ["WeightedAverageNumberOfDilutedSharesOutstanding"],
        "type": "per_period",
    },
}


# ── XBRL parsing ───────────────────────────────────────────────────────────────

def parse_date(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d")


def date_str(d: datetime) -> str:
    return d.strftime("%Y-%m-%d")


def parse_xbrl(filepath: str) -> tuple[dict, dict, dict]:
    """
    Parse an XBRL instance document.
    Returns (contexts, facts, nsmap).
    - contexts: {context_id: {type, date/start/end, days, has_dimensions}}
    - facts: {(namespace, local_name): [(context_id, value), ...]}
    - nsmap: {prefix: uri}
    """
    nsmap = {}
    for _, elem in ET.iterparse(filepath, events=["start-ns"]):
        prefix, uri = elem
        nsmap[prefix] = uri

    tree = ET.parse(filepath)
    root = tree.getroot()

    xbrli = nsmap.get("", "http://www.xbrl.org/2003/instance")

    # Parse contexts
    contexts = {}
    for ctx in root.findall(f"{{{xbrli}}}context"):
        ctx_id = ctx.get("id")
        period = ctx.find(f"{{{xbrli}}}period")
        entity = ctx.find(f"{{{xbrli}}}entity")
        segment = entity.find(f"{{{xbrli}}}segment") if entity is not None else None
        has_dims = segment is not None and len(segment) > 0

        instant = period.find(f"{{{xbrli}}}instant")
        start = period.find(f"{{{xbrli}}}startDate")
        end = period.find(f"{{{xbrli}}}endDate")

        info = {"id": ctx_id, "has_dimensions": has_dims}
        if instant is not None:
            info["type"] = "instant"
            info["date"] = instant.text
        elif start is not None and end is not None:
            info["type"] = "duration"
            info["start"] = start.text
            info["end"] = end.text
            info["days"] = (parse_date(end.text) - parse_date(start.text)).days

        contexts[ctx_id] = info

    # Parse facts — collect all elements under the root that have contextRef
    facts = {}
    for elem in root:
        ctx_ref = elem.get("contextRef")
        if ctx_ref is None:
            continue
        # Skip dimensioned contexts
        ctx = contexts.get(ctx_ref)
        if ctx is None or ctx.get("has_dimensions"):
            continue

        tag = elem.tag  # e.g., {http://fasb.org/us-gaap/2024}Revenues
        if "}" in tag:
            ns_uri = tag[1:tag.index("}")]
            local = tag[tag.index("}") + 1:]
        else:
            continue

        try:
            val = int(float(elem.text))
        except (TypeError, ValueError):
            continue

        key = local  # use just the local name, we'll match by concept name
        if key not in facts:
            facts[key] = []
        # Deduplicate: same context + same value
        entry = (ctx_ref, val)
        if entry not in facts[key]:
            facts[key].append(entry)

    return contexts, facts, nsmap


def parse_dei(filepath: str) -> dict:
    """
    Extract DEI (Document and Entity Information) elements from an XBRL file.
    Returns dict with fiscal_year, fiscal_period, period_end_date, fy_end_date.
    """
    tree = ET.parse(filepath)
    root = tree.getroot()

    dei_fields = {
        "DocumentFiscalYearFocus": None,
        "DocumentFiscalPeriodFocus": None,
        "DocumentPeriodEndDate": None,
        "CurrentFiscalYearEndDate": None,
    }

    for elem in root:
        tag = elem.tag
        if "}" in tag:
            local = tag[tag.index("}") + 1:]
        else:
            continue
        if local in dei_fields and elem.text:
            dei_fields[local] = elem.text.strip()

    return dei_fields


def classify_contexts(contexts: dict, report_date: str,
                      fy_start_date: str = None) -> dict:
    """
    Identify key contexts for a filing based on its report date.
    Returns dict with keys like 'current_instant', 'current_discrete',
    'current_ytd', 'prior_instant', etc.

    fy_start_date: fiscal year start date from DEI (e.g., "2024-01-01").
    Used to identify the exact FY context.
    """
    report_dt = parse_date(report_date)
    fy_start_dt = parse_date(fy_start_date) if fy_start_date else None
    classified = {}

    # Collect candidate FY contexts (duration ending at report date, ~1 year)
    fy_candidates = []

    for ctx_id, ctx in contexts.items():
        if ctx.get("has_dimensions"):
            continue

        if ctx["type"] == "instant":
            dt = parse_date(ctx["date"])
            diff = abs((dt - report_dt).days)
            if diff <= 3:  # current quarter-end (within 3 days of report date)
                classified["current_instant"] = ctx_id

        elif ctx["type"] == "duration":
            end_dt = parse_date(ctx["end"])
            diff = abs((end_dt - report_dt).days)
            if diff > 3:
                continue  # not ending at current quarter

            days = ctx["days"]
            if 60 <= days <= 120:
                classified["current_discrete"] = ctx_id
            elif 150 <= days <= 210:
                classified["current_ytd_h1"] = ctx_id
            elif 240 <= days <= 300:
                classified["current_ytd_9m"] = ctx_id
            elif 340 <= days <= 380:
                fy_candidates.append((ctx_id, ctx))

    # Pick FY context: prefer the one closest to DEI-derived FY start
    if fy_candidates:
        if fy_start_dt and len(fy_candidates) > 1:
            fy_candidates.sort(
                key=lambda c: abs((parse_date(c[1]["start"]) - fy_start_dt).days)
            )
        best_id, best_ctx = fy_candidates[0]
        classified["current_fy"] = best_id
        classified["fy_start"] = best_ctx["start"]
    elif fy_start_dt:
        # No 340-380 day candidate; shouldn't happen for 10-Ks but
        # set fy_start from DEI as fallback
        classified["fy_start"] = fy_start_date

    # Also find the prior year-end instant (for BS comparatives)
    if "fy_start" in classified:
        fy_start_dt = parse_date(classified["fy_start"])
        prior_end_dt = fy_start_dt - timedelta(days=1)
        for ctx_id, ctx in contexts.items():
            if ctx.get("has_dimensions") or ctx["type"] != "instant":
                continue
            dt = parse_date(ctx["date"])
            if abs((dt - prior_end_dt).days) <= 3:
                classified["prior_instant"] = ctx_id
                break

    return classified


# ── Filing extraction ──────────────────────────────────────────────────────────

def extract_single_filing(filing_dir: str, components: dict = None,
                          company_module=None) -> dict | None:
    """
    Extract raw values from a single filing's XBRL.
    Returns a dict with filing metadata and raw extracted values.
    """
    if components is None:
        components = COMPONENTS

    meta_path = os.path.join(filing_dir, "filing_meta.json")
    if not os.path.exists(meta_path):
        return None

    with open(meta_path) as f:
        meta = json.load(f)

    # Find the XBRL file
    xbrl_path = os.path.join(filing_dir, meta["xbrl_filename"])
    if not os.path.exists(xbrl_path):
        return None

    contexts, facts, nsmap = parse_xbrl(xbrl_path)

    # Parse DEI first — we need fiscal year dates to classify contexts
    dei = parse_dei(xbrl_path)
    if company_module and hasattr(company_module, "fix_dei"):
        dei = company_module.fix_dei(dei, meta)
    if not dei["DocumentFiscalYearFocus"] or not dei["DocumentFiscalPeriodFocus"]:
        raise ValueError(
            f"Filing {meta['accession']} missing DEI fiscal year/period elements"
        )

    fiscal_year = int(dei["DocumentFiscalYearFocus"])
    fiscal_period = dei["DocumentFiscalPeriodFocus"]
    if fiscal_period == "FY":
        fiscal_period = "Q4"

    # Derive fiscal year start date from DEI
    fy_end_mmdd = dei.get("CurrentFiscalYearEndDate", "")  # e.g., "--12-31"
    fy_start_date = None
    if fy_end_mmdd and len(fy_end_mmdd) >= 5:
        fy_end_month = int(fy_end_mmdd[2:4])
        fy_end_day = int(fy_end_mmdd[5:7]) if len(fy_end_mmdd) >= 7 else 31
        # FY start = day after prior FY end
        fy_end_prior = datetime(fiscal_year - 1, fy_end_month, fy_end_day)
        fy_start_date = date_str(fy_end_prior + timedelta(days=1))

    classified = classify_contexts(contexts, meta["report_date"], fy_start_date)

    record = {
        "accession": meta["accession"],
        "form": meta["form"],
        "report_date": meta["report_date"],
        "filing_date": meta["filing_date"],
        "fy_start": classified.get("fy_start"),
        "classified_contexts": classified,
        "fiscal_year": fiscal_year,
        "fiscal_period": fiscal_period,
    }

    # Extract values for each component
    values = {}
    for comp_name, comp_def in components.items():
        comp_values = {}

        if comp_def.get("sum_concepts") and comp_def["type"] == "stock":
            # Sum multiple concepts (e.g., current + noncurrent lease liabilities)
            ctx_id = classified.get("current_instant")
            if ctx_id:
                total = 0
                found_any = False
                for concept in comp_def["concepts"]:
                    for cref, val in facts.get(concept, []):
                        if cref == ctx_id:
                            total += val
                            found_any = True
                            break
                if found_any:
                    comp_values["instant"] = total
            values[comp_name] = comp_values
            continue

        if comp_def.get("sum_concepts") and comp_def["type"] == "flow":
            # Sum multiple flow concepts (e.g., D&A components on cash flow statement)
            negate_set = set(comp_def.get("negate_in_sum", []))
            discrete_ctx = classified.get("current_discrete")
            if discrete_ctx:
                total = 0
                found_any = False
                for concept in comp_def["concepts"]:
                    for cref, val in facts.get(concept, []):
                        if cref == discrete_ctx:
                            if concept in negate_set:
                                val = -val
                            total += val
                            found_any = True
                            break
                if found_any:
                    comp_values["discrete"] = total

            # YTD contexts
            for ytd_key in ["current_ytd_h1", "current_ytd_9m", "current_fy"]:
                ytd_ctx = classified.get(ytd_key)
                if ytd_ctx:
                    total = 0
                    found_any = False
                    for concept in comp_def["concepts"]:
                        for cref, val in facts.get(concept, []):
                            if cref == ytd_ctx:
                                if concept in negate_set:
                                    val = -val
                                total += val
                                found_any = True
                                break
                    if found_any:
                        comp_values["ytd"] = total
                        comp_values["ytd_type"] = ytd_key

            # For Q1, the discrete IS the YTD
            if record["fiscal_period"] == "Q1" and "discrete" in comp_values:
                comp_values["ytd"] = comp_values["discrete"]
                comp_values["ytd_type"] = "q1_discrete"

            values[comp_name] = comp_values
            continue

        for concept in comp_def["concepts"]:
            if concept not in facts:
                continue

            entries = facts[concept]

            if comp_def["type"] == "stock":
                # Balance sheet: use current instant
                ctx_id = classified.get("current_instant")
                if ctx_id:
                    for cref, val in entries:
                        if cref == ctx_id:
                            comp_values["instant"] = val
                            break

            elif comp_def["type"] == "per_period":
                # Diluted shares etc: use discrete quarterly context
                ctx_id = classified.get("current_discrete")
                if ctx_id:
                    for cref, val in entries:
                        if cref == ctx_id:
                            comp_values["discrete"] = val
                            break
                # Fallback: for 10-K (Q4), use FY context if discrete not found
                if "discrete" not in comp_values:
                    fy_ctx = classified.get("current_fy")
                    if fy_ctx:
                        for cref, val in entries:
                            if cref == fy_ctx:
                                comp_values["discrete"] = val
                                break

            elif comp_def["type"] == "flow":
                # Flow items: extract both discrete and YTD where available
                discrete_ctx = classified.get("current_discrete")
                if discrete_ctx:
                    for cref, val in entries:
                        if cref == discrete_ctx:
                            comp_values["discrete"] = val
                            break

                # YTD contexts (try all available)
                for ytd_key in ["current_ytd_h1", "current_ytd_9m", "current_fy"]:
                    ytd_ctx = classified.get(ytd_key)
                    if ytd_ctx:
                        for cref, val in entries:
                            if cref == ytd_ctx:
                                comp_values["ytd"] = val
                                comp_values["ytd_type"] = ytd_key
                                break

                # For Q1, the discrete IS the YTD
                if record["fiscal_period"] == "Q1" and "discrete" in comp_values:
                    comp_values["ytd"] = comp_values["discrete"]
                    comp_values["ytd_type"] = "q1_discrete"

            if comp_values:
                break  # Found values for this concept, stop trying alternatives

        values[comp_name] = comp_values

    record["values"] = values
    return record


# ── Quarterly derivation ───────────────────────────────────────────────────────

def derive_quarterly_values(filing_extractions: list, components: dict = None) -> list:
    """
    Given raw filing extractions (one per filing), derive quarterly values.

    For IS items: use discrete quarterly values from Q1/Q2/Q3, derive Q4 from FY - 9M_YTD.
    For CF items: derive all from YTD subtraction (Q1 = YTD, Q2 = H1-Q1, Q3 = 9M-H1, Q4 = FY-9M).
    For BS items: use instant values directly.
    For per_period: use discrete values directly.
    """
    if components is None:
        components = COMPONENTS

    # Sort by fiscal year and period
    period_order = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}
    filing_extractions.sort(
        key=lambda f: (f.get("fiscal_year", 0), period_order.get(f.get("fiscal_period", ""), 0))
    )

    # Group by fiscal year
    by_fy = {}
    for f in filing_extractions:
        fy = f.get("fiscal_year")
        fp = f.get("fiscal_period")
        if fy and fp:
            by_fy.setdefault(fy, {})[fp] = f

    results = []
    for fy in sorted(by_fy.keys()):
        quarters = by_fy[fy]
        for fp in ["Q1", "Q2", "Q3", "Q4"]:
            filing = quarters.get(fp)
            if not filing:
                continue

            record = {
                "ticker": "",  # filled in by caller
                "fiscal_year": fy,
                "fiscal_period": fp,
                "period_end": filing["report_date"],
                "period_start": filing.get("fy_start") if fp == "Q1" else None,
                "form": filing["form"],
                "accession": filing["accession"],
                "filed": filing["filing_date"],
            }

            # Calendar year/quarter from period_end for cross-company normalization
            period_end_dt = parse_date(filing["report_date"])
            record["calendar_year"] = period_end_dt.year
            record["calendar_quarter"] = (period_end_dt.month - 1) // 3 + 1

            # Fill in period_start for Q2-Q4 from prior quarter's report_date + 1
            if fp != "Q1":
                prev_fp = {"Q2": "Q1", "Q3": "Q2", "Q4": "Q3"}[fp]
                prev = quarters.get(prev_fp)
                if prev:
                    prev_end = parse_date(prev["report_date"])
                    record["period_start"] = date_str(prev_end + timedelta(days=1))

            for comp_name, comp_def in components.items():
                raw = filing["values"].get(comp_name, {})
                value = None

                if comp_def["type"] == "stock":
                    value = raw.get("instant")

                elif comp_def["type"] == "per_period":
                    value = raw.get("discrete")

                elif comp_def["type"] == "flow":
                    stmt = comp_def.get("statement", "is")

                    if stmt == "is" and fp != "Q4":
                        # IS items: use discrete quarterly value for Q1-Q3
                        value = raw.get("discrete")

                    if value is None:
                        # CF items or Q4 IS: derive from YTD subtraction
                        current_ytd = raw.get("ytd")
                        if current_ytd is not None:
                            if fp == "Q1":
                                value = current_ytd
                            else:
                                # Get prior quarter's YTD (0 if not reported)
                                prior_ytd = _get_prior_ytd(
                                    quarters, fp, comp_name, comp_def
                                )
                                if prior_ytd is None:
                                    prior_ytd = 0
                                value = current_ytd - prior_ytd

                if value is not None and comp_def.get("negate"):
                    value = -value

                if value is None and comp_def["type"] in ("stock", "flow"):
                    value = 0  # missing line item = no activity

                if value is None and "default" in comp_def:
                    value = comp_def["default"]

                record[comp_name] = value

            results.append(record)

    return results


def _get_prior_ytd(quarters: dict, current_fp: str, comp_name: str,
                   comp_def: dict) -> int | None:
    """Get the YTD value from the prior quarter's filing for subtraction."""
    if current_fp == "Q2":
        prior = quarters.get("Q1")
    elif current_fp == "Q3":
        prior = quarters.get("Q2")
    elif current_fp == "Q4":
        prior = quarters.get("Q3")
    else:
        return None

    if not prior:
        return None

    raw = prior["values"].get(comp_name, {})
    return raw.get("ytd")


# ── Restatement overrides ────────────────────────────────────────────────────

def apply_restatement_overrides(results: list, ticker: str,
                                components: dict = None) -> list:
    """
    Scan all filings for prior-period values that supersede earlier extractions.
    When any filing contains a value for a period already in our output, the
    most recently filed document's value wins.

    Handles both:
    - Duration contexts (flow, per_period): ~90-day periods matching output quarters
    - Instant contexts (stock): dates matching output quarter-end dates

    Applies to all component types. No type-specific scoping.
    """
    if components is None:
        components = COMPONENTS

    ticker_dir = os.path.join(DATA_DIR, ticker)
    if not os.path.isdir(ticker_dir):
        return results

    # Collect all filings with metadata, sorted by filing date
    all_filings = []
    for dirname in sorted(os.listdir(ticker_dir)):
        meta_path = os.path.join(ticker_dir, dirname, "filing_meta.json")
        if not os.path.exists(meta_path):
            continue
        with open(meta_path) as f:
            meta = json.load(f)
        all_filings.append((os.path.join(ticker_dir, dirname), meta))

    # Detect stock splits from XBRL (collect all split ratios)
    split_ratios = set()
    for filing_dir, meta in all_filings:
        xbrl_path = os.path.join(filing_dir, meta["xbrl_filename"])
        if not os.path.exists(xbrl_path):
            continue
        _, split_facts, _ = parse_xbrl(xbrl_path)
        split_entries = split_facts.get("StockholdersEquityNoteStockSplitConversionRatio1", [])
        for _, val in split_entries:
            if val > 1:
                split_ratios.add(val)

    override_count = 0
    split_skips = 0

    for filing_dir, meta in all_filings:
        xbrl_path = os.path.join(filing_dir, meta["xbrl_filename"])
        if not os.path.exists(xbrl_path):
            continue

        contexts, facts, nsmap = parse_xbrl(xbrl_path)
        report_dt = parse_date(meta["report_date"])

        # Only apply overrides from filings that contain error corrections
        # or are amended filings (10-Q/A, 10-K/A)
        is_amendment = "/A" in meta.get("form", "")
        has_error_correction = False
        for ecf_concept in ["DocumentFinStmtErrorCorrectionFlag"]:
            entries = facts.get(ecf_concept, [])
            for _, val in entries:
                if val == 1:  # parsed as int(float("true")) won't work; check raw
                    has_error_correction = True
                    break

        # Also check raw XML for the flag since boolean "true" may not parse as int
        if not has_error_correction:
            import xml.etree.ElementTree as ET
            tree = ET.parse(xbrl_path)
            root = tree.getroot()
            for elem in root:
                if "DocumentFinStmtErrorCorrectionFlag" in elem.tag:
                    if elem.text and elem.text.strip().lower() == "true":
                        has_error_correction = True
                    break

        if not is_amendment and not has_error_correction:
            continue

        # Find prior-period duration contexts (~90-day, not ending at this filing's report date)
        prior_duration_ctxs = {}  # ctx_id -> end_date
        for ctx_id, ctx in contexts.items():
            if ctx.get("has_dimensions") or ctx.get("type") != "duration":
                continue
            days = ctx.get("days", 0)
            if not (60 <= days <= 120):
                continue
            end_dt = parse_date(ctx["end"])
            if abs((end_dt - report_dt).days) <= 3:
                continue
            prior_duration_ctxs[ctx_id] = ctx["end"]

        # Find prior-period instant contexts (not matching this filing's report date)
        prior_instant_ctxs = {}  # ctx_id -> date
        for ctx_id, ctx in contexts.items():
            if ctx.get("has_dimensions") or ctx.get("type") != "instant":
                continue
            dt = parse_date(ctx["date"])
            if abs((dt - report_dt).days) <= 3:
                continue
            prior_instant_ctxs[ctx_id] = ctx["date"]

        if not prior_duration_ctxs and not prior_instant_ctxs:
            continue

        # Match prior-period contexts to output quarters and override
        for r in results:
            period_end = r.get("period_end")
            if not period_end:
                continue

            # Only override if this filing was filed after the original
            if meta["filing_date"] <= r.get("filed", ""):
                continue

            period_end_dt = parse_date(period_end)

            # Find matching duration context for this quarter
            matching_duration = None
            for ctx_id, end_date in prior_duration_ctxs.items():
                if abs((parse_date(end_date) - period_end_dt).days) <= 3:
                    matching_duration = ctx_id
                    break

            # Find matching instant context for this quarter
            matching_instant = None
            for ctx_id, inst_date in prior_instant_ctxs.items():
                if abs((parse_date(inst_date) - period_end_dt).days) <= 3:
                    matching_instant = ctx_id
                    break

            if not matching_duration and not matching_instant:
                continue

            for comp_name, comp_def in components.items():
                comp_type = comp_def["type"]

                # Pick the right context type for this component
                if comp_type == "stock":
                    ctx_id = matching_instant
                else:
                    ctx_id = matching_duration

                if not ctx_id:
                    continue

                if comp_def.get("sum_concepts"):
                    # Sum all matching concepts for this context
                    total = 0
                    found_any = False
                    for concept in comp_def["concepts"]:
                        for cref, val in facts.get(concept, []):
                            if cref == ctx_id:
                                total += val
                                found_any = True
                                break
                    if found_any:
                        old_val = r.get(comp_name)
                        if old_val is not None and old_val != total:
                            r[comp_name] = total
                            override_count += 1
                        elif old_val is None:
                            r[comp_name] = total
                            override_count += 1
                else:
                    for concept in comp_def["concepts"]:
                        if concept not in facts:
                            continue

                        for cref, val in facts[concept]:
                            if cref != ctx_id:
                                continue

                            if comp_def.get("negate"):
                                val = -val

                            old_val = r.get(comp_name)
                            if old_val is not None and old_val != val:
                                # For diluted shares: check if difference is
                                # due to a stock split (skip pre-split values)
                                if comp_name == "diluted_shares_q" and split_ratios and old_val != 0:
                                    ratio = val / old_val
                                    if any(abs(ratio - sr) < 0.01 for sr in split_ratios):
                                        # New value is pre-split, keep post-split original
                                        split_skips += 1
                                        break

                                r[comp_name] = val
                                override_count += 1
                            elif old_val is None:
                                r[comp_name] = val
                                override_count += 1

                        break  # matched concept, stop trying alternatives

    if split_ratios:
        ratios_str = ", ".join(f"{int(r)}:1" for r in sorted(split_ratios))
        print(f"  Detected stock split(s): {ratios_str}")
        if split_skips:
            print(f"  Skipped {split_skips} pre-split diluted share overrides")

    if override_count > 0:
        print(f"  Applied {override_count} restatement overrides from later filings")

    return results


# ── Main extraction ────────────────────────────────────────────────────────────

def extract_company(ticker: str, components: dict = None,
                    fy_start: int = None, fy_end: int = None) -> list:
    """
    Extract all quarterly components for a company from downloaded filings.
    Returns a list of quarterly records.
    """
    if components is None:
        components = dict(COMPONENTS)

    ticker_dir = os.path.join(DATA_DIR, ticker)
    if not os.path.isdir(ticker_dir):
        print(f"No filings found for {ticker}. Run fetch.py first.")
        return []

    # Try to load company-specific overrides
    company_module = None
    try:
        company_module = import_module(f"companies.{ticker.lower()}")
        print(f"Loaded company overrides: companies/{ticker.lower()}.py")
    except ImportError:
        pass

    if company_module and hasattr(company_module, "get_components"):
        components = company_module.get_components(components)

    # Parse all filings
    filing_dirs = sorted(os.listdir(ticker_dir))
    print(f"Parsing {len(filing_dirs)} filings for {ticker}...")

    extractions = []
    for dirname in filing_dirs:
        filing_dir = os.path.join(ticker_dir, dirname)
        if not os.path.isdir(filing_dir):
            continue

        extraction = extract_single_filing(filing_dir, components, company_module)
        if extraction is None:
            continue

        fy = extraction.get("fiscal_year")
        print(f"  {extraction['form']:4s} {extraction['report_date']}  →  FY{fy} {extraction['fiscal_period']}")
        extractions.append(extraction)

    # Derive quarterly values (uses full filing set for derivation dependencies)
    print(f"\nDeriving quarterly values...")
    results = derive_quarterly_values(extractions, components)

    # Apply restatement overrides from 10-K comparative data
    print(f"Checking for restatement overrides...")
    results = apply_restatement_overrides(results, ticker, components)

    # Set ticker on all records
    for r in results:
        r["ticker"] = ticker

    # Post-process with company module
    if company_module and hasattr(company_module, "post_process_all"):
        results = company_module.post_process_all(results, extractions)
    elif company_module and hasattr(company_module, "post_process"):
        for r in results:
            r = company_module.post_process(r, extractions)

    # Filter to output fiscal year range (after all derivation and overrides)
    # Default: all derived quarters. Use --fy-start/--fy-end to narrow.
    if fy_start or fy_end:
        if fy_start:
            results = [r for r in results if r["fiscal_year"] >= fy_start]
        if fy_end:
            results = [r for r in results if r["fiscal_year"] <= fy_end]

    # Report extraction quality
    for r in results:
        extracted = sum(1 for k, v in r.items() if k in components and v is not None)
        total = len(components)
        print(f"  FY{r['fiscal_year']} {r['fiscal_period']}: {extracted}/{total} components")

    return results


def main():
    parser = argparse.ArgumentParser(description="Extract financial data from XBRL filings")
    parser.add_argument("--ticker", required=True, help="Stock ticker symbol")
    parser.add_argument("--fy-start", type=int, help="Start fiscal year (inclusive)")
    parser.add_argument("--fy-end", type=int, help="End fiscal year (inclusive)")
    parser.add_argument("--output-dir", default="output", help="Output directory")
    args = parser.parse_args()

    results = extract_company(args.ticker, fy_start=args.fy_start, fy_end=args.fy_end)

    os.makedirs(args.output_dir, exist_ok=True)
    output_path = os.path.join(args.output_dir, f"{args.ticker.lower()}.json")
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nWrote {len(results)} quarters to {output_path}")


if __name__ == "__main__":
    main()
