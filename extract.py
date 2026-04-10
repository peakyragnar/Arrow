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
        "concepts": ["NetIncomeLoss"],
        "type": "flow", "statement": "is",
    },
    "interest_expense_q": {
        "concepts": ["InterestExpense", "InterestExpenseNonoperating", "InterestExpenseDebt"],
        "type": "flow", "statement": "is", "negate": True,
    },

    # Balance Sheet (stock, instant values)
    "equity_q": {
        "concepts": ["StockholdersEquity"],
        "type": "stock",
    },
    "short_term_debt_q": {
        "concepts": ["DebtCurrent", "LongTermDebtCurrent", "ShortTermBorrowings"],
        "type": "stock", "default": 0,
    },
    "long_term_debt_q": {
        "concepts": ["LongTermDebtNoncurrent"],
        "type": "stock",
    },
    "operating_lease_liabilities_q": {
        "concepts": ["OperatingLeaseLiability"],
        "type": "stock",
    },
    "cash_q": {
        "concepts": ["CashAndCashEquivalentsAtCarryingValue"],
        "type": "stock",
    },
    "short_term_investments_q": {
        "concepts": ["MarketableSecuritiesCurrent", "ShortTermInvestments",
                      "AvailableForSaleSecuritiesDebtSecuritiesCurrent"],
        "type": "stock",
    },
    "accounts_receivable_q": {
        "concepts": ["AccountsReceivableNetCurrent"],
        "type": "stock",
    },
    "inventory_q": {
        "concepts": ["InventoryNet"],
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
                      "DepreciationAndAmortization"],
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


def classify_contexts(contexts: dict, report_date: str) -> dict:
    """
    Identify key contexts for a filing based on its report date.
    Returns dict with keys like 'current_instant', 'current_discrete',
    'current_ytd', 'prior_instant', etc.
    """
    report_dt = parse_date(report_date)
    classified = {}

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
            elif days > 340:
                classified["current_fy"] = ctx_id

            # fy_start: prefer the longest context's start (most accurate)
            if "fy_start" not in classified or days > classified.get("_fy_start_days", 0):
                classified["fy_start"] = ctx["start"]
                classified["_fy_start_days"] = days

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

def extract_single_filing(filing_dir: str, components: dict = None) -> dict | None:
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
    classified = classify_contexts(contexts, meta["report_date"])

    record = {
        "accession": meta["accession"],
        "form": meta["form"],
        "report_date": meta["report_date"],
        "filing_date": meta["filing_date"],
        "fy_start": classified.get("fy_start"),
        "classified_contexts": classified,
    }

    # Determine fiscal year and period from the filing
    # Q1 10-Q: has current_discrete only (~90 days)
    # Q2 10-Q: has current_discrete + current_ytd_h1
    # Q3 10-Q: has current_discrete + current_ytd_9m
    # 10-K: has current_fy only
    if meta["form"] == "10-K":
        record["fiscal_period"] = "Q4"
    elif "current_ytd_9m" in classified:
        record["fiscal_period"] = "Q3"
    elif "current_ytd_h1" in classified:
        record["fiscal_period"] = "Q2"
    else:
        record["fiscal_period"] = "Q1"

    # Determine fiscal year from fy_start
    if classified.get("fy_start"):
        fy_start_dt = parse_date(classified["fy_start"])
        # Fiscal year is typically named by the calendar year of the FY end
        # e.g., FY2025 starts Jan 29, 2024 → FY end is Jan 2025 → FY=2025
        report_dt = parse_date(meta["report_date"])
        if meta["form"] == "10-K":
            # The report_date IS the FY end
            fy_end_dt = report_dt
        else:
            # Estimate FY end: fy_start + ~365 days
            fy_end_dt = fy_start_dt + timedelta(days=365)
        record["fiscal_year"] = fy_end_dt.year

    # Extract values for each component
    values = {}
    for comp_name, comp_def in components.items():
        comp_values = {}

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
                                # Get prior quarter's YTD
                                prior_ytd = _get_prior_ytd(
                                    quarters, fp, comp_name, comp_def
                                )
                                if prior_ytd is not None:
                                    value = current_ytd - prior_ytd

                if value is not None and comp_def.get("negate"):
                    value = -value

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
    Scan all 10-K filings for prior-period quarterly values that supersede
    the original 10-Q extractions. When a 10-K contains a discrete ~90-day
    context for a prior quarter, its value overrides the original.

    This handles restatements presented as comparative quarterly data in 10-Ks,
    which have no explicit amendment indicator in the XBRL.
    """
    if components is None:
        components = COMPONENTS

    ticker_dir = os.path.join(DATA_DIR, ticker)
    if not os.path.isdir(ticker_dir):
        return results

    # Build lookup: (fiscal_year, fiscal_period) -> result record
    result_map = {}
    for r in results:
        key = (r["fiscal_year"], r["fiscal_period"])
        result_map[key] = r

    # Collect all 10-K filings sorted by filing date (most recent last)
    ten_ks = []
    for dirname in sorted(os.listdir(ticker_dir)):
        meta_path = os.path.join(ticker_dir, dirname, "filing_meta.json")
        if not os.path.exists(meta_path):
            continue
        with open(meta_path) as f:
            meta = json.load(f)
        if meta["form"] == "10-K":
            ten_ks.append((os.path.join(ticker_dir, dirname), meta))

    override_count = 0

    for filing_dir, meta in ten_ks:
        xbrl_path = os.path.join(filing_dir, meta["xbrl_filename"])
        if not os.path.exists(xbrl_path):
            continue

        contexts, facts, nsmap = parse_xbrl(xbrl_path)
        report_dt = parse_date(meta["report_date"])

        # Find all ~90-day (discrete quarter) duration contexts NOT ending
        # at this 10-K's report date — these are prior-period comparatives
        prior_quarter_contexts = {}  # ctx_id -> (start, end)
        for ctx_id, ctx in contexts.items():
            if ctx.get("has_dimensions") or ctx.get("type") != "duration":
                continue
            days = ctx.get("days", 0)
            if not (60 <= days <= 120):
                continue
            end_dt = parse_date(ctx["end"])
            if abs((end_dt - report_dt).days) <= 3:
                continue  # this is the 10-K's own quarter, skip
            prior_quarter_contexts[ctx_id] = (ctx["start"], ctx["end"])

        if not prior_quarter_contexts:
            continue

        # For each prior-period context, try to match it to a result quarter
        for ctx_id, (start, end) in prior_quarter_contexts.items():
            # Find which result quarter this context belongs to
            target = None
            for r in results:
                if r.get("period_end") and abs((parse_date(r["period_end"]) - parse_date(end)).days) <= 3:
                    target = r
                    break
            if not target:
                continue

            # Only override if the 10-K was filed after the original filing
            if meta["filing_date"] <= target.get("filed", ""):
                continue

            # Extract values for each flow/IS component from this context
            for comp_name, comp_def in components.items():
                if comp_def["type"] not in ("flow",):
                    continue
                if comp_def.get("statement") == "cf":
                    continue  # CF values are YTD-derived, not discrete comparatives

                for concept in comp_def["concepts"]:
                    if concept not in facts:
                        continue

                    for cref, val in facts[concept]:
                        if cref != ctx_id:
                            continue

                        if comp_def.get("negate"):
                            val = -val

                        old_val = target.get(comp_name)
                        if old_val != val:
                            target[comp_name] = val
                            override_count += 1

                    break  # matched concept, stop trying alternatives

    if override_count > 0:
        print(f"  Applied {override_count} restatement overrides from 10-K comparatives")

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

        extraction = extract_single_filing(filing_dir, components)
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

    # Filter to requested fiscal year range (after all derivation and overrides)
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
