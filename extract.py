"""
Master extraction script: fetches XBRL companyfacts from SEC EDGAR
and extracts quarterly financial components.

Usage:
    python3 extract.py --cik 0001045810 --ticker NVDA
    python3 extract.py --cik 0001045810 --ticker NVDA --fy-start 2024 --fy-end 2026
"""

import argparse
import json
import os
import urllib.request
from datetime import datetime, timedelta
from importlib import import_module

USER_AGENT = "Arrow research@arrow.dev"

# ── Component definitions ──────────────────────────────────────────────────────
# Each component maps to one or more XBRL concept names (tried in order).
# type: "stock" = balance sheet point-in-time, "flow" = income/cash flow period
# statement: "is" = income statement, "cf" = cash flow statement
# negate: True if the golden convention is opposite sign from XBRL

COMPONENTS = {
    # Income Statement
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

    # Balance Sheet
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

    # Cash Flow Statement
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

    # Per-period (not cumulative — use discrete entry, not YTD derivation)
    "diluted_shares_q": {
        "concepts": ["WeightedAverageNumberOfDilutedSharesOutstanding"],
        "type": "per_period",
    },
}


# ── SEC data fetching ──────────────────────────────────────────────────────────

def fetch_company_facts(cik: str) -> dict:
    """Fetch companyfacts JSON from SEC EDGAR."""
    cik_padded = cik.lstrip("0").zfill(10)
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_padded}.json"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def get_concept_entries(facts: dict, concept_name: str) -> list:
    """Get all USD entries for a concept from companyfacts."""
    for ns in ("us-gaap", "dei", "ifrs-full"):
        ns_facts = facts.get("facts", {}).get(ns, {})
        if concept_name in ns_facts:
            units = ns_facts[concept_name].get("units", {})
            # Try USD first, then shares
            for unit_key in ("USD", "shares"):
                if unit_key in units:
                    return units[unit_key]
    return []


# ── Quarter discovery ──────────────────────────────────────────────────────────

def parse_date(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d")


def period_days(start: str, end: str) -> int:
    return (parse_date(end) - parse_date(start)).days


def discover_quarters(facts: dict, fy_start: int = None, fy_end: int = None) -> list:
    """
    Discover available quarters from the companyfacts data.
    Returns a sorted list of quarter dicts with fiscal year, period, and dates.
    """
    # Use a reliable concept to find filing periods
    for concept in ["Revenues", "Assets", "NetIncomeLoss"]:
        entries = get_concept_entries(facts, concept)
        if entries:
            break

    # Collect unique (fy, fp) from 10-Q/10-K filings.
    # A Q1 10-Q has fp=Q1 form=10-Q; a 10-K has fp=FY form=10-K.
    # Later filings (e.g., next year's 10-K) restate data with the same (fy, fp)
    # but different accession. We want the ORIGINAL filing for each quarter:
    # - Q1/Q2/Q3: the 10-Q with matching fp
    # - Q4: the 10-K with fp=FY
    filings = {}
    for e in entries:
        form = e.get("form")
        if form not in ("10-Q", "10-K"):
            continue
        fy = e.get("fy")
        fp = e.get("fp")
        if fy is None or fp is None:
            continue
        if fy_start and fy < fy_start:
            continue
        if fy_end and fy > fy_end:
            continue

        # Only accept the original filing for this quarter
        # Q1/Q2/Q3: must be from a 10-Q with matching fp
        # Q4/FY: must be from the 10-K
        if fp in ("Q1", "Q2", "Q3") and form != "10-Q":
            continue
        if fp == "FY" and form != "10-K":
            continue

        actual_fp = "Q4" if fp == "FY" else fp
        key = (fy, actual_fp)
        if key not in filings:
            filings[key] = {
                "fiscal_year": fy,
                "fiscal_period": actual_fp,
                "form": form,
                "accession": e.get("accn", ""),
                "filed": e.get("filed", ""),
            }

    # Now find period_end dates from balance sheet data
    bs_entries = get_concept_entries(facts, "Assets")
    # Map (fy, fp) -> period_end using the current-period BS entry
    # Take the LATEST end date per (fy, fp) from the matching accession,
    # since each filing contains comparative (older) and current (newer) BS entries.
    for e in bs_entries:
        if e.get("form") not in ("10-Q", "10-K"):
            continue
        fy = e.get("fy")
        fp = e.get("fp")
        if fp == "FY":
            fp = "Q4"
        key = (fy, fp)
        if key in filings and e.get("accn") == filings[key]["accession"]:
            if "period_end" not in filings[key] or e["end"] > filings[key]["period_end"]:
                filings[key]["period_end"] = e["end"]

    # Determine fiscal year start dates from flow entries
    flow_entries = get_concept_entries(facts, "Revenues") or get_concept_entries(facts, "NetIncomeLoss")
    fy_starts = {}  # fy -> start date of that fiscal year
    for e in flow_entries:
        if e.get("form") not in ("10-Q", "10-K"):
            continue
        fp = e.get("fp")
        fy = e.get("fy")
        start = e.get("start")
        end = e.get("end")
        if not start or not end or not fy:
            continue
        # Q1 entries with ~90 day period give us the FY start
        days = period_days(start, end)
        if fp == "Q1" and 60 <= days <= 120:
            if fy not in fy_starts:
                fy_starts[fy] = start

        # FY entries also give us the start
        if fp == "FY" and days > 300:
            fy_starts[fy] = start

    # Attach FY start dates and compute period_start
    for key, info in filings.items():
        fy = info["fiscal_year"]
        fp = info["fiscal_period"]
        if fy in fy_starts:
            info["fy_start"] = fy_starts[fy]

        # Compute period_start from prior quarter's end + 1 day
        if fp == "Q1" and fy in fy_starts:
            info["period_start"] = fy_starts[fy]

    # Sort by fiscal year and quarter
    quarter_order = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}
    quarters = sorted(filings.values(),
                      key=lambda q: (q["fiscal_year"], quarter_order.get(q["fiscal_period"], 0)))

    # Fill in period_start for Q2-Q4 from prior quarter's period_end
    for i, q in enumerate(quarters):
        if "period_start" not in q and i > 0:
            prev = quarters[i - 1]
            if (prev["fiscal_year"] == q["fiscal_year"] or
                (prev["fiscal_year"] == q["fiscal_year"] - 1 and prev["fiscal_period"] == "Q4")):
                if "period_end" in prev:
                    prev_end = parse_date(prev["period_end"])
                    q["period_start"] = (prev_end + timedelta(days=1)).strftime("%Y-%m-%d")

    return quarters


# ── Value extraction ───────────────────────────────────────────────────────────

def extract_stock_value(entries: list, period_end: str) -> int | None:
    """Extract a balance-sheet (stock) value at a specific quarter-end date."""
    candidates = []
    for e in entries:
        if e.get("form") not in ("10-Q", "10-K") or e.get("start"):
            # Stock entries should have no start date (or empty)
            if e.get("start"):
                continue
        if e["end"] == period_end:
            candidates.append(e)

    if not candidates:
        # Try entries that DO have start dates (some BS items get tagged oddly)
        for e in entries:
            if e.get("form") not in ("10-Q", "10-K"):
                continue
            if e["end"] == period_end:
                candidates.append(e)

    if not candidates:
        return None

    # Prefer the most recently filed entry (latest restatement)
    candidates.sort(key=lambda e: e.get("filed", ""), reverse=True)
    return candidates[0]["val"]


def extract_flow_value_ytd(entries: list, quarter: dict, all_quarters: list) -> int | None:
    """
    Extract a flow value for a quarter using YTD derivation.

    Q1: direct 3-month value
    Q2: H1_YTD - Q1
    Q3: 9M_YTD - H1_YTD
    Q4: FY - 9M_YTD
    """
    fp = quarter["fiscal_period"]
    fy = quarter["fiscal_year"]
    period_end = quarter.get("period_end")
    fy_start = quarter.get("fy_start")

    if not period_end or not fy_start:
        return None

    # Build a map of cumulative entries by their end date
    # Only consider entries that start at the fiscal year start
    cumulative = {}
    for e in entries:
        if e.get("form") not in ("10-Q", "10-K"):
            continue
        if not e.get("start"):
            continue
        if e["start"] != fy_start:
            continue
        end = e["end"]
        days = period_days(e["start"], end)
        # Store by end date, preferring most recently filed
        if end not in cumulative or e.get("filed", "") > cumulative[end].get("filed", ""):
            cumulative[end] = e

    if fp == "Q1":
        # Q1: the cumulative entry IS the quarterly value
        entry = cumulative.get(period_end)
        return entry["val"] if entry else None

    elif fp == "Q2":
        # Q2 = H1_YTD - Q1
        h1_entry = cumulative.get(period_end)
        if not h1_entry:
            return None
        # Find Q1 in the same fiscal year
        q1 = _find_quarter(all_quarters, fy, "Q1")
        if not q1 or "period_end" not in q1:
            return None
        q1_entry = cumulative.get(q1["period_end"])
        if not q1_entry:
            return None
        return h1_entry["val"] - q1_entry["val"]

    elif fp == "Q3":
        # Q3 = 9M_YTD - H1_YTD
        nine_m = cumulative.get(period_end)
        if not nine_m:
            return None
        q2 = _find_quarter(all_quarters, fy, "Q2")
        if not q2 or "period_end" not in q2:
            return None
        h1_entry = cumulative.get(q2["period_end"])
        if not h1_entry:
            return None
        return nine_m["val"] - h1_entry["val"]

    elif fp == "Q4":
        # Q4 = FY - 9M_YTD
        fy_entry = cumulative.get(period_end)
        if not fy_entry:
            return None
        q3 = _find_quarter(all_quarters, fy, "Q3")
        if not q3 or "period_end" not in q3:
            return None
        nine_m = cumulative.get(q3["period_end"])
        if not nine_m:
            return None
        return fy_entry["val"] - nine_m["val"]

    return None


def extract_per_period_value(entries: list, quarter: dict) -> int | None:
    """
    Extract a per-period value (like diluted shares) using the discrete
    quarterly entry, not YTD derivation.

    Prefers the entry from the ORIGINAL filing (matching accession) to get
    point-in-time values rather than restated values from later filings.
    """
    period_end = quarter.get("period_end")
    accession = quarter.get("accession", "")
    if not period_end:
        return None

    # Find discrete quarterly entries ending at period_end
    candidates = []
    for e in entries:
        if e.get("form") not in ("10-Q", "10-K"):
            continue
        if e["end"] != period_end:
            continue
        if not e.get("start"):
            continue
        days = period_days(e["start"], e["end"])
        # Discrete quarter: ~60-120 days
        if 50 <= days <= 130:
            candidates.append(e)

    if not candidates:
        # Fallback: try to find any entry ending at period_end with a start date
        for e in entries:
            if e.get("form") not in ("10-Q", "10-K"):
                continue
            if e["end"] == period_end and e.get("start"):
                candidates.append(e)

    if not candidates:
        return None

    # Prefer entry from the original filing (matching accession)
    for c in candidates:
        if c.get("accn") == accession:
            return c["val"]

    # Fallback: most recently filed
    candidates.sort(key=lambda e: e.get("filed", ""), reverse=True)
    return candidates[0]["val"]


def _find_quarter(quarters: list, fy: int, fp: str) -> dict | None:
    for q in quarters:
        if q["fiscal_year"] == fy and q["fiscal_period"] == fp:
            return q
    return None


# ── Main extraction ────────────────────────────────────────────────────────────

def extract_company(cik: str, ticker: str, components: dict = None,
                    fy_start: int = None, fy_end: int = None) -> list:
    """
    Extract all quarterly components for a company.
    Returns a list of quarterly records.
    """
    if components is None:
        components = COMPONENTS

    print(f"Fetching companyfacts for {ticker} (CIK {cik})...")
    facts = fetch_company_facts(cik)

    print("Discovering quarters...")
    quarters = discover_quarters(facts, fy_start, fy_end)
    print(f"Found {len(quarters)} quarters")

    # Try to load company-specific overrides
    company_module = None
    try:
        company_module = import_module(f"companies.{ticker.lower()}")
        print(f"Loaded company overrides: companies/{ticker.lower()}.py")
    except ImportError:
        pass

    # Let company module override components if needed
    if company_module and hasattr(company_module, "get_components"):
        components = company_module.get_components(components)

    results = []
    for q in quarters:
        if "period_end" not in q:
            print(f"  Skipping {q['fiscal_year']} {q['fiscal_period']}: no period_end")
            continue

        record = {
            "ticker": ticker,
            "cik": cik,
            "fiscal_year": q["fiscal_year"],
            "fiscal_period": q["fiscal_period"],
            "period_end": q["period_end"],
            "period_start": q.get("period_start"),
            "form": q["form"],
            "accession": q["accession"],
            "filed": q.get("filed"),
        }

        for comp_name, comp_def in components.items():
            value = None
            used_concept = None

            for concept in comp_def["concepts"]:
                entries = get_concept_entries(facts, concept)
                if not entries:
                    continue

                if comp_def["type"] == "stock":
                    value = extract_stock_value(entries, q["period_end"])
                elif comp_def["type"] == "per_period":
                    value = extract_per_period_value(entries, q)
                elif comp_def["type"] == "flow":
                    value = extract_flow_value_ytd(entries, q, quarters)

                if value is not None:
                    used_concept = concept
                    break

            if value is not None and comp_def.get("negate"):
                value = -value

            # Apply default for components that can legitimately be zero when absent
            if value is None and "default" in comp_def:
                value = comp_def["default"]

            record[comp_name] = value

        # Let company module post-process
        if company_module and hasattr(company_module, "post_process"):
            record = company_module.post_process(record, facts, q, quarters)

        results.append(record)
        status = f"  {q['fiscal_year']} {q['fiscal_period']}: "
        extracted = sum(1 for k, v in record.items()
                        if k in components and v is not None)
        total = len(components)
        status += f"{extracted}/{total} components"
        print(status)

    return results


def main():
    parser = argparse.ArgumentParser(description="Extract financial data from SEC XBRL")
    parser.add_argument("--cik", required=True, help="SEC CIK number")
    parser.add_argument("--ticker", required=True, help="Stock ticker symbol")
    parser.add_argument("--fy-start", type=int, help="Start fiscal year (inclusive)")
    parser.add_argument("--fy-end", type=int, help="End fiscal year (inclusive)")
    parser.add_argument("--output-dir", default="output", help="Output directory")
    args = parser.parse_args()

    results = extract_company(args.cik, args.ticker, fy_start=args.fy_start, fy_end=args.fy_end)

    os.makedirs(args.output_dir, exist_ok=True)
    output_path = os.path.join(args.output_dir, f"{args.ticker.lower()}.json")
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nWrote {len(results)} quarters to {output_path}")


if __name__ == "__main__":
    main()
