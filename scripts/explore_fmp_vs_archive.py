"""Exploratory: compare FMP financials to archive gold for NVDA.

Purpose
-------
Before we commit to a production FMP mapper, we need empirical answers to:
  - What FMP field name corresponds to each XBRL concept?
  - Are FMP's sign conventions consistent with the gold values?
  - Are there concepts FMP is missing or aggregates differently?
  - Does FMP's period_end match SEC's for 52/53-week filers like NVDA?

This script answers all four by cross-referencing FMP's quarterly + annual
endpoints against the 12 archive JSONs in archive/ai_extract/NVDA (FY24 Q1 -> FY26 Q4).

Output
------
  data/exports/fmp_vs_archive_NVDA.csv

One row per (period_end, xbrl_concept) tuple. Columns:
  statement, xbrl_concept, fmp_field, period_end, period_type,
  gold_value_millions, fmp_value_millions, delta, pct_delta, sign_match,
  status (one of: MATCH | DELTA_OK | DELTA_SMALL | DELTA_LARGE | SIGN_FLIP |
                  FMP_MISSING | ARCHIVE_MISSING | UNMAPPED)

Throwaway: this script exists to drive docs/reference/fmp_mapping.md. Once
that doc is written, this script can be deleted.

Scope
-----
- IS: uses 3-month discrete where available (Q2+ archive files have both
  3-month and YTD ranges; we pick the 3-month).
- BS: point-in-time snapshot by period_end date.
- CF: YTD only in archive. This script flags CF comparisons as
  'YTD_IN_ARCHIVE' for Q2/Q3; Q1 CF is directly comparable (YTD = discrete).
  FY CF (from 10-K files) compares to FMP's FY row.
- Unit: archive is USD_millions; FMP is USD. We convert FMP by dividing by
  1,000,000 before comparison.
"""

from __future__ import annotations

import csv
import json
import os
import re
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

REPO_ROOT = Path(__file__).resolve().parents[1]
ARCHIVE_DIR = REPO_ROOT / "archive" / "ai_extract" / "NVDA"
CACHE_DIR = REPO_ROOT / "data" / "raw" / "fmp"
EXPORT_PATH = REPO_ROOT / "data" / "exports" / "fmp_vs_archive_NVDA.csv"

FMP_BASE = "https://financialmodelingprep.com/stable"
USER_AGENT = "arrow-exploration/0.1 (michael@exascale.capital)"

# Tolerances (fraction of larger absolute value)
PCT_DELTA_OK = 0.001      # within 0.1% → MATCH
PCT_DELTA_SMALL = 0.01    # within 1% → DELTA_SMALL (likely rounding)
# Anything larger → DELTA_LARGE


# ---------------------------------------------------------------------------
# Seed mapping: XBRL concept → FMP field name
#
# Inferred from FMP's response keys. This is the seed we compare against;
# the script emits UNMAPPED rows for every XBRL concept not in this dict so
# we can see coverage gaps.
# ---------------------------------------------------------------------------

XBRL_TO_FMP: dict[str, dict] = {
    # Income statement
    "us-gaap:Revenues":                              {"fmp": "revenue",                                  "statement": "income_statement"},
    "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax": {"fmp": "revenue", "statement": "income_statement"},
    "us-gaap:CostOfRevenue":                         {"fmp": "costOfRevenue",                            "statement": "income_statement"},
    "us-gaap:GrossProfit":                           {"fmp": "grossProfit",                              "statement": "income_statement"},
    "us-gaap:ResearchAndDevelopmentExpense":         {"fmp": "researchAndDevelopmentExpenses",           "statement": "income_statement"},
    "us-gaap:SellingGeneralAndAdministrativeExpense":{"fmp": "sellingGeneralAndAdministrativeExpenses",  "statement": "income_statement"},
    "us-gaap:OperatingExpenses":                     {"fmp": "operatingExpenses",                        "statement": "income_statement"},
    "us-gaap:OperatingIncomeLoss":                   {"fmp": "operatingIncome",                          "statement": "income_statement"},
    "us-gaap:InvestmentIncomeInterest":              {"fmp": "interestIncome",                           "statement": "income_statement"},
    "us-gaap:InterestExpense":                       {"fmp": "interestExpense",                          "statement": "income_statement"},
    "us-gaap:NonoperatingIncomeExpense":             {"fmp": "totalOtherIncomeExpensesNet",              "statement": "income_statement"},
    "us-gaap:OtherNonoperatingIncomeExpense":        {"fmp": "nonOperatingIncomeExcludingInterest",      "statement": "income_statement"},
    "us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest":
                                                     {"fmp": "incomeBeforeTax",                          "statement": "income_statement"},
    "us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments":
                                                     {"fmp": "incomeBeforeTax",                          "statement": "income_statement"},
    "us-gaap:IncomeTaxExpenseBenefit":               {"fmp": "incomeTaxExpense",                         "statement": "income_statement"},
    "us-gaap:NetIncomeLoss":                         {"fmp": "netIncome",                                "statement": "income_statement"},
    "us-gaap:EarningsPerShareBasic":                 {"fmp": "eps",                                      "statement": "income_statement"},
    "us-gaap:EarningsPerShareDiluted":               {"fmp": "epsDiluted",                               "statement": "income_statement"},
    "us-gaap:WeightedAverageNumberOfSharesOutstandingBasic": {"fmp": "weightedAverageShsOut",            "statement": "income_statement"},
    "us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding": {"fmp": "weightedAverageShsOutDil",       "statement": "income_statement"},

    # Balance sheet
    "us-gaap:CashAndCashEquivalentsAtCarryingValue": {"fmp": "cashAndCashEquivalents",      "statement": "balance_sheet"},
    "us-gaap:MarketableSecuritiesCurrent":           {"fmp": "shortTermInvestments",        "statement": "balance_sheet"},
    "us-gaap:AccountsReceivableNetCurrent":          {"fmp": "accountsReceivables",         "statement": "balance_sheet"},
    "us-gaap:InventoryNet":                          {"fmp": "inventory",                   "statement": "balance_sheet"},
    "us-gaap:PrepaidExpenseAndOtherAssetsCurrent":   {"fmp": "prepaids",                    "statement": "balance_sheet"},
    "us-gaap:OtherAssetsCurrent":                    {"fmp": "otherCurrentAssets",          "statement": "balance_sheet"},
    "us-gaap:AssetsCurrent":                         {"fmp": "totalCurrentAssets",          "statement": "balance_sheet"},
    "us-gaap:PropertyPlantAndEquipmentNet":          {"fmp": "propertyPlantEquipmentNet",   "statement": "balance_sheet"},
    "us-gaap:Goodwill":                              {"fmp": "goodwill",                    "statement": "balance_sheet"},
    "us-gaap:IntangibleAssetsNetExcludingGoodwill":  {"fmp": "intangibleAssets",            "statement": "balance_sheet"},
    "us-gaap:MarketableSecuritiesNoncurrent":        {"fmp": "longTermInvestments",         "statement": "balance_sheet"},
    "us-gaap:OtherAssetsNoncurrent":                 {"fmp": "otherNonCurrentAssets",       "statement": "balance_sheet"},
    "us-gaap:AssetsNoncurrent":                      {"fmp": "totalNonCurrentAssets",       "statement": "balance_sheet"},
    "us-gaap:Assets":                                {"fmp": "totalAssets",                 "statement": "balance_sheet"},
    "us-gaap:AccountsPayableCurrent":                {"fmp": "accountPayables",             "statement": "balance_sheet"},
    "us-gaap:AccruedLiabilitiesCurrent":             {"fmp": "accruedExpenses",             "statement": "balance_sheet"},
    "us-gaap:AccruedLiabilitiesAndOtherLiabilitiesCurrent": {"fmp": "accruedExpenses",      "statement": "balance_sheet"},
    "us-gaap:ShortTermBorrowings":                   {"fmp": "shortTermDebt",               "statement": "balance_sheet"},
    "us-gaap:LongTermDebtCurrent":                   {"fmp": "shortTermDebt",               "statement": "balance_sheet"},
    "us-gaap:OtherLiabilitiesCurrent":               {"fmp": "otherCurrentLiabilities",     "statement": "balance_sheet"},
    "us-gaap:LiabilitiesCurrent":                    {"fmp": "totalCurrentLiabilities",     "statement": "balance_sheet"},
    "us-gaap:LongTermDebtNoncurrent":                {"fmp": "longTermDebt",                "statement": "balance_sheet"},
    "us-gaap:OtherLiabilitiesNoncurrent":            {"fmp": "otherNonCurrentLiabilities",  "statement": "balance_sheet"},
    "us-gaap:LiabilitiesNoncurrent":                 {"fmp": "totalNonCurrentLiabilities",  "statement": "balance_sheet"},
    "us-gaap:Liabilities":                           {"fmp": "totalLiabilities",            "statement": "balance_sheet"},
    "us-gaap:CommonStocksIncludingAdditionalPaidInCapital": {"fmp": "commonStock",          "statement": "balance_sheet"},
    "us-gaap:RetainedEarningsAccumulatedDeficit":    {"fmp": "retainedEarnings",            "statement": "balance_sheet"},
    "us-gaap:AccumulatedOtherComprehensiveIncomeLossNetOfTax": {"fmp": "accumulatedOtherComprehensiveIncomeLoss", "statement": "balance_sheet"},
    "us-gaap:StockholdersEquity":                    {"fmp": "totalStockholdersEquity",     "statement": "balance_sheet"},
    "us-gaap:LiabilitiesAndStockholdersEquity":      {"fmp": "totalLiabilitiesAndTotalEquity", "statement": "balance_sheet"},

    # Cash flow
    "us-gaap:AllocatedShareBasedCompensationExpense": {"fmp": "stockBasedCompensation",     "statement": "cash_flow"},
    "us-gaap:ShareBasedCompensation":                 {"fmp": "stockBasedCompensation",     "statement": "cash_flow"},
    "us-gaap:DepreciationDepletionAndAmortization":   {"fmp": "depreciationAndAmortization","statement": "cash_flow"},
    "us-gaap:DepreciationAndAmortization":            {"fmp": "depreciationAndAmortization","statement": "cash_flow"},
    "us-gaap:DeferredIncomeTaxExpenseBenefit":        {"fmp": "deferredIncomeTax",          "statement": "cash_flow"},
    "us-gaap:IncreaseDecreaseInAccountsReceivable":   {"fmp": "accountsReceivables",        "statement": "cash_flow"},
    "us-gaap:IncreaseDecreaseInInventories":          {"fmp": "inventory",                  "statement": "cash_flow"},
    "us-gaap:IncreaseDecreaseInAccountsPayable":      {"fmp": "accountsPayables",           "statement": "cash_flow"},
    "us-gaap:NetCashProvidedByUsedInOperatingActivities": {"fmp": "netCashProvidedByOperatingActivities", "statement": "cash_flow"},
    "us-gaap:PaymentsToAcquirePropertyPlantAndEquipment": {"fmp": "investmentsInPropertyPlantAndEquipment", "statement": "cash_flow"},
    "us-gaap:PaymentsToAcquireInvestments":           {"fmp": "purchasesOfInvestments",     "statement": "cash_flow"},
    "us-gaap:ProceedsFromSaleOfAvailableForSaleSecurities": {"fmp": "salesMaturitiesOfInvestments", "statement": "cash_flow"},
    "us-gaap:ProceedsFromSaleAndMaturityOfOtherInvestments": {"fmp": "salesMaturitiesOfInvestments", "statement": "cash_flow"},
    "us-gaap:NetCashProvidedByUsedInInvestingActivities": {"fmp": "netCashProvidedByInvestingActivities", "statement": "cash_flow"},
    "us-gaap:PaymentsOfDividends":                    {"fmp": "netDividendsPaid",           "statement": "cash_flow"},
    "us-gaap:PaymentsOfDividendsCommonStock":         {"fmp": "commonDividendsPaid",        "statement": "cash_flow"},
    "us-gaap:PaymentsForRepurchaseOfCommonStock":     {"fmp": "commonStockRepurchased",     "statement": "cash_flow"},
    "us-gaap:NetCashProvidedByUsedInFinancingActivities": {"fmp": "netCashProvidedByFinancingActivities", "statement": "cash_flow"},
    "us-gaap:CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect":
                                                      {"fmp": "netChangeInCash",            "statement": "cash_flow"},
    "us-gaap:CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents":
                                                      {"fmp": "cashAtEndOfPeriod",          "statement": "cash_flow"},
    "us-gaap:CashAndCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect":
                                                      {"fmp": "netChangeInCash",            "statement": "cash_flow"},
    "us-gaap:IncomeTaxesPaidNet":                     {"fmp": "incomeTaxesPaid",            "statement": "cash_flow"},
    "us-gaap:InterestPaidNet":                        {"fmp": "interestPaid",               "statement": "cash_flow"},
}


# ---------------------------------------------------------------------------
# Archive loading
# ---------------------------------------------------------------------------


@dataclass
class GoldFact:
    statement: str
    xbrl_concept: str
    label: str
    period_end: date          # last day of the reporting period (or instant for BS)
    period_type: str          # 'quarter' | 'annual'
    is_ytd: bool              # True if this value is YTD for multi-quarter periods
    value_millions: float     # signed value
    filename: str             # source archive file


def _infer_period_type(filename: str) -> str:
    return "annual" if "10k" in filename.lower() else "quarter"


def _parse_range_key(key: str, filing_period_end: date) -> tuple[date | None, bool]:
    """Given a range key '2023-01-30_2023-04-30' or instant '2023-04-30',
    return (end_date, is_ytd). is_ytd is True if the range spans >3 months."""
    if "_" in key:
        start_s, end_s = key.split("_")
        start = date.fromisoformat(start_s)
        end = date.fromisoformat(end_s)
        days = (end - start).days
        # Current filing's primary range; either 3-month discrete or multi-month YTD
        if end != filing_period_end:
            # Prior-year comparative — skip, we only want current period
            return None, False
        is_ytd = days > 100   # ~3 months ≈ 90 days; YTD 6M+ is ~180+
        return end, is_ytd
    else:
        end = date.fromisoformat(key)
        if end != filing_period_end:
            return None, False
        return end, False


def _filing_period_end(d: dict) -> date:
    """Extract the filing's primary period_end from the metadata or IS values."""
    # Heuristic: the 3-month (or 12-month for 10-K) range whose end we treat as filing date.
    # Grab the revenue line and use its LARGEST end date across the ranges present — that's
    # the current period for this filing.
    is_items = d["ai_extraction"]["income_statement"]["line_items"]
    ends = set()
    for item in is_items:
        for key in (item.get("values") or {}).keys():
            if "_" in key:
                _, end_s = key.split("_")
                ends.add(date.fromisoformat(end_s))
            else:
                ends.add(date.fromisoformat(key))
    return max(ends)


def load_archive_gold() -> list[GoldFact]:
    files = sorted(ARCHIVE_DIR.glob("q*_fy*_10*.json"))
    facts: list[GoldFact] = []

    for f in files:
        with open(f) as fh:
            d = json.load(fh)

        period_end = _filing_period_end(d)
        period_type = _infer_period_type(f.name)

        for statement_key, items in [
            ("income_statement", d["ai_extraction"]["income_statement"]["line_items"]),
            ("balance_sheet",    d["ai_extraction"]["balance_sheet"]["line_items"]),
            ("cash_flow",        d["ai_extraction"]["cash_flow"]["line_items"]),
        ]:
            for item in items:
                concept = item.get("xbrl_concept")
                if not concept:
                    continue
                values = item.get("values") or {}
                label = item.get("label", "")

                # For IS and CF: find the current period range (3-month for Q1, otherwise
                # prefer 3-month if present; else YTD flagged).
                # For BS: find the instant matching period_end.
                if statement_key == "balance_sheet":
                    for key, v in values.items():
                        if v is None:
                            continue
                        parsed, _ytd = _parse_range_key(key, period_end)
                        if parsed == period_end:
                            facts.append(GoldFact(
                                statement=statement_key,
                                xbrl_concept=concept,
                                label=label,
                                period_end=period_end,
                                period_type=period_type,
                                is_ytd=False,
                                value_millions=float(v),
                                filename=f.name,
                            ))
                            break
                else:
                    # IS / CF: try 3-month first, then YTD
                    three_month = None
                    ytd = None
                    for key, v in values.items():
                        if v is None or "_" not in key:
                            continue
                        start_s, end_s = key.split("_")
                        start = date.fromisoformat(start_s)
                        end = date.fromisoformat(end_s)
                        if end != period_end:
                            continue  # prior-year comparative
                        days = (end - start).days
                        if days <= 100:
                            three_month = float(v)
                        else:
                            ytd = float(v)
                    chosen = three_month if three_month is not None else ytd
                    if chosen is None:
                        continue
                    facts.append(GoldFact(
                        statement=statement_key,
                        xbrl_concept=concept,
                        label=label,
                        period_end=period_end,
                        period_type=period_type,
                        is_ytd=(three_month is None),
                        value_millions=chosen,
                        filename=f.name,
                    ))
    return facts


# ---------------------------------------------------------------------------
# FMP fetching (cached to disk)
# ---------------------------------------------------------------------------


def _fmp_get(endpoint: str, symbol: str, period: str) -> list[dict]:
    """Fetch an FMP endpoint, caching the JSON to data/raw/fmp/{endpoint}/{SYMBOL}/{key}.json."""
    api_key = os.environ["FMP_API"]
    cache_file = CACHE_DIR / endpoint / symbol.upper() / f"{period.lower()}.json"
    if cache_file.exists():
        return json.loads(cache_file.read_text())

    cache_file.parent.mkdir(parents=True, exist_ok=True)
    url = f"{FMP_BASE}/{endpoint}?symbol={symbol}&period={period}&limit=80&apikey={api_key}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req) as r:
        raw = r.read()
    data = json.loads(raw)
    cache_file.write_text(raw.decode("utf-8"))
    return data


def fetch_fmp_all() -> dict[tuple[str, str, str], dict]:
    """Return { (statement, period_type, period_end_iso): fmp_row_dict }."""
    results: dict[tuple[str, str, str], dict] = {}
    for endpoint, statement in [
        ("income-statement",      "income_statement"),
        ("balance-sheet-statement","balance_sheet"),
        ("cash-flow-statement",   "cash_flow"),
    ]:
        for fmp_period, period_type in [("quarter", "quarter"), ("annual", "annual")]:
            rows = _fmp_get(endpoint, "NVDA", fmp_period)
            for row in rows:
                # FMP "period=quarter" returns Q1/Q2/Q3/Q4; "period=annual" returns FY.
                fmp_per = row.get("period", "")
                if fmp_period == "quarter" and fmp_per not in ("Q1", "Q2", "Q3", "Q4"):
                    continue
                if fmp_period == "annual" and fmp_per != "FY":
                    continue
                date_s = row["date"]
                key = (statement, period_type, date_s)
                results[key] = row
    return results


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------


def classify(gold: float, fmp: float) -> tuple[str, float]:
    if fmp is None:
        return "FMP_MISSING", 0.0
    if gold is None:
        return "ARCHIVE_MISSING", 0.0
    sign_flip = (gold != 0 and fmp != 0 and (gold < 0) != (fmp < 0))
    denom = max(abs(gold), abs(fmp))
    pct = 0.0 if denom == 0 else abs(gold - fmp) / denom
    if sign_flip:
        return "SIGN_FLIP", pct
    if pct <= PCT_DELTA_OK:
        return "MATCH", pct
    if pct <= PCT_DELTA_SMALL:
        return "DELTA_SMALL", pct
    return "DELTA_LARGE", pct


def compare(gold_facts: list[GoldFact], fmp_rows: dict) -> list[dict]:
    out_rows: list[dict] = []
    for g in gold_facts:
        mapping = XBRL_TO_FMP.get(g.xbrl_concept)
        if mapping is None:
            out_rows.append({
                "statement":       g.statement,
                "xbrl_concept":    g.xbrl_concept,
                "label":           g.label,
                "fmp_field":       "",
                "period_end":      g.period_end.isoformat(),
                "period_type":     g.period_type,
                "is_ytd":          g.is_ytd,
                "gold_value_m":    g.value_millions,
                "fmp_value_m":     "",
                "delta_m":         "",
                "pct_delta":       "",
                "sign_match":      "",
                "status":          "UNMAPPED",
                "filename":        g.filename,
            })
            continue

        fmp_field = mapping["fmp"]
        key = (g.statement, g.period_type, g.period_end.isoformat())
        fmp_row = fmp_rows.get(key)
        if fmp_row is None:
            status = "FMP_PERIOD_MISSING"
            fmp_value_m = None
        else:
            fmp_raw = fmp_row.get(fmp_field)
            if fmp_raw is None:
                fmp_value_m = None
            else:
                # FMP returns absolute USD; archive is in USD_millions.
                # EPS is per-share, not millions.
                if g.xbrl_concept in ("us-gaap:EarningsPerShareBasic", "us-gaap:EarningsPerShareDiluted"):
                    fmp_value_m = float(fmp_raw)
                else:
                    fmp_value_m = float(fmp_raw) / 1_000_000

        status, pct = classify(g.value_millions, fmp_value_m)
        if fmp_row is None:
            status = "FMP_PERIOD_MISSING"

        delta_m = "" if fmp_value_m is None else g.value_millions - fmp_value_m
        sign_match = "" if fmp_value_m is None else ((g.value_millions >= 0) == (fmp_value_m >= 0))

        # For CF YTD, flag — comparison is apples-to-oranges for Q2/Q3/FY
        if g.statement == "cash_flow" and g.is_ytd and g.period_type == "quarter":
            status = f"YTD_IN_ARCHIVE_vs_FMP_DISCRETE ({status})"

        out_rows.append({
            "statement":     g.statement,
            "xbrl_concept":  g.xbrl_concept,
            "label":         g.label,
            "fmp_field":     fmp_field,
            "period_end":    g.period_end.isoformat(),
            "period_type":   g.period_type,
            "is_ytd":        g.is_ytd,
            "gold_value_m":  g.value_millions,
            "fmp_value_m":   "" if fmp_value_m is None else round(fmp_value_m, 4),
            "delta_m":       "" if delta_m == "" else round(delta_m, 4),
            "pct_delta":     "" if fmp_value_m is None else round(pct, 6),
            "sign_match":    sign_match,
            "status":        status,
            "filename":      g.filename,
        })
    return out_rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    print("Loading archive gold ...", file=sys.stderr)
    gold = load_archive_gold()
    print(f"  {len(gold)} facts across {len(set(g.filename for g in gold))} filings", file=sys.stderr)

    print("Fetching FMP (cached after first run) ...", file=sys.stderr)
    fmp = fetch_fmp_all()
    print(f"  {len(fmp)} FMP rows across 3 statements × {{Q, FY}}", file=sys.stderr)

    print("Comparing ...", file=sys.stderr)
    rows = compare(gold, fmp)

    EXPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    # Sort: largest pct_delta first, then UNMAPPED, then matches
    def sort_key(r):
        status = r["status"]
        if status == "MATCH":
            return (3, 0, r["statement"])
        if status == "UNMAPPED":
            return (2, 0, r["xbrl_concept"])
        pct = r["pct_delta"] if isinstance(r["pct_delta"], (int, float)) else 0.0
        return (1, -pct, r["statement"])

    rows.sort(key=sort_key)
    fieldnames = list(rows[0].keys()) if rows else []
    with open(EXPORT_PATH, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    # Summary
    from collections import Counter
    status_counts = Counter(r["status"].split(" ")[0] if "(" in r["status"] else r["status"] for r in rows)
    print("\n=== Summary ===", file=sys.stderr)
    for s, c in status_counts.most_common():
        print(f"  {s:<30} {c:>5}", file=sys.stderr)
    print(f"\nWrote {EXPORT_PATH}", file=sys.stderr)


if __name__ == "__main__":
    main()
