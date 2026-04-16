"""
Metric calculation script: computes financial metrics from extracted data.

Reads extraction output from output/{ticker}.json,
calculates all metrics defined in formulas.md, and writes enriched JSON to
dashboard/data/{ticker}.json.

Usage:
    python3 calculate.py --ticker NVDA
    python3 calculate.py --all
"""

import argparse
import glob
import json
import os

OUTPUT_DIR = "output"
DASHBOARD_DATA_DIR = "dashboard/data"

PERIOD_ORDER = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}


# ── Helpers ───────────────────────────────────────────────────────────────────

def safe_divide(numerator, denominator, suppress_negative=False):
    """Divide safely. Returns None if denominator is 0, None, or negative when suppress_negative."""
    if numerator is None or denominator is None:
        return None
    if denominator == 0:
        return None
    if suppress_negative and denominator < 0:
        return None
    return numerator / denominator


def ttm(records, idx, field):
    """Sum field over 4 quarters ending at idx. Returns None if insufficient data."""
    if idx < 3:
        return None
    vals = [records[i].get(field) for i in range(idx - 3, idx + 1)]
    if any(v is None for v in vals):
        return None
    return sum(vals)


def ttm_computed(records, idx, func):
    """TTM sum using a per-record computation function. Returns None if insufficient data."""
    if idx < 3:
        return None
    vals = [func(records[i]) for i in range(idx - 3, idx + 1)]
    if any(v is None for v in vals):
        return None
    return sum(vals)


def tax_rate_ttm(records, idx):
    """TTM effective tax rate, fallback 21% if pretax income <= 0."""
    tax = ttm(records, idx, "income_tax_expense_q")
    pretax = ttm(records, idx, "pretax_income_q")
    if tax is None or pretax is None or pretax <= 0:
        return 0.21
    rate = tax / pretax
    if rate < 0 or rate > 1:
        return 0.21
    return rate


def invested_capital(record):
    """Adjusted invested capital from a single quarter's balance sheet."""
    fields = ["equity_q", "short_term_debt_q", "long_term_debt_q",
              "operating_lease_liabilities_q", "cash_q",
              "short_term_investments_q", "rd_asset_q"]
    vals = {f: record.get(f) for f in fields}
    if any(v is None for v in vals.values()):
        return None
    return (vals["equity_q"] + vals["short_term_debt_q"] + vals["long_term_debt_q"]
            + vals["operating_lease_liabilities_q"]
            - vals["cash_q"] - vals["short_term_investments_q"]
            + vals["rd_asset_q"])


def gross_profit_q(record):
    """Quarterly gross profit."""
    rev = record.get("revenue_q")
    cogs = record.get("cogs_q")
    if rev is None or cogs is None:
        return None
    return rev - cogs


def adj_nopat_q(record, tax_rate):
    """Single quarter adjusted NOPAT."""
    oi = record.get("operating_income_q")
    rd_adj = record.get("rd_OI_adjustment_q")
    if oi is None or rd_adj is None:
        return None
    return (oi + rd_adj) * (1 - tax_rate)


def nwc(record):
    """Net working capital: AR + Inventory - AP."""
    ar = record.get("accounts_receivable_q")
    inv = record.get("inventory_q")
    ap = record.get("accounts_payable_q")
    if any(v is None for v in [ar, inv, ap]):
        return None
    return ar + inv - ap


# ── Split normalization ───────────────────────────────────────────────────────

def normalize_splits(records):
    """
    Detect stock splits/reverse splits and add diluted_shares_split_adjusted_q.
    Normalizes all quarters to the most recent share basis.
    """
    # Collect split events walking chronologically
    split_events = []  # list of (index, ratio) where ratio = shares[i+1] / shares[i]
    for i in range(len(records) - 1):
        cur = records[i].get("diluted_shares_q")
        nxt = records[i + 1].get("diluted_shares_q")
        if cur is None or nxt is None or cur == 0:
            continue
        ratio = nxt / cur
        if ratio >= 1.5:
            # Forward split — round to nearest 0.5
            rounded = round(ratio * 2) / 2
            split_events.append((i, rounded))
            print(f"  Split detected between {records[i]['fiscal_period']} FY{records[i]['fiscal_year']} "
                  f"and {records[i+1]['fiscal_period']} FY{records[i+1]['fiscal_year']}: "
                  f"{rounded}:1 forward split")
        elif ratio <= 0.67:
            rounded = round(ratio * 2) / 2
            if rounded == 0:
                rounded = ratio  # very large reverse split, keep exact
            split_events.append((i, rounded))
            print(f"  Split detected between {records[i]['fiscal_period']} FY{records[i]['fiscal_year']} "
                  f"and {records[i+1]['fiscal_period']} FY{records[i+1]['fiscal_year']}: "
                  f"1:{round(1/rounded)} reverse split")

    # Build cumulative adjustment factor for each quarter
    # Work backwards from the end: the most recent quarter has factor 1.0
    # Each split event multiplies the factor for all preceding quarters
    factors = [1.0] * len(records)
    for split_idx, ratio in split_events:
        for i in range(split_idx + 1):
            factors[i] *= ratio

    for i, record in enumerate(records):
        shares = record.get("diluted_shares_q")
        if shares is not None:
            record["diluted_shares_split_adjusted_q"] = round(shares * factors[i])
        else:
            record["diluted_shares_split_adjusted_q"] = None


# ── Metric functions ──────────────────────────────────────────────────────────
# Each returns a dict of computed fields (None for not-yet-calculable)

def calc_roic(records, idx):
    """Metric 1: ROIC (Adjusted) = Adjusted NOPAT TTM / Average Adjusted Invested Capital"""
    if idx < 4:
        return {"roic_adjusted": None}
    tax = tax_rate_ttm(records, idx)
    nopat = ttm_computed(records, idx, lambda r: adj_nopat_q(r, tax))
    ic_end = invested_capital(records[idx])
    ic_begin = invested_capital(records[idx - 4])
    if any(v is None for v in [nopat, ic_end, ic_begin]):
        return {"roic_adjusted": None}
    avg_ic = (ic_begin + ic_end) / 2
    return {"roic_adjusted": safe_divide(nopat, avg_ic, suppress_negative=True)}


def calc_roiic(records, idx):
    """Metric 2: ROIIC = Delta Adjusted NOPAT TTM / Delta Adjusted Invested Capital"""
    if idx < 7:
        return {"roiic": None}
    tax_cur = tax_rate_ttm(records, idx)
    tax_prior = tax_rate_ttm(records, idx - 4)
    nopat_cur = ttm_computed(records, idx, lambda r: adj_nopat_q(r, tax_cur))
    nopat_prior = ttm_computed(records, idx - 4, lambda r: adj_nopat_q(r, tax_prior))
    ic_cur = invested_capital(records[idx])
    ic_prior = invested_capital(records[idx - 4])
    if any(v is None for v in [nopat_cur, nopat_prior, ic_cur, ic_prior]):
        return {"roiic": None}
    delta_nopat = nopat_cur - nopat_prior
    delta_ic = ic_cur - ic_prior
    return {"roiic": safe_divide(delta_nopat, delta_ic)}


def calc_reinvestment_rate(records, idx):
    """Metric 3: Reinvestment Rate = Reinvestment TTM / Adjusted NOPAT TTM"""
    if idx < 4:
        return {"reinvestment_rate": None, "reinvestment_ttm": None}

    capex = ttm(records, idx, "capex_q")
    dna = ttm(records, idx, "dna_q")
    acq = ttm(records, idx, "acquisitions_q")

    nwc_cur = nwc(records[idx])
    nwc_prior = nwc(records[idx - 4])

    rd_asset_cur = records[idx].get("rd_asset_q")
    rd_asset_prior = records[idx - 4].get("rd_asset_q")

    if any(v is None for v in [capex, dna, acq, nwc_cur, nwc_prior,
                                rd_asset_cur, rd_asset_prior]):
        return {"reinvestment_rate": None, "reinvestment_ttm": None}

    delta_nwc = nwc_cur - nwc_prior
    delta_rd_asset = rd_asset_cur - rd_asset_prior

    # CapEx and acquisitions stored as negative, use abs
    reinvestment = abs(capex) + delta_nwc + abs(acq) - dna + delta_rd_asset

    tax = tax_rate_ttm(records, idx)
    nopat = ttm_computed(records, idx, lambda r: adj_nopat_q(r, tax))

    return {
        "reinvestment_ttm": round(reinvestment),
        "reinvestment_rate": safe_divide(reinvestment, nopat, suppress_negative=True),
    }


def calc_gross_profit_growth(records, idx):
    """Metric 4: Gross Profit TTM Growth YoY"""
    if idx < 7:
        return {"gross_profit_ttm_growth": None}
    gp_cur = ttm_computed(records, idx, gross_profit_q)
    gp_prior = ttm_computed(records, idx - 4, gross_profit_q)
    if any(v is None for v in [gp_cur, gp_prior]):
        return {"gross_profit_ttm_growth": None}
    return {"gross_profit_ttm_growth": safe_divide(gp_cur - gp_prior, gp_prior,
                                                    suppress_negative=True)}


def calc_revenue_growth_yoy(records, idx):
    """Metric 5a: Revenue Growth YoY (TTM)"""
    if idx < 7:
        return {"revenue_growth_yoy": None}
    rev_cur = ttm(records, idx, "revenue_q")
    rev_prior = ttm(records, idx - 4, "revenue_q")
    if any(v is None for v in [rev_cur, rev_prior]):
        return {"revenue_growth_yoy": None}
    return {"revenue_growth_yoy": safe_divide(rev_cur - rev_prior, rev_prior,
                                               suppress_negative=True)}


def calc_revenue_growth_qoq(records, idx):
    """Metric 5b: Revenue Growth QoQ Annualized"""
    if idx < 1:
        return {"revenue_growth_qoq_ann": None}
    cur = records[idx].get("revenue_q")
    prior = records[idx - 1].get("revenue_q")
    if cur is None or prior is None or prior <= 0:
        return {"revenue_growth_qoq_ann": None}
    return {"revenue_growth_qoq_ann": (cur / prior) ** 4 - 1}


def calc_incremental_gross_margin(records, idx):
    """Metric 6: Incremental Gross Margin = Delta GP TTM / Delta Revenue TTM"""
    if idx < 7:
        return {"incremental_gross_margin": None}
    gp_cur = ttm_computed(records, idx, gross_profit_q)
    gp_prior = ttm_computed(records, idx - 4, gross_profit_q)
    rev_cur = ttm(records, idx, "revenue_q")
    rev_prior = ttm(records, idx - 4, "revenue_q")
    if any(v is None for v in [gp_cur, gp_prior, rev_cur, rev_prior]):
        return {"incremental_gross_margin": None}
    delta_rev = rev_cur - rev_prior
    delta_gp = gp_cur - gp_prior
    return {"incremental_gross_margin": safe_divide(delta_gp, delta_rev)}


def calc_incremental_operating_margin(records, idx):
    """Metric 7: Incremental Operating Margin = Delta OI TTM / Delta Revenue TTM"""
    if idx < 7:
        return {"incremental_operating_margin": None}
    oi_cur = ttm(records, idx, "operating_income_q")
    oi_prior = ttm(records, idx - 4, "operating_income_q")
    rev_cur = ttm(records, idx, "revenue_q")
    rev_prior = ttm(records, idx - 4, "revenue_q")
    if any(v is None for v in [oi_cur, oi_prior, rev_cur, rev_prior]):
        return {"incremental_operating_margin": None}
    return {"incremental_operating_margin": safe_divide(oi_cur - oi_prior,
                                                         rev_cur - rev_prior)}


def calc_nopat_margin(records, idx):
    """Metric 8: NOPAT Margin = Adjusted NOPAT TTM / Revenue TTM"""
    if idx < 3:
        return {"nopat_margin": None}
    tax = tax_rate_ttm(records, idx)
    nopat = ttm_computed(records, idx, lambda r: adj_nopat_q(r, tax))
    rev = ttm(records, idx, "revenue_q")
    if any(v is None for v in [nopat, rev]):
        return {"nopat_margin": None}
    return {"nopat_margin": safe_divide(nopat, rev, suppress_negative=True)}


def calc_cfo_nopat(records, idx):
    """Metric 9: CFO / NOPAT"""
    if idx < 3:
        return {"cfo_over_nopat": None}
    cfo = ttm(records, idx, "cfo_q")
    tax = tax_rate_ttm(records, idx)
    nopat = ttm_computed(records, idx, lambda r: adj_nopat_q(r, tax))
    if any(v is None for v in [cfo, nopat]):
        return {"cfo_over_nopat": None}
    return {"cfo_over_nopat": safe_divide(cfo, nopat, suppress_negative=True)}


def calc_fcf_nopat(records, idx):
    """Metric 10: FCF / NOPAT = (CFO - CapEx) / NOPAT"""
    if idx < 3:
        return {"fcf_over_nopat": None}
    cfo = ttm(records, idx, "cfo_q")
    capex = ttm(records, idx, "capex_q")
    tax = tax_rate_ttm(records, idx)
    nopat = ttm_computed(records, idx, lambda r: adj_nopat_q(r, tax))
    if any(v is None for v in [cfo, capex, nopat]):
        return {"fcf_over_nopat": None}
    fcf = cfo - abs(capex)  # capex stored negative, CFO - |CapEx|
    return {"fcf_over_nopat": safe_divide(fcf, nopat, suppress_negative=True)}


def calc_accruals_ratio(records, idx):
    """Metric 11: Accruals Ratio = (Net Income TTM - CFO TTM) / Avg Total Assets"""
    if idx < 4:
        return {"accruals_ratio": None}
    ni = ttm(records, idx, "net_income_q")
    cfo = ttm(records, idx, "cfo_q")
    assets_cur = records[idx].get("total_assets_q")
    assets_prior = records[idx - 4].get("total_assets_q")
    if any(v is None for v in [ni, cfo, assets_cur, assets_prior]):
        return {"accruals_ratio": None}
    avg_assets = (assets_cur + assets_prior) / 2
    return {"accruals_ratio": safe_divide(ni - cfo, avg_assets)}


def calc_ccc(records, idx):
    """Metric 12: Cash Conversion Cycle = DSO + DIO - DPO"""
    if idx < 3:
        return {"cash_conversion_cycle": None}
    dso_dio_dpo = calc_dso_dio_dpo(records, idx)
    dso = dso_dio_dpo.get("dso")
    dio = dso_dio_dpo.get("dio")
    dpo = dso_dio_dpo.get("dpo")
    if any(v is None for v in [dso, dio, dpo]):
        return {"cash_conversion_cycle": None}
    return {"cash_conversion_cycle": dso + dio - dpo}


def calc_sbc_pct_revenue(records, idx):
    """Metric 13: SBC as % of Revenue TTM"""
    if idx < 3:
        return {"sbc_pct_revenue": None}
    sbc = ttm(records, idx, "sbc_q")
    rev = ttm(records, idx, "revenue_q")
    if any(v is None for v in [sbc, rev]):
        return {"sbc_pct_revenue": None}
    return {"sbc_pct_revenue": safe_divide(sbc, rev, suppress_negative=True)}


def calc_diluted_share_growth(records, idx):
    """Metric 14: Diluted Share Count Growth YoY (same quarter)"""
    if idx < 4:
        return {"diluted_share_growth": None}
    cur = records[idx].get("diluted_shares_split_adjusted_q")
    prior = records[idx - 4].get("diluted_shares_split_adjusted_q")
    if cur is None or prior is None or prior == 0:
        return {"diluted_share_growth": None}
    return {"diluted_share_growth": (cur - prior) / prior}


def calc_net_debt(records, idx):
    """Metric 15: Net Debt and Net Debt / EBITDA TTM"""
    r = records[idx]
    fields = ["short_term_debt_q", "long_term_debt_q", "operating_lease_liabilities_q",
              "cash_q", "short_term_investments_q"]
    vals = {f: r.get(f) for f in fields}
    if any(v is None for v in vals.values()):
        return {"net_debt": None, "net_debt_ebitda": None}

    nd = (vals["short_term_debt_q"] + vals["long_term_debt_q"]
          + vals["operating_lease_liabilities_q"]
          - vals["cash_q"] - vals["short_term_investments_q"])

    # EBITDA ratio needs TTM
    ratio = None
    if idx >= 3:
        oi = ttm(records, idx, "operating_income_q")
        dna = ttm(records, idx, "dna_q")
        if oi is not None and dna is not None:
            ebitda = oi + dna
            ratio = safe_divide(nd, ebitda)

    return {"net_debt": round(nd), "net_debt_ebitda": ratio}


def calc_interest_coverage(records, idx):
    """Metric 16: Interest Coverage = OI / |Interest Expense|"""
    oi = records[idx].get("operating_income_q")
    ie = records[idx].get("interest_expense_q")
    if oi is None or ie is None or ie == 0:
        return {"interest_coverage": None}
    return {"interest_coverage": safe_divide(oi, abs(ie))}


def calc_revenue_per_employee(records, idx):
    """Metric 18: Revenue per Employee = Revenue TTM / Employee Count"""
    if idx < 3:
        return {"revenue_per_employee": None}
    rev = ttm(records, idx, "revenue_q")
    emp = records[idx].get("employee_count")
    if rev is None or emp is None or emp == 0:
        return {"revenue_per_employee": None}
    return {"revenue_per_employee": round(rev / emp)}


def calc_working_capital_intensity(records, idx):
    """Metric 19: Working Capital Intensity = NWC / Revenue TTM"""
    if idx < 3:
        return {"working_capital_intensity": None}
    wc = nwc(records[idx])
    rev = ttm(records, idx, "revenue_q")
    if wc is None or rev is None:
        return {"working_capital_intensity": None}
    return {"working_capital_intensity": safe_divide(wc, rev, suppress_negative=True)}


def calc_dso_dio_dpo(records, idx):
    """Metric 20: DSO, DIO, DPO"""
    if idx < 3:
        return {"dso": None, "dio": None, "dpo": None}
    r = records[idx]
    ar = r.get("accounts_receivable_q")
    inv = r.get("inventory_q")
    ap = r.get("accounts_payable_q")
    rev = ttm(records, idx, "revenue_q")
    cogs = ttm(records, idx, "cogs_q")

    dso = safe_divide(ar, rev) * 365 if ar is not None and rev else None
    dio = safe_divide(inv, cogs) * 365 if inv is not None and cogs else None
    dpo = safe_divide(ap, cogs) * 365 if ap is not None and cogs else None

    return {"dso": dso, "dio": dio, "dpo": dpo}


def calc_unlevered_fcf(records, idx):
    """Metric 21: Unlevered FCF = CFO + |Interest| * (1 - Tax Rate) - |CapEx|"""
    if idx < 3:
        return {"unlevered_fcf": None}
    cfo = ttm(records, idx, "cfo_q")
    ie = ttm(records, idx, "interest_expense_q")
    capex = ttm(records, idx, "capex_q")
    if any(v is None for v in [cfo, ie, capex]):
        return {"unlevered_fcf": None}
    tax = tax_rate_ttm(records, idx)
    ufcf = cfo + abs(ie) * (1 - tax) - abs(capex)
    return {"unlevered_fcf": round(ufcf)}


# ── Orchestrator ──────────────────────────────────────────────────────────────

METRIC_FUNCS = [
    calc_roic,
    calc_roiic,
    calc_reinvestment_rate,
    calc_gross_profit_growth,
    calc_revenue_growth_yoy,
    calc_revenue_growth_qoq,
    calc_incremental_gross_margin,
    calc_incremental_operating_margin,
    calc_nopat_margin,
    calc_cfo_nopat,
    calc_fcf_nopat,
    calc_accruals_ratio,
    calc_ccc,
    calc_sbc_pct_revenue,
    calc_diluted_share_growth,
    calc_net_debt,
    calc_interest_coverage,
    calc_revenue_per_employee,
    calc_working_capital_intensity,
    calc_dso_dio_dpo,
    calc_unlevered_fcf,
]


def forward_fill_lease_liabilities(records):
    """Forward-fill operating lease liabilities across quarters.

    Some companies (e.g. FCX) only disclose lease liabilities in 10-K filings,
    leaving 10-Q quarters as 0. Carry the most recent non-zero value forward
    so metrics like ROIC and Net Debt don't have quarterly discontinuities.
    """
    last_value = 0
    filled = 0
    for r in records:
        val = r.get("operating_lease_liabilities_q", 0) or 0
        if val != 0:
            last_value = val
        elif last_value != 0:
            r["operating_lease_liabilities_q"] = last_value
            filled += 1
    if filled:
        print(f"  Forward-filled operating_lease_liabilities_q for {filled} quarters")


def calculate_all_metrics(records):
    """Compute all metrics for a sorted list of quarterly records."""
    records.sort(key=lambda r: (r["fiscal_year"], PERIOD_ORDER[r["fiscal_period"]]))

    print("Normalizing share counts for splits...")
    normalize_splits(records)

    forward_fill_lease_liabilities(records)

    print("Computing metrics...")
    for idx, record in enumerate(records):
        for func in METRIC_FUNCS:
            result = func(records, idx)
            record.update(result)

    return records


# ── Main ──────────────────────────────────────────────────────────────────────

def process_ticker(ticker):
    """Process a single ticker: read extraction output, calculate metrics, write dashboard data."""
    input_path = os.path.join(OUTPUT_DIR, f"{ticker.lower()}.json")
    if not os.path.exists(input_path):
        print(f"No extraction output found at {input_path}")
        return False

    print(f"\n{'='*60}")
    print(f"Calculating metrics for {ticker}")
    print(f"{'='*60}")

    with open(input_path) as f:
        records = json.load(f)

    if not records:
        print("No records found.")
        return False

    records = calculate_all_metrics(records)

    # Write enriched output
    os.makedirs(DASHBOARD_DATA_DIR, exist_ok=True)
    output_path = os.path.join(DASHBOARD_DATA_DIR, f"{ticker.lower()}.json")
    with open(output_path, "w") as f:
        json.dump(records, f, indent=2)
    print(f"\nWrote {len(records)} quarters to {output_path}")

    # Print summary
    metric_keys = set()
    for func in METRIC_FUNCS:
        result = func(records, 0)
        metric_keys.update(result.keys())

    for r in records:
        computed = sum(1 for k in metric_keys if r.get(k) is not None)
        print(f"  FY{r['fiscal_year']} {r['fiscal_period']}: {computed}/{len(metric_keys)} metrics")

    return True


def update_manifest(tickers):
    """Write manifest.json listing available tickers."""
    manifest_path = os.path.join(DASHBOARD_DATA_DIR, "manifest.json")
    with open(manifest_path, "w") as f:
        json.dump(sorted(tickers), f, indent=2)
    print(f"\nUpdated {manifest_path}: {sorted(tickers)}")


def main():
    parser = argparse.ArgumentParser(description="Calculate financial metrics")
    parser.add_argument("--ticker", help="Stock ticker (or use --all)")
    parser.add_argument("--all", action="store_true", help="Process all tickers in output/")
    args = parser.parse_args()

    if not args.ticker and not args.all:
        parser.error("Specify --ticker or --all")

    if args.all:
        pattern = os.path.join(OUTPUT_DIR, "*.json")
        files = glob.glob(pattern)
        tickers = [os.path.splitext(os.path.basename(f))[0].upper() for f in files]
        processed = []
        for ticker in sorted(tickers):
            if process_ticker(ticker):
                processed.append(ticker.lower())
        update_manifest(processed)
    else:
        ticker = args.ticker.upper()
        if process_ticker(ticker):
            # Update manifest with all existing dashboard data files
            existing = glob.glob(os.path.join(DASHBOARD_DATA_DIR, "*.json"))
            tickers = [os.path.splitext(os.path.basename(f))[0]
                        for f in existing if "manifest" not in f]
            update_manifest(tickers)

    print("\nServe dashboard: python3 -m http.server 8000 --directory dashboard")


if __name__ == "__main__":
    main()
