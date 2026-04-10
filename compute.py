"""
Post-extraction computations: R&D capitalization and employee count.

Runs after extract.py. Takes the extracted quarterly JSON, adds:
- rd_amortization_q: quarterly R&D amortization (20-quarter schedule)
- rd_asset_q: unamortized R&D balance
- rd_OI_adjustment_q: R&D(t) - Amortization(t)
- employee_count: from 10-K HTML, carried forward

Usage:
    python3 compute.py --ticker NVDA
    python3 compute.py --ticker NVDA --fy-start 2024 --fy-end 2026
"""

import argparse
import json
import os
import re
from extract import parse_xbrl, classify_contexts, DATA_DIR


# ── R&D Capitalization ─────────────────────────────────────────────────────────

def get_annual_rd_history(ticker: str, first_quarter: dict) -> list[tuple[str, int]]:
    """
    Extract annual R&D expense for years before the extraction window.

    Scans all downloaded 10-Ks, preferring the most recent filing's values
    (most accurate due to restatements). Returns entries for years whose
    period ends before the first extraction quarter.

    Returns list of (end_date, annual_rd) sorted oldest to newest.
    """
    ticker_dir = os.path.join(DATA_DIR, ticker)
    first_date = first_quarter["period_end"]

    # Collect all 10-Ks, most recent last
    ten_ks = []
    for dirname in sorted(os.listdir(ticker_dir)):
        meta_path = os.path.join(ticker_dir, dirname, "filing_meta.json")
        if not os.path.exists(meta_path):
            continue
        with open(meta_path) as f:
            meta = json.load(f)
        if meta["form"] != "10-K":
            continue
        meta["dir"] = os.path.join(ticker_dir, dirname)
        ten_ks.append(meta)

    ten_ks.sort(key=lambda m: m["report_date"])

    # Extract annual R&D from each 10-K, most recent wins (iterate newest first)
    rd_by_end_date = {}
    for meta in reversed(ten_ks):
        xbrl_path = os.path.join(meta["dir"], meta["xbrl_filename"])
        contexts, facts, nsmap = parse_xbrl(xbrl_path)

        rd_entries = facts.get("ResearchAndDevelopmentExpense", [])
        for cref, val in rd_entries:
            ctx = contexts.get(cref, {})
            if ctx.get("type") != "duration":
                continue
            days = ctx.get("days", 0)
            if 340 < days < 380:
                end_date = ctx["end"]
                # Only keep if this year hasn't been set by a more recent 10-K
                if end_date not in rd_by_end_date:
                    rd_by_end_date[end_date] = val

    # Filter to years ending before the extraction window, keep most recent 3
    annual_rd = [(end_date, val) for end_date, val in rd_by_end_date.items()
                 if end_date < first_date]
    annual_rd.sort()
    annual_rd = annual_rd[-3:]

    if annual_rd:
        for end_date, val in annual_rd:
            print(f"  R&D lookback: {end_date} = {val:>15,}")
    else:
        print("  Warning: no prior annual R&D found in downloaded 10-Ks")

    return annual_rd


def build_quarterly_rd_series(annual_rd: list, quarterly_rd: list[int]) -> list[float]:
    """
    Build the full quarterly R&D series for the 20-quarter amortization schedule.

    annual_rd: [(end_date, value), ...] from prior 10-K (3 years, oldest first)
    quarterly_rd: [rd_q1, rd_q2, ...] actual quarterly R&D from extraction

    Returns a list where index 0 is the oldest estimated quarter and the last
    entries are the actual quarterly values.
    """
    # Estimated quarterly values from annual (divide by 4)
    estimated = []
    for _, annual_val in annual_rd:
        quarterly_est = annual_val / 4
        estimated.extend([quarterly_est] * 4)

    # Combine: estimated quarters first, then actual quarters
    full_series = estimated + quarterly_rd
    return full_series


def compute_rd_capitalization(rd_series: list[float], num_actual_quarters: int,
                              amort_life: int = 20) -> list[dict]:
    """
    Compute R&D amortization, asset, and OI adjustment for each actual quarter.

    rd_series: full quarterly R&D series (estimated + actual)
    num_actual_quarters: how many actual quarters are at the end of the series
    amort_life: number of quarters for straight-line amortization (default 20)

    Returns list of dicts (one per actual quarter) with:
      rd_amortization_q, rd_asset_q, rd_OI_adjustment_q
    """
    num_estimated = len(rd_series) - num_actual_quarters
    results = []

    for i in range(num_estimated, len(rd_series)):
        # i is the index of the current quarter in the full series
        current_rd = rd_series[i]

        amort = 0.0
        asset = 0.0

        for j in range(amort_life):
            idx = i - j
            if idx < 0:
                break  # no data that far back, treated as 0
            rd_j = rd_series[idx]

            # Amortization: each vintage contributes rd/N per quarter
            amort += rd_j / amort_life

            # Asset: each vintage weighted by remaining life
            remaining_weight = (amort_life - j) / amort_life
            asset += rd_j * remaining_weight

        oi_adj = current_rd - amort

        results.append({
            "rd_amortization_q": round(amort),
            "rd_asset_q": round(asset),
            "rd_OI_adjustment_q": round(oi_adj),
        })

    return results


# ── Employee Count ─────────────────────────────────────────────────────────────

def extract_employee_count_from_html(html_path: str) -> int | None:
    """
    Extract total employee count from a 10-K HTML filing.
    Looks for patterns like "26,196 employees" or "approximately 36,000 employees".
    """
    try:
        with open(html_path, "r", errors="replace") as f:
            html = f.read()
    except FileNotFoundError:
        return None

    # Strip HTML tags for cleaner text matching
    text = re.sub(r'<[^>]+>', ' ', html)
    text = re.sub(r'\s+', ' ', text)

    # Find all "N employees" or "N full-time employees" patterns
    matches = re.findall(r'([\d,]+)\s*(?:full-time\s+)?employees', text, re.IGNORECASE)
    if not matches:
        return None

    # Parse all matches, take the largest (total headcount > subgroup counts)
    counts = []
    for m in matches:
        try:
            n = int(m.replace(",", ""))
            if n >= 100:  # filter out tiny numbers and years
                counts.append(n)
        except ValueError:
            continue

    return max(counts) if counts else None


def get_employee_counts(ticker: str) -> dict:
    """
    Extract employee count from each 10-K filing.
    Returns {report_date: employee_count}.
    """
    ticker_dir = os.path.join(DATA_DIR, ticker)
    counts = {}

    for dirname in sorted(os.listdir(ticker_dir)):
        meta_path = os.path.join(ticker_dir, dirname, "filing_meta.json")
        if not os.path.exists(meta_path):
            continue
        with open(meta_path) as f:
            meta = json.load(f)
        if meta["form"] != "10-K":
            continue

        html_path = os.path.join(ticker_dir, dirname, meta["html_filename"])
        count = extract_employee_count_from_html(html_path)
        if count:
            counts[meta["report_date"]] = count
            print(f"  10-K {meta['report_date']}: {count:,} employees")

    return counts


def assign_employee_counts(records: list, employee_counts: dict) -> None:
    """
    Assign employee count to each quarterly record.
    For Q4 (10-K quarter): use that 10-K's count.
    For Q1-Q3: carry forward from the most recent 10-K.
    """
    # Sort 10-K dates
    sorted_dates = sorted(employee_counts.keys())

    for record in records:
        period_end = record["period_end"]
        # Find the most recent 10-K on or before this quarter's period_end
        applicable = None
        for d in sorted_dates:
            if d <= period_end:
                applicable = d
        record["employee_count"] = employee_counts.get(applicable) if applicable else None


# ── Main ───────────────────────────────────────────────────────────────────────

def compute_all(ticker: str, fy_start: int = None, fy_end: int = None):
    """Add R&D capitalization and employee count to extracted data."""
    output_path = os.path.join("output", f"{ticker.lower()}.json")
    with open(output_path) as f:
        records = json.load(f)

    if not records:
        print("No records found.")
        return

    # Filter by fiscal year if specified
    if fy_start:
        records = [r for r in records if r["fiscal_year"] >= fy_start]
    if fy_end:
        records = [r for r in records if r["fiscal_year"] <= fy_end]

    records.sort(key=lambda r: (r["fiscal_year"], {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}[r["fiscal_period"]]))

    # ── R&D Capitalization ──
    print("Computing R&D capitalization...")
    quarterly_rd = [r.get("rd_expense_q") for r in records]
    if all(v is not None for v in quarterly_rd):
        annual_rd = get_annual_rd_history(ticker, records[0])
        if annual_rd:
            rd_series = build_quarterly_rd_series(annual_rd, quarterly_rd)
            rd_results = compute_rd_capitalization(rd_series, len(quarterly_rd))

            for i, record in enumerate(records):
                record.update(rd_results[i])
                print(f"  FY{record['fiscal_year']} {record['fiscal_period']}: "
                      f"amort={rd_results[i]['rd_amortization_q']:>15,}  "
                      f"asset={rd_results[i]['rd_asset_q']:>15,}  "
                      f"OI_adj={rd_results[i]['rd_OI_adjustment_q']:>13,}")
    else:
        print("  Skipping: some quarters missing R&D expense data")

    # ── Employee Count ──
    print("\nExtracting employee counts...")
    employee_counts = get_employee_counts(ticker)
    assign_employee_counts(records, employee_counts)

    # Save updated records
    with open(output_path, "w") as f:
        json.dump(records, f, indent=2)
    print(f"\nUpdated {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Compute R&D capitalization and employee count")
    parser.add_argument("--ticker", required=True, help="Stock ticker")
    parser.add_argument("--fy-start", type=int, help="Start fiscal year")
    parser.add_argument("--fy-end", type=int, help="End fiscal year")
    args = parser.parse_args()

    compute_all(args.ticker, args.fy_start, args.fy_end)


if __name__ == "__main__":
    main()
