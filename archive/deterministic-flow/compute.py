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
import html as html_module
import json
import os
import re
DATA_DIR = "data/filings"


# ── R&D Capitalization ─────────────────────────────────────────────────────────

def compute_rd_capitalization(quarterly_rd: list, amort_life: int = 20) -> list[dict]:
    """
    Compute R&D amortization, asset, and OI adjustment for each quarter.

    Uses actual quarterly R&D values directly. For each quarter, looks back
    up to amort_life quarters. If fewer quarters of history exist, the
    missing lags are treated as 0.

    quarterly_rd: [rd_q1, rd_q2, ...] actual quarterly R&D from extraction
    amort_life: number of quarters for straight-line amortization (default 20)

    Returns list of dicts (one per quarter) with:
      rd_amortization_q, rd_asset_q, rd_OI_adjustment_q
    """
    results = []

    for i in range(len(quarterly_rd)):
        current_rd = quarterly_rd[i]
        # Use integer numerators, divide once at the end to avoid float accumulation
        amort_num = 0  # sum of rd_j values (divide by amort_life at end)
        asset_num = 0  # sum of rd_j × (N-1-j) (divide by amort_life at end)

        for j in range(amort_life):
            idx = i - j
            if idx < 0:
                break
            rd_j = quarterly_rd[idx]
            amort_num += rd_j
            weight = amort_life - 1 - j
            if weight > 0:
                asset_num += rd_j * weight

        amort = amort_num / amort_life
        asset = asset_num / amort_life
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
    Looks for patterns like "26,196 employees" or "approximately 36,000 employees"
    or "workforce of 16,068".
    """
    try:
        with open(html_path, "r", errors="replace") as f:
            html = f.read()
    except FileNotFoundError:
        return None

    # Remove iXBRL hidden metadata block (contains plan names like
    # "A2012EmployeeStockPurchasePlanMember" that produce false matches)
    html = re.sub(r'<ix:hidden>.*?</ix:hidden>', ' ', html, flags=re.DOTALL)

    # Strip HTML tags for cleaner text matching
    text = re.sub(r'<[^>]+>', ' ', html)
    # Decode HTML entities (e.g., &#160; -> actual non-breaking space)
    text = html_module.unescape(text)
    # Normalize all whitespace including non-breaking spaces (\xa0)
    text = re.sub(r'[\s\xa0]+', ' ', text)

    # Find all "N employees" or "N full-time employees" or "workforce of N" patterns
    patterns = [
        r'([\d,]+)\s+(?:full-time\s+)?employees',
        r'employed\s+approximately\s+([\d,]+)',
        r'workforce\s+of\s+([\d,]+)',
        r'headcount\s+(?:of|was|increased\s+to)\s+([\d,]+)',
    ]
    matches = []
    for pattern in patterns:
        matches.extend(re.findall(pattern, text, re.IGNORECASE))
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
    # Default: use the last 20 extracted quarters of R&D expense.
    # The 20-quarter amortization schedule is fully covered by actual data.
    # Company scripts can override via fix_rd_series(rd, records) to handle
    # bad/missing quarters (e.g., Dell VMware spin, Palantir missing IPO quarter).
    print("Computing R&D capitalization...")
    company_module = None
    try:
        from importlib import import_module
        company_module = import_module(f"companies.{ticker.lower()}")
    except ImportError:
        pass

    # Use last 20 quarters for R&D
    rd_records = records[-20:] if len(records) > 20 else records
    quarterly_rd = [r.get("rd_expense_q") for r in rd_records]

    if all(v is not None for v in quarterly_rd):
        # Allow company module to fix bad R&D values (prepend, replace, etc.)
        if company_module and hasattr(company_module, "fix_rd_series"):
            quarterly_rd = company_module.fix_rd_series(quarterly_rd, rd_records)

        rd_results = compute_rd_capitalization(quarterly_rd)

        # Assign results: if fix_rd_series prepended quarters, skip those
        offset = len(rd_results) - len(rd_records)
        for i, record in enumerate(rd_records):
            result = rd_results[offset + i]
            record.update(result)
            print(f"  FY{record['fiscal_year']} {record['fiscal_period']}: "
                  f"amort={result['rd_amortization_q']:>15,}  "
                  f"asset={result['rd_asset_q']:>15,}  "
                  f"OI_adj={result['rd_OI_adjustment_q']:>13,}")
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
