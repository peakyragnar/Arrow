"""
Run the full pipeline for a ticker: Stage 1 + Stage 2 + Stage 3 + CSV export.
All output goes to test/ subdirectory.

Usage:
    python3 ai_extract/run_full_pipeline.py --ticker NVDA
"""

import argparse
import csv
import glob
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))


def find_filings(ticker):
    """Map per-filing JSONs to accession numbers."""
    filings_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'filings', ticker)
    extract_dir = os.path.join(os.path.dirname(__file__), ticker)

    # Build report_date -> accession map
    date_to_acc = {}
    for acc in os.listdir(filings_dir):
        meta_path = os.path.join(filings_dir, acc, 'filing_meta.json')
        if os.path.exists(meta_path):
            with open(meta_path) as f:
                meta = json.load(f)
            date_to_acc[meta['report_date']] = (acc, meta['form'])

    # For each per-filing JSON, find the accession
    filings = []
    for fpath in sorted(glob.glob(os.path.join(extract_dir, 'q*_fy*_10*.json'))):
        fname = os.path.basename(fpath)
        with open(fpath) as f:
            data = json.load(f)
        ai = data.get('ai_extraction', {})
        is_items = ai.get('income_statement', {}).get('line_items', [])
        for item in is_items:
            if item.get('xbrl_concept') == 'us-gaap:Revenues':
                periods = list(item.get('values', {}).keys())
                for p in periods:
                    end_date = p.split('_')[1] if '_' in p else p
                    if end_date in date_to_acc:
                        acc, form = date_to_acc[end_date]
                        filings.append({
                            'filename': fname,
                            'accession': acc,
                            'form': form,
                            'report_date': end_date,
                        })
                        break
                break

    return filings


def run_stage1(ticker, filings, test_dir, model='claude-sonnet-4-6'):
    """Run Stage 1 for all filings."""
    total_cost = 0
    for f in filings:
        output_path = os.path.join(test_dir, f['filename'])
        if os.path.exists(output_path):
            print(f"  Stage 1: {f['filename']} already exists, skipping")
            continue

        print(f"\n  Stage 1: {f['filename']} ({f['accession']})...")
        cmd = (f"python3 ai_extract/analyze_statement.py "
               f"--ticker {ticker} --accession {f['accession']} "
               f"--statement all --output {output_path} "
               f"--model {model} --test")
        ret = os.system(cmd)
        if ret != 0:
            print(f"  ERROR: Stage 1 failed for {f['filename']}")
            continue


def run_stage2(ticker, test_dir, model='claude-sonnet-4-6'):
    """Run Stage 2 v2 for all filings in test dir."""
    from ai_formula import map_filing_v2

    filing_paths = sorted(glob.glob(os.path.join(test_dir, 'q*_fy*_10*.json')))
    # Exclude formula files
    filing_paths = [p for p in filing_paths if 'formula' not in os.path.basename(p)]

    all_results = []
    total_in = 0
    total_out = 0

    for fpath in filing_paths:
        fname = os.path.basename(fpath)
        formula_path = os.path.join(test_dir, f'formula_v2_{fname}')
        if os.path.exists(formula_path):
            print(f"  Stage 2: {fname} already exists, loading...")
            with open(formula_path) as f:
                result = json.load(f)
            all_results.append(result)
            continue

        print(f"\n  Stage 2: {fname}...")
        with open(fpath) as f:
            extraction = json.load(f)

        result, in_tok, out_tok = map_filing_v2(extraction, ticker, model)
        total_in += in_tok
        total_out += out_tok

        pe = result.get('period_end', '?')
        form = result.get('form', '?')
        analytical = result.get('analytical', {})
        rev = analytical.get('revenue', 0)
        rev_str = f"{rev/1e9:.1f}B" if rev else "?"
        print(f"    {pe} ({form}): revenue={rev_str}, {len(analytical)} analytical fields")

        with open(formula_path, 'w') as f:
            json.dump(result, f, indent=2)

        all_results.append(result)

    if total_in > 0:
        in_cost = total_in * 3.0 / 1e6
        out_cost = total_out * 15.0 / 1e6
        print(f"\n  Stage 2 total: ${in_cost:.2f} + ${out_cost:.2f} = ${in_cost + out_cost:.2f}")

    return all_results


def run_stage3(all_results, ticker, test_dir):
    """Run Stage 3: quarterly derivation."""
    from ai_formula import derive_quarterly_v2, merge_quarters

    print("\n  Stage 3: Deriving quarterly values...")
    records = derive_quarterly_v2(all_results)
    final = merge_quarters(records, ticker)

    quarterly_path = os.path.join(test_dir, 'quarterly.json')
    with open(quarterly_path, 'w') as f:
        json.dump(final, f, indent=2)
    print(f"  {len(final)} quarterly records saved to {quarterly_path}")

    return final


def export_csv(records, ticker, test_dir):
    """Export quarterly records to CSV."""
    if not records:
        print("  No records to export")
        return

    # Collect all field names across all records
    all_fields = set()
    for r in records:
        all_fields.update(r.keys())

    # Order fields: metadata first, then sorted
    meta_fields = ['ticker', 'period_end', 'period_start']
    analytical_fields = sorted(f for f in all_fields if f not in meta_fields)
    ordered_fields = meta_fields + analytical_fields

    csv_path = os.path.join(test_dir, f'{ticker.lower()}_quarterly.csv')
    with open(csv_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=ordered_fields, extrasaction='ignore')
        writer.writeheader()
        for r in records:
            writer.writerow(r)

    print(f"  CSV exported to {csv_path}")
    print(f"  {len(records)} rows, {len(ordered_fields)} columns")


def main():
    parser = argparse.ArgumentParser(description='Run full pipeline for a ticker')
    parser.add_argument('--ticker', required=True)
    parser.add_argument('--model', default='claude-sonnet-4-6')
    parser.add_argument('--skip-stage1', action='store_true', help='Skip Stage 1, use existing extractions')
    args = parser.parse_args()

    ticker = args.ticker.upper()
    test_dir = os.path.join(os.path.dirname(__file__), ticker, 'test')
    os.makedirs(test_dir, exist_ok=True)

    print(f"{'='*60}")
    print(f"  FULL PIPELINE: {ticker}")
    print(f"  Output: {test_dir}")
    print(f"{'='*60}")

    # Find all filings
    filings = find_filings(ticker)
    print(f"\n  Found {len(filings)} filings")
    for f in filings:
        print(f"    {f['filename']} -> {f['accession']} ({f['form']} {f['report_date']})")

    # Stage 1
    if not args.skip_stage1:
        print(f"\n{'='*60}")
        print(f"  STAGE 1: AI Extraction (~${1.41 * len(filings):.0f} estimated)")
        print(f"{'='*60}")
        run_stage1(ticker, filings, test_dir, args.model)
    else:
        print("\n  Skipping Stage 1, copying existing extractions...")
        extract_dir = os.path.join(os.path.dirname(__file__), ticker)
        for f in filings:
            src = os.path.join(extract_dir, f['filename'])
            dst = os.path.join(test_dir, f['filename'])
            if not os.path.exists(dst) and os.path.exists(src):
                import shutil
                shutil.copy2(src, dst)
                print(f"    Copied {f['filename']}")

    # Stage 2
    print(f"\n{'='*60}")
    print(f"  STAGE 2: Normalization (~${0.16 * len(filings):.0f} estimated)")
    print(f"{'='*60}")
    all_results = run_stage2(ticker, test_dir, args.model)

    # Stage 3
    print(f"\n{'='*60}")
    print(f"  STAGE 3: Quarterly Derivation (no AI)")
    print(f"{'='*60}")
    records = run_stage3(all_results, ticker, test_dir)

    # CSV export
    print(f"\n{'='*60}")
    print(f"  CSV EXPORT")
    print(f"{'='*60}")
    export_csv(records, ticker, test_dir)

    print(f"\n{'='*60}")
    print(f"  DONE")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
