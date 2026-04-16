"""
Run deterministic extraction test across all verified periods.
Compares XBRL-only extraction against AI-verified extractions.

Usage:
    python3 ai_extract/test_all_periods.py --ticker NVDA
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from parse_xbrl import parse_filing


def load_parsed_xbrl(ticker, accession):
    base_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'filings', ticker, accession)
    path = os.path.join(base_dir, 'parsed_xbrl.json')
    with open(path) as f:
        return json.load(f)


def get_statement_concepts(presentation, role_substring):
    """Get ordered list of concepts for a statement from presentation linkbase."""
    for section in presentation:
        if role_substring in section['role']:
            concepts = []
            seen = set()
            children_map = {}
            for entry in section['structure']:
                children_map[entry['parent']] = entry['children']

            def walk(parent, depth=0):
                if parent in children_map:
                    for child in children_map[parent]:
                        if child not in seen and 'Abstract' not in child:
                            seen.add(child)
                            concepts.append({'concept': child, 'depth': depth})
                        walk(child, depth + 1)

            all_children = set()
            for entry in section['structure']:
                for c in entry['children']:
                    all_children.add(c)
            roots = [e['parent'] for e in section['structure'] if e['parent'] not in all_children]
            for root in roots:
                walk(root, 0)
            return concepts
    return []


def get_fact_values(facts, concept, dimensioned=False):
    """Get period->value mappings. Returns values in millions (integers) or per-share."""
    values = {}
    for f in facts:
        if f['concept'] != concept:
            continue
        if f['dimensioned'] != dimensioned:
            continue
        if f['value_numeric'] is None:
            continue

        period = f['period']
        if not period:
            continue

        if period['type'] == 'duration':
            key = f"{period['startDate']}_{period['endDate']}"
        else:
            key = period['date']

        unit = f.get('unit', '')
        if unit and 'USD' in unit and 'shares' not in unit:
            values[key] = round(f['value_numeric'] / 1e6)
        elif unit and 'shares' in unit and 'USD' not in unit:
            values[key] = round(f['value_numeric'] / 1e6)
        elif unit and 'USD' in unit and 'shares' in unit:
            values[key] = round(f['value_numeric'], 2)
        else:
            values[key] = f['value_numeric']

    return values


def extract_statement(parsed, role_substring):
    """Deterministically extract a statement."""
    concepts = get_statement_concepts(parsed['presentation'], role_substring)
    line_items = []
    for item in concepts:
        values = get_fact_values(parsed['facts'], item['concept'])
        if values:
            line_items.append({
                'concept': item['concept'],
                'values': values
            })
    return line_items


def verify_calculations(parsed, facts):
    """Verify ALL calculation relationships. Returns list of results."""
    results = []
    for section in parsed.get('calculations', []):
        for formula in section['formulas']:
            parent = formula['parent']
            parent_values = get_fact_values(facts, parent)

            for period, parent_val in parent_values.items():
                computed = 0
                missing = []
                for child in formula['children']:
                    child_values = get_fact_values(facts, child['concept'])
                    if period in child_values:
                        computed += child['weight'] * child_values[period]
                    else:
                        missing.append(child['concept'].split(':')[-1])

                if missing:
                    results.append({
                        'section': section['role'],
                        'parent': parent,
                        'period': period,
                        'status': 'MISSING',
                        'missing': missing
                    })
                else:
                    diff = abs(parent_val - computed)
                    results.append({
                        'section': section['role'],
                        'parent': parent,
                        'period': period,
                        'status': 'PASS' if diff < 1 else 'FAIL',
                        'expected': parent_val,
                        'computed': round(computed),
                        'diff': round(diff)
                    })
    return results


def compare_statement(det_items, verified_items, current_periods):
    """Compare deterministic vs verified for one statement.

    current_periods: set of period keys to compare (skip comparatives).
    Returns (matches, mismatches_list).
    """
    verified_by_concept = {}
    for item in verified_items:
        concept = item.get('xbrl_concept')
        if concept:
            verified_by_concept[concept] = item

    matches = 0
    mismatches = []

    for item in det_items:
        if item['concept'] in verified_by_concept:
            v = verified_by_concept[item['concept']]
            d_vals = item['values']
            v_vals = v.get('values', {})

            for period in current_periods:
                dv = d_vals.get(period)
                vv = v_vals.get(period)
                if dv is not None and vv is not None:
                    # Allow sign differences and check absolute match
                    if abs(abs(dv) - abs(vv)) <= 1:
                        if dv != vv:
                            mismatches.append({
                                'concept': item['concept'].split(':')[-1],
                                'period': period,
                                'det': dv,
                                'verified': vv,
                                'type': 'SIGN' if abs(dv) == abs(vv) else 'ROUNDING'
                            })
                        else:
                            matches += 1
                    else:
                        mismatches.append({
                            'concept': item['concept'].split(':')[-1],
                            'period': period,
                            'det': dv,
                            'verified': vv,
                            'type': 'VALUE'
                        })

    return matches, mismatches


def find_current_periods(parsed):
    """Determine the current reporting periods from the filing metadata."""
    report_date = parsed.get('report_date', '')
    # Find the most common duration and instant periods
    duration_periods = set()
    instant_periods = set()
    for f in parsed['facts']:
        if f['dimensioned']:
            continue
        p = f.get('period')
        if not p:
            continue
        if p['type'] == 'duration':
            key = f"{p['startDate']}_{p['endDate']}"
            if p['endDate'] == report_date:
                duration_periods.add(key)
        else:
            if p['date'] == report_date:
                instant_periods.add(p['date'])

    return duration_periods | instant_periods


def map_accession_to_verified(ticker):
    """Build mapping from accession -> verified extraction file."""
    extract_dir = os.path.join(os.path.dirname(__file__), ticker)
    filings_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'filings', ticker)
    mapping = {}

    # Build report_date -> accession map
    date_to_acc = {}
    for acc in os.listdir(filings_dir):
        meta_path = os.path.join(filings_dir, acc, 'filing_meta.json')
        if os.path.exists(meta_path):
            with open(meta_path) as f:
                meta = json.load(f)
            date_to_acc[meta['report_date']] = acc

    # Build report_date -> verified file map from the verified filenames
    # q1_fy26_10q.json -> we need to figure out the report date
    # Easier: load each verified file, look at IS values, match to accession by period keys
    for fname in os.listdir(extract_dir):
        if not fname.endswith('.json') or fname in ('mapped.json', 'formula_mapped.json', 'quarterly.json'):
            continue
        fpath = os.path.join(extract_dir, fname)
        with open(fpath) as f:
            data = json.load(f)

        ai = data.get('ai_extraction', {})
        is_items = ai.get('income_statement', {}).get('line_items', [])

        # Get period keys from IS line items
        for item in is_items:
            if item.get('xbrl_concept') == 'us-gaap:Revenues':
                periods = list(item.get('values', {}).keys())
                if periods:
                    # The period with the later end date is the current period
                    # Period format: startDate_endDate
                    for p in periods:
                        if '_' in p:
                            end_date = p.split('_')[1]
                            if end_date in date_to_acc:
                                acc = date_to_acc[end_date]
                                if acc not in mapping:
                                    mapping[acc] = (fname, fpath)
                break

    return mapping


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--ticker', required=True)
    args = parser.parse_args()

    print(f"Mapping accessions to verified extractions...")
    mapping = map_accession_to_verified(args.ticker)
    print(f"  Found {len(mapping)} matched filings\n")

    all_calc_results = []
    all_comparison_results = []

    for acc in sorted(mapping.keys()):
        fname, fpath = mapping[acc]
        parsed = load_parsed_xbrl(args.ticker, acc)
        with open(fpath) as f:
            verified = json.load(f)
        ai = verified.get('ai_extraction', {})

        report_date = parsed['report_date']
        form = parsed['form']
        current_periods = find_current_periods(parsed)

        print(f"{'='*80}")
        print(f"  {fname}  |  {form} {report_date}  |  {acc}")
        print(f"  Current periods: {current_periods}")
        print(f"{'='*80}")

        # Verify calculations
        calc_results = verify_calculations(parsed, parsed['facts'])
        passed = sum(1 for r in calc_results if r['status'] == 'PASS')
        failed = [r for r in calc_results if r['status'] == 'FAIL']
        missing = [r for r in calc_results if r['status'] == 'MISSING']

        print(f"\n  Calculations: {passed} pass, {len(failed)} fail, {len(missing)} missing")
        for r in failed:
            short = r['parent'].split(':')[-1]
            print(f"    FAIL {short} [{r['period']}] expected={r['expected']} computed={r['computed']} diff={r['diff']}")

        all_calc_results.extend(calc_results)

        # Compare statements
        stmt_map = {
            'IS': ('StatementsofIncome', 'income_statement'),
            'BS': ('BalanceSheets', 'balance_sheet'),
            'CF': ('StatementsofCashFlows', 'cash_flow'),
        }

        for label, (role_key, verified_key) in stmt_map.items():
            det_items = extract_statement(parsed, role_key)
            verified_stmt = ai.get(verified_key, {})
            verified_items = verified_stmt.get('line_items', [])

            matches, mismatches = compare_statement(det_items, verified_items, current_periods)

            sign_mismatches = [m for m in mismatches if m['type'] == 'SIGN']
            round_mismatches = [m for m in mismatches if m['type'] == 'ROUNDING']
            value_mismatches = [m for m in mismatches if m['type'] == 'VALUE']

            status = "OK" if not value_mismatches and not round_mismatches else "ISSUES"
            print(f"\n  {label}: {matches} exact, {len(sign_mismatches)} sign, {len(round_mismatches)} rounding, {len(value_mismatches)} value")

            for m in round_mismatches + value_mismatches:
                print(f"    {m['type']:8s} {m['concept']:50s} det={m['det']}  verified={m['verified']}")

            all_comparison_results.extend(mismatches)

        print()

    # Summary
    print(f"\n{'='*80}")
    print(f"  OVERALL SUMMARY ACROSS ALL {len(mapping)} FILINGS")
    print(f"{'='*80}")

    total_calc_pass = sum(1 for r in all_calc_results if r['status'] == 'PASS')
    total_calc_fail = sum(1 for r in all_calc_results if r['status'] == 'FAIL')
    total_calc_missing = sum(1 for r in all_calc_results if r['status'] == 'MISSING')
    print(f"\n  Calculations: {total_calc_pass} pass, {total_calc_fail} fail, {total_calc_missing} missing")

    # Group calc failures by parent concept
    fail_by_concept = {}
    for r in all_calc_results:
        if r['status'] == 'FAIL':
            short = r['parent'].split(':')[-1]
            fail_by_concept.setdefault(short, []).append(r)
    if fail_by_concept:
        print("\n  Calculation failures by concept:")
        for concept, failures in sorted(fail_by_concept.items()):
            diffs = [f['diff'] for f in failures]
            print(f"    {concept}: {len(failures)} failures, diffs={diffs}")

    sign_count = sum(1 for r in all_comparison_results if r['type'] == 'SIGN')
    round_count = sum(1 for r in all_comparison_results if r['type'] == 'ROUNDING')
    value_count = sum(1 for r in all_comparison_results if r['type'] == 'VALUE')
    print(f"\n  Comparison vs verified: {sign_count} sign diffs, {round_count} rounding, {value_count} value mismatches")

    if value_count > 0:
        print("\n  VALUE mismatches (real errors):")
        for r in all_comparison_results:
            if r['type'] == 'VALUE':
                print(f"    {r['concept']:50s} det={r['det']}  verified={r['verified']}  period={r['period']}")

    if round_count > 0:
        print("\n  ROUNDING mismatches:")
        for r in all_comparison_results:
            if r['type'] == 'ROUNDING':
                print(f"    {r['concept']:50s} det={r['det']}  verified={r['verified']}  diff={abs(r['det'])-abs(r['verified'])}")


if __name__ == '__main__':
    main()
