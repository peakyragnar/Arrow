"""
Test: can we extract financial statements deterministically from XBRL linkbase data?

Reads parsed_xbrl.json (facts + calculations + presentation + definitions),
extracts IS/BS/CF values and verifies all calculation relationships.
Compares output against the verified AI extraction.

Usage:
    python3 ai_extract/test_deterministic.py --ticker NVDA --accession 0001045810-25-000116
"""

import argparse
import json
import os
import sys


def load_parsed_xbrl(ticker, accession):
    """Load the parsed XBRL JSON for a filing."""
    base_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'filings', ticker, accession)
    path = os.path.join(base_dir, 'parsed_xbrl.json')
    with open(path) as f:
        return json.load(f)


def load_verified_extraction(ticker, accession):
    """Load the verified AI extraction JSON for comparison."""
    # Map accession to filename by looking at what exists
    extract_dir = os.path.join(os.path.dirname(__file__), ticker)
    if not os.path.exists(extract_dir):
        return None
    for fname in os.listdir(extract_dir):
        if not fname.endswith('.json') or fname in ('mapped.json', 'formula_mapped.json', 'quarterly.json'):
            continue
        path = os.path.join(extract_dir, fname)
        with open(path) as f:
            data = json.load(f)
        # Check if this file matches our accession
        meta = data.get('ai_extraction', {})
        # Try matching by checking values against our facts
        if 'formula_verification' in data:
            # Load meta from filing_meta.json to match
            base_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'filings', ticker, accession)
            meta_path = os.path.join(base_dir, 'filing_meta.json')
            with open(meta_path) as mf:
                filing_meta = json.load(mf)
            report_date = filing_meta.get('report_date', '')
            # Match by report date in filename
            if report_date.replace('-', '') in fname.replace('-', ''):
                return data
            # Match by fiscal quarter naming
            # q1_fy26_10q.json -> report_date 2025-04-27
            # We'll just try all files and match by IS revenue values

    # Fallback: try all files and match by revenue
    return None


def get_statement_concepts(presentation, role_substring):
    """Get ordered list of concepts for a statement from presentation linkbase."""
    for section in presentation:
        if role_substring in section['role']:
            # Build ordered concept list from parent->children structure
            concepts = []
            seen = set()

            # First, build a full parent->children map
            children_map = {}
            for entry in section['structure']:
                children_map[entry['parent']] = entry['children']

            # Walk the tree depth-first to get presentation order
            def walk(parent, depth=0):
                if parent in children_map:
                    for child in children_map[parent]:
                        if child not in seen and 'Abstract' not in child:
                            seen.add(child)
                            concepts.append({'concept': child, 'depth': depth})
                        walk(child, depth + 1)

            # Find root parents (those that aren't children of anything)
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
    """Get all period->value mappings for a concept from undimensioned facts."""
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

        # Convert to millions (values are in raw units)
        unit = f.get('unit', '')
        if unit and 'USD' in unit and 'shares' not in unit:
            val = f['value_numeric'] / 1e6
            # Round to integer millions
            values[key] = int(round(val))
        elif unit and 'shares' in unit and 'USD' not in unit:
            val = f['value_numeric'] / 1e6
            values[key] = int(round(val))
        elif unit and 'USD' in unit and 'shares' in unit:
            # Per-share values, keep as-is
            values[key] = round(f['value_numeric'], 2)
        else:
            values[key] = f['value_numeric']

    return values


def extract_statement(parsed, role_substring):
    """Deterministically extract a financial statement from parsed XBRL data."""
    concepts = get_statement_concepts(parsed['presentation'], role_substring)

    line_items = []
    for item in concepts:
        concept = item['concept']
        values = get_fact_values(parsed['facts'], concept)
        if values:  # Only include items that have values
            line_items.append({
                'concept': concept,
                'depth': item['depth'],
                'values': values
            })

    return line_items


def verify_calculations(parsed, role_substring, facts):
    """Verify all calculation relationships for a statement section."""
    results = []

    for section in parsed['calculations']:
        if role_substring in section['role']:
            for formula in section['formulas']:
                parent = formula['parent']
                parent_values = get_fact_values(facts, parent)

                if not parent_values:
                    results.append({
                        'formula': parent,
                        'status': 'NO_DATA',
                        'detail': f'No values found for {parent}'
                    })
                    continue

                for period, parent_val in parent_values.items():
                    computed = 0
                    missing = []
                    for child in formula['children']:
                        child_values = get_fact_values(facts, child['concept'])
                        if period in child_values:
                            computed += child['weight'] * child_values[period]
                        else:
                            missing.append(child['concept'])

                    if missing:
                        results.append({
                            'formula': f'{parent} [{period}]',
                            'status': 'MISSING_CHILDREN',
                            'detail': f'Missing: {missing}'
                        })
                    else:
                        diff = abs(parent_val - computed)
                        status = 'PASS' if diff < 1 else 'FAIL'
                        results.append({
                            'formula': f'{parent} [{period}]',
                            'status': status,
                            'expected': parent_val,
                            'computed': int(round(computed)),
                            'diff': int(round(diff))
                        })

    return results


def compare_with_verified(deterministic_items, verified_statement, statement_name):
    """Compare deterministic extraction against verified AI extraction."""
    verified_items = verified_statement.get('line_items', [])

    print(f"\n{'='*80}")
    print(f"  {statement_name}: DETERMINISTIC vs VERIFIED")
    print(f"{'='*80}")
    print(f"  Deterministic items: {len(deterministic_items)}")
    print(f"  Verified items:     {len(verified_items)}")

    # Build lookup by concept
    verified_by_concept = {}
    for item in verified_items:
        concept = item.get('xbrl_concept')
        if concept:
            verified_by_concept[concept] = item

    matches = 0
    mismatches = 0
    missing_in_verified = 0
    missing_in_deterministic = 0

    det_concepts = set()
    for item in deterministic_items:
        det_concepts.add(item['concept'])
        if item['concept'] in verified_by_concept:
            v = verified_by_concept[item['concept']]
            # Compare values
            d_vals = item['values']
            v_vals = v.get('values', {})

            all_match = True
            for period in set(list(d_vals.keys()) + list(v_vals.keys())):
                dv = d_vals.get(period)
                vv = v_vals.get(period)
                if dv is not None and vv is not None:
                    if abs(dv - vv) > 0.01:
                        all_match = False
                        print(f"  MISMATCH {item['concept'].split(':')[-1]:50s} period={period}  det={dv}  verified={vv}")

            if all_match:
                matches += 1
            else:
                mismatches += 1
        else:
            missing_in_verified += 1
            vals = list(item['values'].values())
            print(f"  EXTRA    {item['concept'].split(':')[-1]:50s} values={vals[:2]}")

    for concept, item in verified_by_concept.items():
        if concept not in det_concepts:
            missing_in_deterministic += 1
            vals = list(item.get('values', {}).values())
            print(f"  MISSING  {concept.split(':')[-1] if concept else item['label']:50s} values={vals[:2]}")

    print(f"\n  Summary: {matches} match, {mismatches} mismatch, "
          f"{missing_in_verified} extra in deterministic, {missing_in_deterministic} missing from deterministic")

    return matches, mismatches, missing_in_verified, missing_in_deterministic


def main():
    parser = argparse.ArgumentParser(description='Test deterministic XBRL extraction')
    parser.add_argument('--ticker', required=True)
    parser.add_argument('--accession', required=True)
    parser.add_argument('--verified', help='Path to verified extraction JSON')
    args = parser.parse_args()

    print(f"Loading parsed XBRL for {args.ticker} / {args.accession}...")
    parsed = load_parsed_xbrl(args.ticker, args.accession)
    print(f"  {parsed['total_facts']} facts, {len(parsed.get('calculations', []))} calc sections, "
          f"{len(parsed.get('presentation', []))} pres sections")

    # Extract three statements
    statements = {
        'Income Statement': ('StatementsofIncome', 'income_statement'),
        'Balance Sheet': ('BalanceSheets', 'balance_sheet'),
        'Cash Flow': ('StatementsofCashFlows', 'cash_flow'),
    }

    for name, (role_key, verified_key) in statements.items():
        print(f"\n--- {name} ---")
        items = extract_statement(parsed, role_key)
        print(f"  Extracted {len(items)} line items")
        for item in items:
            short = item['concept'].split(':')[-1]
            vals = list(item['values'].items())[:2]
            print(f"    {'  ' * item['depth']}{short}: {vals}")

    # Verify calculations
    print(f"\n{'='*80}")
    print("  CALCULATION VERIFICATION")
    print(f"{'='*80}")

    all_checks = []
    for section in parsed.get('calculations', []):
        checks = verify_calculations(parsed, section['role'], parsed['facts'])
        for c in checks:
            all_checks.append(c)
            status_icon = {'PASS': 'OK', 'FAIL': 'XX', 'MISSING_CHILDREN': '??', 'NO_DATA': '--'}
            icon = status_icon.get(c['status'], '??')
            if c['status'] == 'PASS':
                print(f"  [{icon}] {c['formula']}")
            elif c['status'] == 'FAIL':
                print(f"  [{icon}] {c['formula']}  expected={c['expected']} computed={c['computed']} diff={c['diff']}")
            else:
                print(f"  [{icon}] {c['formula']}  {c.get('detail', '')}")

    passed = sum(1 for c in all_checks if c['status'] == 'PASS')
    failed = sum(1 for c in all_checks if c['status'] == 'FAIL')
    missing = sum(1 for c in all_checks if c['status'] in ('MISSING_CHILDREN', 'NO_DATA'))
    print(f"\n  Total: {passed} passed, {failed} failed, {missing} missing data")

    # Compare with verified extraction
    if args.verified:
        with open(args.verified) as f:
            verified = json.load(f)
        ai = verified.get('ai_extraction', verified)

        total_match = 0
        total_mismatch = 0
        total_extra = 0
        total_missing = 0

        for name, (role_key, verified_key) in statements.items():
            items = extract_statement(parsed, role_key)
            if verified_key in ai:
                m, mm, e, mi = compare_with_verified(items, ai[verified_key], name)
                total_match += m
                total_mismatch += mm
                total_extra += e
                total_missing += mi

        print(f"\n{'='*80}")
        print(f"  OVERALL: {total_match} match, {total_mismatch} mismatch, "
              f"{total_extra} extra, {total_missing} missing")
        print(f"{'='*80}")

    # Extract segment data from dimensioned facts
    print(f"\n--- Segment Data ---")
    dim_facts = [f for f in parsed['facts'] if f['dimensioned']]

    # Group by dimension axis
    by_axis = {}
    for f in dim_facts:
        if f['value_numeric'] is None:
            continue
        for d in (f.get('dimensions') or []):
            axis = d['dimension']
            member = d['member']
            by_axis.setdefault(axis, {}).setdefault(member, []).append(f)

    for axis, members in sorted(by_axis.items()):
        # Only show revenue-related segment data
        rev_members = {}
        for member, facts in members.items():
            for f in facts:
                if 'Revenue' in f['concept'] or 'Revenues' in f['concept']:
                    short_member = member.split(':')[-1]
                    vals = get_fact_values([f], f['concept'], dimensioned=True)
                    if vals:
                        rev_members[short_member] = vals

        if rev_members:
            short_axis = axis.split(':')[-1]
            print(f"\n  {short_axis}:")
            for member, vals in sorted(rev_members.items()):
                print(f"    {member}: {vals}")


if __name__ == '__main__':
    main()
