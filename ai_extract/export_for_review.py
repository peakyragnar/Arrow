"""
Export AI extraction to CSV for human review.
Edit the CSV to correct any errors, then use it as golden eval.

Usage:
    python3 ai_extract/export_for_review.py ai_extract/nvda_q1fy26_all_statements.json
"""

import csv
import json
import sys
import os


def export(path):
    with open(path) as f:
        data = json.load(f)

    ai = data['ai_extraction']
    out_path = path.replace('.json', '_review.csv')

    rows = []

    # Determine if this is an "all" extraction or single statement
    if 'income_statement' in ai:
        statements = [
            ('income_statement', 'IS'),
            ('balance_sheet', 'BS'),
            ('cash_flow', 'CF'),
        ]
    else:
        # Single statement - wrap it
        statements = [('_single', '')]
        ai = {'_single': ai}

    # Collect all periods across all statements
    all_periods = []
    for stmt_key, _ in statements:
        stmt = ai.get(stmt_key, {})
        for item in stmt.get('line_items', []):
            for p in item.get('values', {}):
                if p not in all_periods:
                    all_periods.append(p)

    # Build rows
    for stmt_key, stmt_prefix in statements:
        stmt = ai.get(stmt_key, {})

        for item in stmt.get('line_items', []):
            indent = "  " * item.get('indent_level', 0)
            label = f"{indent}{item['label']}"
            vals = item.get('values', {})

            row = {
                'statement': stmt_prefix,
                'label': label,
                'xbrl_concept': item.get('xbrl_concept', ''),
                'unit': item.get('unit', ''),
                'xbrl_match': item.get('xbrl_match', ''),
            }
            for p in all_periods:
                row[p] = vals.get(p, '')

            # Add a "correct" column for each period for the reviewer to fill in
            for p in all_periods:
                row[f'{p}_correct'] = ''

            rows.append(row)

        # Add "not on statement" items
        for item in stmt.get('xbrl_not_on_statement', []):
            val = item.get('value', '')
            period = item.get('period', '')

            # Handle dict values (multi-period)
            if isinstance(val, dict):
                row = {
                    'statement': f'{stmt_prefix}-hidden',
                    'label': f"  [NOT ON STMT] {item.get('concept', '')}",
                    'xbrl_concept': item.get('concept', ''),
                    'unit': 'USD_millions',
                    'xbrl_match': 'n/a',
                }
                for p in all_periods:
                    row[p] = val.get(p, '')
                    row[f'{p}_correct'] = ''
                row['reason'] = item.get('reason', '')
                rows.append(row)
            else:
                row = {
                    'statement': f'{stmt_prefix}-hidden',
                    'label': f"  [NOT ON STMT] {item.get('concept', '')}",
                    'xbrl_concept': item.get('concept', ''),
                    'unit': 'USD_millions',
                    'xbrl_match': 'n/a',
                }
                for p in all_periods:
                    if p == period or period in p:
                        row[p] = val
                    else:
                        row[p] = ''
                    row[f'{p}_correct'] = ''
                row['reason'] = item.get('reason', '')
                rows.append(row)

    # Write CSV
    if not rows:
        print("No data to export")
        return

    fieldnames = ['statement', 'label', 'xbrl_concept', 'unit', 'xbrl_match']
    for p in all_periods:
        fieldnames.append(p)
        fieldnames.append(f'{p}_correct')
    if any('reason' in r for r in rows):
        fieldnames.append('reason')

    with open(out_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)

    print(f"Exported {len(rows)} rows to {out_path}")
    print(f"Review in a spreadsheet. Fill in '{all_periods[0]}_correct' columns where values need correction.")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python3 ai_extract/export_for_review.py <extraction.json>")
        sys.exit(1)
    export(sys.argv[1])
