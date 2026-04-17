"""Universal Stage 2 audit CSV.

Reads `ai_extract/{TICKER}/test/formula_mapped_v3.json` and renders a
spreadsheet that an analyst can read like a financial statement, with:

  INCOME STATEMENT
    As-reported rows (one per xbrl_concept, merged labels across filings)
    Normalized detail buckets (AI-assigned, canonical names)
    Normalized subtotals (face-authoritative with recon_delta)
    As-reported formula ties per quarter

  BALANCE SHEET   (same sub-sections)
  CASH FLOW       (same sub-sections)
  SEGMENTS        (per axis, sum-to-consolidated tie)
  VERIFICATION    (failures + warnings)

Columns are one per quarter (chronological by period_end). No hardcoded
row labels — every row comes from the data.

Usage:
    python3 ai_extract/export_full_check_csv.py --ticker NVDA [--test]
"""
import argparse
import csv
import json
import os
import sys
from datetime import datetime


STATEMENT_ORDER = [
    ('income_statement', 'INCOME STATEMENT'),
    ('balance_sheet', 'BALANCE SHEET'),
    ('cash_flow', 'CASH FLOW'),
]


def fmt_num(v):
    """Render a number in millions, rounded, with comma separators. Empty string
    for None. Preserves sign.
    """
    if v is None:
        return ''
    if isinstance(v, bool):
        return 'TRUE' if v else 'FALSE'
    if not isinstance(v, (int, float)):
        return str(v)
    # Stage 2 stores in USD_millions for monetary items; ratios/shares smaller.
    # Round to integer if ≥ 1, else keep 4 decimals.
    if abs(v) >= 1:
        return f'{round(v):,}'
    return f'{v:.4f}'


def sort_quarters(quarters):
    """Sort quarter labels chronologically. Format: {PREFIX}{YY}Q{N} e.g. FY24Q1."""
    def key(q):
        # Best-effort sort key
        if len(q) >= 6 and 'Q' in q:
            prefix = q[:2]
            try:
                yy = int(q[2:4])
                qn = int(q[-1])
                return (prefix, yy, qn)
            except ValueError:
                pass
        return ('ZZ', 99, 99)
    return sorted(quarters, key=key)


def collect_quarter_labels(result):
    """Union of all quarter labels present anywhere in the result."""
    qs = set()
    for stmt_data in result.get('statements', {}).values():
        for r in stmt_data.get('rows', []):
            qs.update((r.get('values_by_quarter') or {}).keys())
        for entry in stmt_data.get('normalized', {}).get('detail', []):
            qs.update((entry.get('values_by_quarter') or {}).keys())
        for entry in stmt_data.get('normalized', {}).get('subtotals', []):
            qs.update((entry.get('values_by_quarter') or {}).keys())
    return sort_quarters(qs)


def write_header_row(writer, quarters):
    writer.writerow(['xbrl_concept / bucket', 'label'] + quarters)


def write_section_header(writer, title, n_cols):
    writer.writerow([title] + [''] * (n_cols - 1))


def write_blank(writer, n_cols):
    writer.writerow([''] * n_cols)


def _flatten_row_values(rec):
    """Unwrap {face, note, total} dict to single value. Prefer `total`; fall
    back to `face`.
    """
    if isinstance(rec, dict):
        if 'total' in rec and rec['total'] is not None:
            return rec['total']
        if 'face' in rec and rec['face'] is not None:
            return rec['face']
        return None
    return rec


def render_statement(writer, stmt_name, stmt_data, quarters, title):
    n_cols = 2 + len(quarters)
    write_section_header(writer, f'── {title} (USD millions) ──', n_cols)
    writer.writerow([''] * n_cols)

    # --- As-reported rows ---
    write_section_header(writer, '  AS-REPORTED ROWS (from Stage 1, merged by xbrl_concept)', n_cols)
    write_header_row(writer, quarters)
    rows = stmt_data.get('rows', [])
    # Sort: face first (is_note_detail=False), then note; within each by concept
    rows_sorted = sorted(rows, key=lambda r: (r.get('is_note_detail', False),
                                              r.get('indent_level', 0),
                                              r.get('xbrl_concept', '')))
    last_was_face = False
    for r in rows_sorted:
        is_note = r.get('is_note_detail', False)
        if is_note and last_was_face:
            write_blank(writer, n_cols)
            writer.writerow(['', '  — Note detail (xbrl_not_on_statement) —'] + [''] * len(quarters))
        last_was_face = not is_note
        labels = r.get('labels') or []
        label = ' | '.join(labels) if isinstance(labels, list) else str(labels)
        values = r.get('values_by_quarter', {}) or {}
        row_out = [r.get('xbrl_concept', ''), label]
        for q in quarters:
            row_out.append(fmt_num(values.get(q)))
        writer.writerow(row_out)

    write_blank(writer, n_cols)

    # --- Normalized detail buckets ---
    normalized = stmt_data.get('normalized', {}) or {}
    detail = normalized.get('detail', []) or []
    write_section_header(writer, '  NORMALIZED DETAIL BUCKETS', n_cols)
    write_header_row(writer, quarters)
    for entry in detail:
        bucket = entry.get('bucket')
        sources = entry.get('sources', []) or []
        src_summary = ', '.join(
            (s.get('concept') if isinstance(s, dict) else s) for s in sources
        ) or '(not assigned)'
        vals = entry.get('values_by_quarter', {}) or {}
        row_out = [bucket, f'← {src_summary}']
        for q in quarters:
            row_out.append(fmt_num(_flatten_row_values(vals.get(q))))
        writer.writerow(row_out)

    write_blank(writer, n_cols)

    # --- Normalized subtotals with recon receipts ---
    subtotals = normalized.get('subtotals', []) or []
    write_section_header(writer, '  NORMALIZED SUBTOTALS (face-authoritative; recon_delta = components − face)', n_cols)
    write_header_row(writer, quarters)
    for entry in subtotals:
        bucket = entry.get('bucket')
        vals = entry.get('values_by_quarter', {}) or {}
        source = entry.get('source', {}) or {}
        # Primary row: subtotal value per quarter
        row_out = [bucket, '(value)']
        for q in quarters:
            v = vals.get(q)
            row_out.append(fmt_num(v))
        writer.writerow(row_out)
        # Secondary row: source (face vs components)
        src_row = ['', '(source)']
        for q in quarters:
            src_row.append(source.get(q, ''))
        writer.writerow(src_row)
        # Tertiary row: recon_delta (component sum − face)
        recon = entry.get('recon_delta', {}) or {}
        if any(recon.values()):
            recon_row = ['', '(recon_delta: components − face)']
            for q in quarters:
                d = (recon.get(q) or {}).get('delta')
                recon_row.append(fmt_num(d))
            writer.writerow(recon_row)

    write_blank(writer, n_cols)

    # --- As-reported formula ties (from Stage 1 formulas, scoped) ---
    formulas = stmt_data.get('formulas', []) or []
    if formulas:
        write_section_header(writer, '  AS-REPORTED FORMULA TIES (Stage 1 declared formulas, scoped per filing)', n_cols)
        write_header_row(writer, quarters)
        for f in formulas:
            result_concept = f.get('result_concept', '')
            comp_str = ' '.join(
                f"{c['sign']}{c['concept']}" for c in (f.get('components') or [])
            )
            expr = f'{result_concept} = {comp_str}'
            ties = f.get('ties_by_quarter', {}) or {}
            if not ties:
                continue
            # Header-ish row naming the formula
            writer.writerow(['  formula', expr] + [''] * len(quarters))
            # Row per tie outcome per quarter
            tie_row = ['', '  tie check']
            for q in quarters:
                rec = ties.get(q)
                if rec is None:
                    tie_row.append('')
                elif rec.get('ties'):
                    tie_row.append('OK')
                else:
                    tie_row.append(f"Δ={fmt_num(rec.get('delta'))}")
            writer.writerow(tie_row)
        write_blank(writer, n_cols)


def render_segments(writer, segments, segment_classifications, quarters):
    n_cols = 2 + len(quarters)
    write_section_header(writer, '── SEGMENTS ──', n_cols)
    write_blank(writer, n_cols)
    axes = segments.get('axes', []) or []
    class_by_dim = {c.get('dimension'): c for c in (segment_classifications or [])}
    for axis in axes:
        dim = axis.get('dimension', '?')
        cls = class_by_dim.get(dim, {})
        axis_type = cls.get('axis_type', '(unclassified)')
        leaf = set(cls.get('leaf_members') or [])
        write_section_header(writer, f'  axis: {dim}  [type: {axis_type}]', n_cols)

        # Columns: quarter|metric combos that appear
        keys = set()
        for r in axis.get('rows', []):
            keys.update((r.get('values_by_quarter_and_metric') or {}).keys())
        keys.update(axis.get('consolidated_by_quarter_and_metric', {}).keys())
        # Sort keys by quarter then metric
        sorted_keys = sorted(keys, key=lambda k: (sort_quarters([k.split('|', 1)[0]])[0] if '|' in k else k, k))
        if not sorted_keys:
            writer.writerow(['', '(no data)'] + [''] * len(quarters))
            write_blank(writer, n_cols)
            continue

        writer.writerow(['member', 'type'] + sorted_keys)
        # Member rows
        for r in axis.get('rows', []):
            member = r.get('member', '')
            mtype = 'leaf' if (not leaf or member in leaf) else 'rollup'
            vals = r.get('values_by_quarter_and_metric', {}) or {}
            row_out = [member, mtype]
            for k in sorted_keys:
                row_out.append(fmt_num(vals.get(k)))
            writer.writerow(row_out)
        # Consolidated row
        cons_row = ['(consolidated)', '']
        for k in sorted_keys:
            cons_row.append(fmt_num(axis.get('consolidated_by_quarter_and_metric', {}).get(k)))
        writer.writerow(cons_row)
        write_blank(writer, n_cols)


def render_verification(writer, result, n_cols):
    write_section_header(writer, '── VERIFICATION ──', n_cols)
    v = result.get('verification', {}) or {}
    passed = v.get('passed', False)
    failures = v.get('failures', []) or []
    writer.writerow(['passed', 'TRUE' if passed else 'FALSE'] + [''] * (n_cols - 2))
    writer.writerow(['failure_count', str(len(failures))] + [''] * (n_cols - 2))
    writer.writerow(['retries', str(v.get('retries', 0))] + [''] * (n_cols - 2))
    write_blank(writer, n_cols)
    if failures:
        writer.writerow(['type', 'message'] + [''] * (n_cols - 2))
        for f in failures:
            writer.writerow([f.get('type', ''), f.get('message', '')] + [''] * (n_cols - 2))
    write_blank(writer, n_cols)


def render_bucket_assignments(writer, result, n_cols):
    """Audit list: every xbrl_concept the AI mapped to a bucket (and every
    excluded concept with its reason). Lets you eyeball whether the AI's
    judgment looks right.
    """
    write_section_header(writer, '── AI BUCKET ASSIGNMENTS (audit trail) ──', n_cols)
    assignments = result.get('bucket_assignments', {}) or {}
    exclusions = result.get('exclusions', []) or []
    writer.writerow(['statement', 'bucket / exclusion', 'concept(s) / reason'] + [''] * (n_cols - 3))
    for stmt_name in ['income_statement', 'balance_sheet', 'cash_flow']:
        stmt_assign = assignments.get(stmt_name, {}) or {}
        for bucket in sorted(stmt_assign.keys()):
            sources = stmt_assign[bucket] or []
            concepts = ', '.join(
                (s.get('concept') if isinstance(s, dict) else s) for s in sources
            )
            writer.writerow([stmt_name, bucket, concepts] + [''] * (n_cols - 3))
    write_blank(writer, n_cols)
    if exclusions:
        writer.writerow(['— excluded concepts —'] + [''] * (n_cols - 1))
        for e in exclusions:
            writer.writerow([e.get('statement', ''),
                             f"EXCLUDED: {e.get('concept', '')}",
                             e.get('reason', '')] + [''] * (n_cols - 3))
    write_blank(writer, n_cols)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--ticker', required=True)
    parser.add_argument('--test', action='store_true', default=True,
                        help='Read from ai_extract/{ticker}/test/ (default)')
    parser.add_argument('--no-test', dest='test', action='store_false')
    parser.add_argument('--input', help='Override input path to formula_mapped_v3.json')
    parser.add_argument('--output', help='Override output CSV path')
    args = parser.parse_args()

    extract_dir = os.path.join(os.path.dirname(__file__), args.ticker)
    work_dir = os.path.join(extract_dir, 'test') if args.test else extract_dir
    in_path = args.input or os.path.join(work_dir, 'formula_mapped_v3.json')
    out_path = args.output or os.path.join(work_dir, f'{args.ticker.lower()}_full_check.csv')

    if not os.path.isfile(in_path):
        print(f'ERROR: input not found: {in_path}', file=sys.stderr)
        sys.exit(1)

    with open(in_path) as f:
        result = json.load(f)

    quarters = collect_quarter_labels(result)
    if not quarters:
        print('ERROR: no quarter labels found in input', file=sys.stderr)
        sys.exit(1)

    n_cols = 2 + len(quarters)

    with open(out_path, 'w', newline='') as f:
        writer = csv.writer(f)
        # Top metadata
        ticker = result.get('ticker', args.ticker)
        reporting_unit = result.get('reporting_unit', '?')
        v = result.get('verification', {}) or {}
        writer.writerow([f'{ticker} — Stage 2 Audit CSV'] + [''] * (n_cols - 1))
        writer.writerow([f'generated: {datetime.now().isoformat(timespec="seconds")}'] + [''] * (n_cols - 1))
        writer.writerow([f'reporting_unit: {reporting_unit}'] + [''] * (n_cols - 1))
        writer.writerow([f'quarters: {len(quarters)}',
                         f'verification.passed: {v.get("passed")}',
                         f'failures: {len(v.get("failures") or [])}']
                        + [''] * (n_cols - 3))
        write_blank(writer, n_cols)

        # Statements
        statements = result.get('statements', {}) or {}
        for key, title in STATEMENT_ORDER:
            stmt_data = statements.get(key, {}) or {}
            render_statement(writer, key, stmt_data, quarters, title)

        # Segments
        segments = result.get('segments', {}) or {}
        classifications = result.get('segment_classifications', []) or []
        render_segments(writer, segments, classifications, quarters)

        # Audit trail
        render_bucket_assignments(writer, result, n_cols)

        # Verification
        render_verification(writer, result, n_cols)

    print(f'Written to {out_path}')
    # Basic summary
    line_count = sum(1 for _ in open(out_path))
    print(f'  quarters: {len(quarters)}')
    print(f'  lines: {line_count}')


if __name__ == '__main__':
    main()
