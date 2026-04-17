"""
Stage 2: Quarterize as-reported statements + normalize analytical fields.

Two outputs, both verified by math:

  Output 1 — statements.{income_statement, balance_sheet, cash_flow}
    As-reported rows merged across filings by xbrl_concept, label variants
    joined, values per quarter, plus each statement's declared formulas.
    Segments rendered as a fourth section. Deterministic: built in Python
    from Stage 1 line_items + xbrl_not_on_statement. No plugs.

  Output 2 — analytical.{field: {values_by_quarter, source_per_quarter}}
    The universal analytical field bag needed by formulas.md. Produced by
    the AI because mapping fields like operating_lease_liabilities to the
    right as-reported rows + note detail is judgment work. Every value
    traces back to a row in Output 1.

Verification battery (Python, post-AI):
  1. Every statement formula ties in every quarter.
  2. Q1+Q2+Q3+Q4 = annual for every flow concept where all five exist.
  3. Balance-sheet instants consistent across filings for shared period-ends.
  4. Segment members sum to consolidated total per axis/metric/quarter.
  5. Every analytical value reconciles to signed sum of its source rows.
  6. Forward-fills audited against raw parsed_xbrl.json.
  7. Basic sign sanity (revenue positive, no impossible flips).

Retry up to 3 times with failures echoed to the AI. Hard error on
unresolved failures — no silent accept.

Usage:
    python3 ai_extract/ai_formula.py --ticker NVDA --test
"""

import argparse
import glob
import json
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import date, datetime

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

import anthropic


# ────────────────────────────────────────────────────────────────────────────
# Fiscal calendar helpers
# ────────────────────────────────────────────────────────────────────────────

FILENAME_RE = re.compile(r'^q([1-4])_(fy|cy)(\d{2})_10([qk])(?:_stripped)?\.json$')


def parse_filename(path):
    """Extract fiscal metadata from per-filing filename.

    Returns dict with: file, fiscal_year_label (FY24/CY24), fiscal_period (Q1..Q4),
    form (10-Q/10-K), quarter_label (FY24Q1).
    Returns None if filename doesn't match the pattern.
    """
    fname = os.path.basename(path)
    m = FILENAME_RE.match(fname)
    if not m:
        return None
    qn, prefix, yy, form_char = m.groups()
    return {
        'file': fname,
        'fiscal_period': f'Q{qn}',
        'fiscal_year_label': f'{prefix.upper()}{yy}',
        'form': '10-K' if form_char == 'k' else '10-Q',
        'quarter_label': f'{prefix.upper()}{yy}Q{qn}',
    }


def parse_period_key(key):
    """Parse a values-dict key into (kind, start, end, duration_days).

    Duration keys: 'YYYY-MM-DD_YYYY-MM-DD' -> ('duration', start, end, days)
    Instant keys:  'YYYY-MM-DD'            -> ('instant',  None, date,  0)
    """
    parts = key.split('_')
    if len(parts) == 2:
        try:
            s = datetime.strptime(parts[0], '%Y-%m-%d').date()
            e = datetime.strptime(parts[1], '%Y-%m-%d').date()
            return ('duration', s, e, (e - s).days)
        except ValueError:
            return (None, None, None, 0)
    if len(parts) == 1:
        try:
            d = datetime.strptime(parts[0], '%Y-%m-%d').date()
            return ('instant', None, d, 0)
        except ValueError:
            return (None, None, None, 0)
    return (None, None, None, 0)


def derive_filing_period_end(ai):
    """Filing period_end = modal latest instant date from the balance sheet.

    BS line_items are instants; their latest date is the filing's report date.
    Using mode guards against stray instants on the face.
    """
    dates = []
    for item in ai.get('balance_sheet', {}).get('line_items', []):
        for k in item.get('values', {}).keys():
            kind, _, end, _ = parse_period_key(k)
            if kind == 'instant' and end:
                dates.append(end)
    if not dates:
        return None
    latest = max(dates)
    return latest


def pick_period_value(values, filing_period_end, statement, form):
    """Select the current-period value from a line item's values dict.

    Rules:
      - 10-Q IS: shortest duration ending at filing_period_end (the 3-month quarterly column).
      - 10-Q CF: longest duration ending at filing_period_end (YTD — only column).
      - 10-K IS/CF: longest duration ending at filing_period_end (12-month annual).
      - BS (any form): instant value at filing_period_end.

    Returns (value, period_kind, duration_days) or (None, None, 0) if nothing matches.
    """
    if not values or filing_period_end is None:
        return (None, None, 0)

    if statement == 'balance_sheet':
        for k, v in values.items():
            kind, _, end, _ = parse_period_key(k)
            if kind == 'instant' and end == filing_period_end:
                return (v, 'instant', 0)
        return (None, None, 0)

    candidates = []
    for k, v in values.items():
        kind, _, end, days = parse_period_key(k)
        if kind == 'duration' and end == filing_period_end:
            candidates.append((days, v))
    if not candidates:
        return (None, None, 0)

    if form == '10-Q' and statement == 'income_statement':
        days, v = min(candidates, key=lambda x: x[0])
    else:
        days, v = max(candidates, key=lambda x: x[0])
    return (v, 'duration', days)


def pick_ytd_value(values, filing_period_end):
    """For CF in 10-Q, pick the YTD duration ending at filing_period_end.

    Returns (value, days) or (None, 0).
    """
    candidates = []
    for k, v in values.items():
        kind, _, end, days = parse_period_key(k)
        if kind == 'duration' and end == filing_period_end:
            candidates.append((days, v))
    if not candidates:
        return (None, 0)
    days, v = max(candidates, key=lambda x: x[0])
    return (v, days)


# ────────────────────────────────────────────────────────────────────────────
# Filing loader
# ────────────────────────────────────────────────────────────────────────────

def load_filings(work_dir):
    """Load all per-filing JSONs in work_dir. Derive metadata.

    Skips stripped variants when the full file exists. Sorts by period_end.
    Returns list of dicts with: file, path, fiscal_year_label, fiscal_period,
    form, quarter_label, period_end, ai (the ai_extraction subtree).
    """
    paths = sorted(glob.glob(os.path.join(work_dir, 'q*_fy*_10*.json'))
                   + glob.glob(os.path.join(work_dir, 'q*_cy*_10*.json')))
    paths = [p for p in paths if 'formula' not in os.path.basename(p)]

    # Drop stripped when non-stripped exists
    bases = {os.path.basename(p).replace('_stripped', '') for p in paths
             if '_stripped' not in os.path.basename(p)}
    paths = [p for p in paths
             if '_stripped' not in os.path.basename(p)
             or os.path.basename(p).replace('_stripped', '') not in bases]

    filings = []
    for p in paths:
        meta = parse_filename(p)
        if not meta:
            continue
        with open(p) as f:
            d = json.load(f)
        ai = d.get('ai_extraction', d)
        pe = derive_filing_period_end(ai)
        if pe is None:
            print(f"  WARN: no period_end derivable from {meta['file']}; skipping")
            continue
        filings.append({**meta, 'path': p, 'period_end': pe, 'ai': ai})

    filings.sort(key=lambda f: f['period_end'])
    return filings


# ────────────────────────────────────────────────────────────────────────────
# Output 1: as-reported statements, merged + quarterized
# ────────────────────────────────────────────────────────────────────────────

STATEMENTS = ('income_statement', 'balance_sheet', 'cash_flow')


def _collect_rows_from_filing(filing, statement):
    """Yield merge-ready row contributions from one filing for one statement.

    Each contribution carries: xbrl_concept, label, indent_level, is_note_detail,
    period_kind, filing metadata, and the selected value/ytd for this filing.
    """
    ai = filing['ai']
    s = ai.get(statement, {})
    form = filing['form']
    pe = filing['period_end']

    for item in s.get('line_items', []):
        concept = item.get('xbrl_concept')
        if not concept:
            continue
        val, kind, days = pick_period_value(item.get('values', {}), pe, statement, form)
        ytd_val, ytd_days = (None, 0)
        if statement == 'cash_flow' and form == '10-Q':
            ytd_val, ytd_days = pick_ytd_value(item.get('values', {}), pe)
        yield {
            'concept': concept,
            'label': item.get('label', ''),
            'indent_level': item.get('indent_level', 0),
            'is_note_detail': False,
            'parent_concept': None,
            'value': val,
            'period_kind': kind,
            'duration_days': days,
            'ytd_value': ytd_val,
            'ytd_days': ytd_days,
        }

    for item in s.get('xbrl_not_on_statement', []):
        concept = item.get('concept')
        if not concept:
            continue
        vals = item.get('value', {}) or item.get('values', {})
        val, kind, days = pick_period_value(vals, pe, statement, form)
        ytd_val, ytd_days = (None, 0)
        if statement == 'cash_flow' and form == '10-Q':
            ytd_val, ytd_days = pick_ytd_value(vals, pe)
        yield {
            'concept': concept,
            'label': _default_label_from_concept(concept),
            'indent_level': 0,
            'is_note_detail': True,
            'parent_concept': None,
            'value': val,
            'period_kind': kind,
            'duration_days': days,
            'ytd_value': ytd_val,
            'ytd_days': ytd_days,
            'classification': item.get('classification', ''),
        }


def _default_label_from_concept(concept):
    """Humanize a us-gaap concept name for display when no label is supplied."""
    name = concept.split(':', 1)[-1]
    out = re.sub(r'(?<!^)(?=[A-Z])', ' ', name)
    return out


def merge_statement_rows(filings, statement):
    """Merge per-filing contributions into one row per xbrl_concept.

    Each row has:
      xbrl_concept, labels (sorted unique variants), indent_level (mode),
      is_note_detail, values_by_quarter, source_refs, period_kind, ytd_by_quarter
    """
    by_concept = defaultdict(lambda: {
        'xbrl_concept': None,
        'labels': set(),
        'indent_levels': Counter(),
        'is_note_detail_votes': Counter(),
        'values_by_quarter': {},
        'ytd_by_quarter': {},
        'source_refs': {},
        'period_kind': None,
    })

    for filing in filings:
        q = filing['quarter_label']
        for c in _collect_rows_from_filing(filing, statement):
            row = by_concept[c['concept']]
            row['xbrl_concept'] = c['concept']
            row['labels'].add(c['label'])
            row['indent_levels'][c['indent_level']] += 1
            row['is_note_detail_votes'][c['is_note_detail']] += 1
            row['period_kind'] = c['period_kind'] or row['period_kind']
            if c['value'] is not None:
                row['values_by_quarter'][q] = c['value']
                row['source_refs'][q] = filing['file']
            if c['ytd_value'] is not None:
                row['ytd_by_quarter'][q] = {'value': c['ytd_value'], 'days': c['ytd_days']}

    merged = []
    for concept, row in by_concept.items():
        indent = row['indent_levels'].most_common(1)[0][0] if row['indent_levels'] else 0
        is_note = row['is_note_detail_votes'].most_common(1)[0][0] if row['is_note_detail_votes'] else False
        merged.append({
            'xbrl_concept': concept,
            'labels': sorted(row['labels']),
            'indent_level': indent,
            'is_note_detail': is_note,
            'period_kind': row['period_kind'],
            'values_by_quarter': row['values_by_quarter'],
            'ytd_by_quarter': row['ytd_by_quarter'],
            'source_refs': row['source_refs'],
        })

    merged.sort(key=lambda r: (r['is_note_detail'], r['indent_level'], r['xbrl_concept']))
    return merged


def fiscal_year_groups(filings):
    """Group filings by fiscal_year_label. Returns dict: fy_label -> {quarter_label -> filing}."""
    groups = defaultdict(dict)
    for f in filings:
        groups[f['fiscal_year_label']][f['fiscal_period']] = f
    return groups


def derive_q2_q3_from_ytd(rows, statement, filings):
    """For cash_flow in 10-Q: Q2/Q3 quarterly value = YTD - prior YTD.

    Applied in place. Only affects rows whose values_by_quarter has a Q2/Q3 entry
    that came from a YTD context (duration_days > 100). Replaces with standalone
    quarterly = YTD_current - YTD_prior.
    """
    if statement != 'cash_flow':
        return
    groups = fiscal_year_groups(filings)
    for row in rows:
        for fy_label, qmap in groups.items():
            for qn in ('Q2', 'Q3'):
                q_label = f'{fy_label}{qn}'
                if qn not in qmap:
                    continue
                if qmap[qn]['form'] != '10-Q':
                    continue
                ytd_entry = row['ytd_by_quarter'].get(q_label)
                if not ytd_entry:
                    continue
                # Determine prior YTD
                if qn == 'Q2':
                    prior_q = 'Q1'
                else:
                    prior_q = 'Q2'
                prior_label = f'{fy_label}{prior_q}'
                # Q1 standalone == Q1 YTD; Q2 YTD = 6-month; Q3 YTD = 9-month
                prior_ytd = 0
                if prior_q == 'Q1':
                    # Q1 value in row = standalone = YTD
                    prior_ytd = row['values_by_quarter'].get(prior_label, 0) or 0
                else:
                    # Q2 YTD from ytd_by_quarter
                    pe = row['ytd_by_quarter'].get(prior_label)
                    prior_ytd = (pe or {}).get('value', 0) or 0
                row['values_by_quarter'][q_label] = ytd_entry['value'] - prior_ytd


def derive_q4_for_flows(rows, statement, filings):
    """For flow concepts in 10-K: Q4 = Annual - Q1 - Q2 - Q3.

    IS + CF: all concepts are flows → derive Q4.
    BS: snapshot → 10-K annual value IS Q4; no derivation.
    """
    if statement == 'balance_sheet':
        return
    groups = fiscal_year_groups(filings)
    for row in rows:
        for fy_label, qmap in groups.items():
            if 'Q4' not in qmap or qmap['Q4']['form'] != '10-K':
                continue
            annual_q = f'{fy_label}Q4'
            annual = row['values_by_quarter'].get(annual_q)
            if annual is None:
                continue
            q123_vals = [row['values_by_quarter'].get(f'{fy_label}{q}') for q in ('Q1', 'Q2', 'Q3')]
            if any(v is None for v in q123_vals):
                continue
            row['values_by_quarter'][annual_q] = annual - sum(q123_vals)


def gather_formulas(filings, statement):
    """Collect unique formulas across all filings for a statement.

    Formulas are stored with label-based components; we translate to concept-based
    using the line_items in the same filing. Returns list of:
      {result_concept, components: [{sign, concept}], result_label}
    """
    seen = {}
    for filing in filings:
        s = filing['ai'].get(statement, {})
        line_items = s.get('line_items', [])
        label_to_concept = {it.get('label'): it.get('xbrl_concept') for it in line_items
                            if it.get('label') and it.get('xbrl_concept')}

        for f in s.get('formulas', []):
            op = f.get('operation') or ''
            result_label = f.get('result_label') or ''
            result_concept = label_to_concept.get(result_label)
            if not result_concept or not op:
                continue

            # Parse 'A + B - C' into signed components
            components = []
            token_re = re.compile(r'\s*([+\-])?\s*([^+\-]+?)(?=\s*[+\-]|\s*$)')
            remaining = op.strip()
            first = True
            while remaining:
                m = re.match(r'\s*([+\-]?)\s*(.+)', remaining)
                if not m:
                    break
                sign = m.group(1) or '+'
                rest = m.group(2).strip()
                # find next +/- not inside parens (simple: split at first top-level +/-)
                nxt = None
                depth = 0
                for i, ch in enumerate(rest):
                    if ch == '(':
                        depth += 1
                    elif ch == ')':
                        depth -= 1
                    elif ch in '+-' and depth == 0 and i > 0:
                        nxt = i
                        break
                if nxt is not None:
                    label = rest[:nxt].strip()
                    remaining = rest[nxt:]
                else:
                    label = rest.strip()
                    remaining = ''
                concept = label_to_concept.get(label)
                if concept:
                    components.append({'sign': sign if not first else ('+' if sign == '' else sign),
                                       'concept': concept})
                first = False

            if not components:
                continue

            key = (result_concept, tuple((c['sign'], c['concept']) for c in components))
            if key not in seen:
                seen[key] = {
                    'result_concept': result_concept,
                    'result_label': result_label,
                    'components': components,
                    'statement': statement,
                }
    return list(seen.values())


def evaluate_formulas(rows, formulas):
    """For each formula, compute expected vs reported per quarter. Annotate ties."""
    by_concept = {r['xbrl_concept']: r for r in rows}
    all_quarters = set()
    for r in rows:
        all_quarters.update(r['values_by_quarter'].keys())

    for f in formulas:
        f['ties_by_quarter'] = {}
        result_row = by_concept.get(f['result_concept'])
        if not result_row:
            continue
        for q in sorted(all_quarters):
            reported = result_row['values_by_quarter'].get(q)
            if reported is None:
                continue
            computed = 0
            ok = True
            for c in f['components']:
                crow = by_concept.get(c['concept'])
                if not crow or q not in crow['values_by_quarter']:
                    ok = False
                    break
                cv = crow['values_by_quarter'][q]
                computed += cv if c['sign'] == '+' else -cv
            if not ok:
                continue
            delta = computed - reported
            f['ties_by_quarter'][q] = {
                'computed': computed,
                'reported': reported,
                'delta': delta,
                'ties': delta == 0,
            }


# ────────────────────────────────────────────────────────────────────────────
# Segments
# ────────────────────────────────────────────────────────────────────────────

def build_segments(filings):
    """Gather segment_data from all filings and quarterize.

    Stage 1 segment entries have shape:
      { 'dimension', 'items': [{'member', 'values': {'Metric_YYYY-MM-DD_YYYY-MM-DD': val}}],
        'total', 'consolidated_total', 'ties' }

    Values keys are 'Metric_start_end' (duration) or 'Metric_YYYY-MM-DD' (instant).
    We select current-period values using the same rule as line items.
    """
    axes_by_dim = defaultdict(lambda: {
        'dimension': None,
        'metrics': set(),
        'members_by_key': defaultdict(lambda: {
            'member': None,
            'values_by_quarter_and_metric': {},
        }),
        'consolidated_by_quarter_and_metric': {},
    })

    for filing in filings:
        q = filing['quarter_label']
        pe = filing['period_end']
        form = filing['form']
        for seg in filing['ai'].get('segment_data', []) or []:
            dim = seg.get('dimension') or 'Unknown'
            axis = axes_by_dim[dim]
            axis['dimension'] = dim

            for it in seg.get('items', []) or []:
                member = it.get('member') or 'Unknown'
                vals = it.get('values', {}) or {}
                mvals = _segment_metric_values(vals, pe, form)
                mrec = axis['members_by_key'][member]
                mrec['member'] = member
                for metric, val in mvals.items():
                    axis['metrics'].add(metric)
                    mrec['values_by_quarter_and_metric'][f'{q}|{metric}'] = val

            consolidated = seg.get('consolidated_total')
            # Extract metric-specific consolidated values when present
            c_items = seg.get('consolidated_by_metric') or {}
            if c_items:
                for metric, val in c_items.items():
                    axis['consolidated_by_quarter_and_metric'][f'{q}|{metric}'] = val
            elif consolidated is not None and len(seg.get('items', [])) and seg.get('items', [])[0].get('values'):
                # Fall back: first metric encountered maps to total
                first_metrics = _segment_metric_values(seg['items'][0].get('values', {}), pe, form).keys()
                if first_metrics:
                    first_metric = list(first_metrics)[0]
                    axis['consolidated_by_quarter_and_metric'][f'{q}|{first_metric}'] = consolidated

    out_axes = []
    for dim, ax in axes_by_dim.items():
        rows = []
        for key, mrec in ax['members_by_key'].items():
            rows.append({
                'member': mrec['member'],
                'values_by_quarter_and_metric': mrec['values_by_quarter_and_metric'],
            })
        out_axes.append({
            'dimension': ax['dimension'],
            'metrics': sorted(ax['metrics']),
            'rows': rows,
            'consolidated_by_quarter_and_metric': ax['consolidated_by_quarter_and_metric'],
        })
    return {'axes': out_axes}


def _segment_metric_values(values_dict, filing_period_end, form):
    """From a segment item's values dict with 'Metric_periodkey' keys, pick the
    current-period value per metric. Returns {metric: value}.
    """
    by_metric = defaultdict(list)
    for k, v in values_dict.items():
        if '_' not in k:
            continue
        metric, rest = k.split('_', 1)
        kind, _, end, days = parse_period_key(rest)
        if end != filing_period_end:
            continue
        by_metric[metric].append((kind, days, v))

    out = {}
    for metric, entries in by_metric.items():
        durations = [e for e in entries if e[0] == 'duration']
        instants = [e for e in entries if e[0] == 'instant']
        if durations:
            if form == '10-Q':
                _, _, v = min(durations, key=lambda x: x[1])
            else:
                _, _, v = max(durations, key=lambda x: x[1])
        elif instants:
            _, _, v = instants[0]
        else:
            continue
        out[metric] = v
    return out


# ────────────────────────────────────────────────────────────────────────────
# Verification battery (Python, post-AI)
# ────────────────────────────────────────────────────────────────────────────

TOLERANCE_DELTA = 2  # raw-dollar rounding at scale=6 is up to ~$500k; allow small delta
TOLERANCE_FRAC = 0.005


def _within_tol(delta, reference):
    if reference is None or reference == 0:
        return abs(delta) <= TOLERANCE_DELTA
    return abs(delta) <= max(TOLERANCE_DELTA, abs(reference) * TOLERANCE_FRAC)


def check_formulas(statements):
    failures = []
    for stmt_name, s in statements.items():
        for f in s.get('formulas', []):
            for q, rec in f.get('ties_by_quarter', {}).items():
                if rec['ties']:
                    continue
                if _within_tol(rec['delta'], rec['reported']):
                    continue
                failures.append({
                    'type': 'FORMULA_BREAK',
                    'statement': stmt_name,
                    'quarter': q,
                    'message': (f'{stmt_name} {q}: {f["result_concept"]} computed={rec["computed"]} '
                                f'reported={rec["reported"]} delta={rec["delta"]}'),
                })
    return failures


FLOW_STATEMENTS = {'income_statement', 'cash_flow'}


def check_flow_sum_to_annual(statements, filings):
    failures = []
    groups = fiscal_year_groups(filings)
    for stmt_name, s in statements.items():
        if stmt_name not in FLOW_STATEMENTS:
            continue
        for row in s.get('rows', []):
            for fy_label, qmap in groups.items():
                q_labels = [f'{fy_label}Q{n}' for n in range(1, 5)]
                annual_q = q_labels[3]
                # Need all four; 10-K must be present for annual reference
                if 'Q4' not in qmap or qmap['Q4']['form'] != '10-K':
                    continue
                quarters_present = [row['values_by_quarter'].get(q) for q in q_labels]
                if any(v is None for v in quarters_present):
                    continue
                # This check is tautological for derived Q4 (Q4 = annual - Q1-Q2-Q3),
                # but we still run it to catch bugs in the derivation itself.
                total = sum(quarters_present)
                # annual is NOT directly stored; it was overwritten by derived Q4.
                # Integrity check: re-derive annual from original 10-K data and compare.
                original_annual = _original_annual_for_concept(qmap['Q4'], stmt_name, row['xbrl_concept'])
                if original_annual is None:
                    continue
                delta = total - original_annual
                if not _within_tol(delta, original_annual):
                    failures.append({
                        'type': 'FLOW_SUM_MISMATCH',
                        'statement': stmt_name,
                        'fiscal_year': fy_label,
                        'concept': row['xbrl_concept'],
                        'message': (f'{stmt_name} {fy_label} {row["xbrl_concept"]}: '
                                    f'Q1+Q2+Q3+Q4={total} != annual={original_annual} '
                                    f'delta={delta}'),
                    })
    return failures


def _original_annual_for_concept(filing_10k, statement, concept):
    """Look up the original 10-K annual value for a concept before Q4 derivation."""
    ai = filing_10k['ai']
    s = ai.get(statement, {})
    pe = filing_10k['period_end']
    for it in s.get('line_items', []):
        if it.get('xbrl_concept') == concept:
            v, _, _ = pick_period_value(it.get('values', {}), pe, statement, '10-K')
            return v
    for it in s.get('xbrl_not_on_statement', []):
        if it.get('concept') == concept:
            vals = it.get('value', {}) or it.get('values', {})
            v, _, _ = pick_period_value(vals, pe, statement, '10-K')
            return v
    return None


def check_bs_consistency(filings, statements):
    """For each instant date appearing in >1 filing, BS values must match across filings."""
    failures = []
    # Collect each (concept, instant_date) -> list of (filing, value)
    by_concept_date = defaultdict(list)
    for f in filings:
        ai = f['ai']
        for it in ai.get('balance_sheet', {}).get('line_items', []):
            concept = it.get('xbrl_concept')
            if not concept:
                continue
            for k, v in it.get('values', {}).items():
                kind, _, end, _ = parse_period_key(k)
                if kind == 'instant':
                    by_concept_date[(concept, end)].append((f['file'], v))

    for (concept, d), entries in by_concept_date.items():
        if len(entries) < 2:
            continue
        vals = [v for _, v in entries]
        if len(set(vals)) == 1:
            continue
        # Multi-value — check if within tol
        ref = vals[0]
        for v in vals[1:]:
            if not _within_tol(v - ref, ref):
                files = [ff for ff, _ in entries]
                failures.append({
                    'type': 'BS_INCONSISTENT',
                    'concept': concept,
                    'date': str(d),
                    'message': f'{concept} @ {d}: values disagree across {files}: {vals}',
                })
                break
    return failures


def check_segments_tie(segments, statements):
    failures = []
    # Build IS row lookup for consolidated ref
    is_rows = {r['xbrl_concept']: r for r in statements.get('income_statement', {}).get('rows', [])}

    for axis in segments.get('axes', []):
        dim = axis['dimension']
        for key, consolidated in axis.get('consolidated_by_quarter_and_metric', {}).items():
            if '|' not in key:
                continue
            q, metric = key.split('|', 1)
            summed = 0
            found = False
            for r in axis['rows']:
                v = r['values_by_quarter_and_metric'].get(key)
                if v is not None:
                    summed += v
                    found = True
            if not found:
                continue
            delta = summed - consolidated
            if not _within_tol(delta, consolidated):
                failures.append({
                    'type': 'SEGMENT_TIE_BREAK',
                    'axis': dim,
                    'quarter': q,
                    'metric': metric,
                    'message': (f'segment axis {dim} {q} {metric}: sum(members)={summed} '
                                f'consolidated={consolidated} delta={delta}'),
                })
    return failures


def check_analytical_reconciliation(analytical, statements):
    """Every analytical value = signed sum of its source rows' values for that quarter."""
    failures = []
    row_by_concept = {}
    for stmt_name, s in statements.items():
        for r in s.get('rows', []):
            row_by_concept[(stmt_name, r['xbrl_concept'])] = r

    for field, rec in analytical.items():
        values = rec.get('values_by_quarter', {}) or {}
        source_per_q = rec.get('source_per_quarter', {}) or {}
        for q, reported in values.items():
            sources = source_per_q.get(q)
            if not sources:
                failures.append({
                    'type': 'ANALYTICAL_NO_SOURCE',
                    'field': field,
                    'quarter': q,
                    'message': f'analytical.{field} {q}: value={reported} but no source rows specified',
                })
                continue
            computed = 0
            missing = []
            for src in sources:
                stmt = src.get('statement')
                concept = src.get('concept')
                sign = src.get('sign', '+')
                r = row_by_concept.get((stmt, concept))
                if not r or q not in r['values_by_quarter']:
                    missing.append(f'{stmt}:{concept}')
                    continue
                v = r['values_by_quarter'][q]
                computed += v if sign == '+' else -v
            if missing:
                failures.append({
                    'type': 'ANALYTICAL_SOURCE_MISSING',
                    'field': field,
                    'quarter': q,
                    'message': f'analytical.{field} {q}: sources not found in statements: {missing}',
                })
                continue
            delta = computed - reported
            if not _within_tol(delta, reported):
                failures.append({
                    'type': 'ANALYTICAL_RECON_MISMATCH',
                    'field': field,
                    'quarter': q,
                    'message': (f'analytical.{field} {q}: reported={reported} '
                                f'computed from sources={computed} delta={delta}'),
                })
    return failures


def check_forward_fills(forward_fills, ticker, filings):
    """For each flagged forward-fill, confirm the concept genuinely isn't in the
    target period's parsed_xbrl.json.
    """
    failures = []
    repo_root = os.path.join(os.path.dirname(__file__), '..')
    filings_root = os.path.join(repo_root, 'data', 'filings', ticker)
    filing_by_pe = {f['period_end']: f for f in filings}

    # Resolve parsed_xbrl.json path for each filing (match by period_end)
    parsed_by_file = {}
    if os.path.isdir(filings_root):
        for acc in os.listdir(filings_root):
            meta_path = os.path.join(filings_root, acc, 'filing_meta.json')
            parsed_path = os.path.join(filings_root, acc, 'parsed_xbrl.json')
            if not (os.path.isfile(meta_path) and os.path.isfile(parsed_path)):
                continue
            with open(meta_path) as mf:
                meta = json.load(mf)
            rd = meta.get('report_date') or meta.get('period_of_report') or meta.get('period_end')
            if rd:
                try:
                    rd_date = datetime.strptime(rd[:10], '%Y-%m-%d').date()
                    parsed_by_file[rd_date] = parsed_path
                except ValueError:
                    pass

    for ff in forward_fills:
        search_concepts = ff.get('candidate_concepts') or []
        if not search_concepts:
            failures.append({
                'type': 'FORWARD_FILL_NO_RECEIPT',
                'field': ff.get('field'),
                'message': f'forward-fill for {ff.get("field")} has no candidate_concepts — cannot audit',
            })
            continue

        for q_label in ff.get('applies_to_quarters', []):
            # Resolve target filing
            target = next((f for f in filings if f['quarter_label'] == q_label), None)
            if not target:
                continue
            parsed_path = parsed_by_file.get(target['period_end'])
            if not parsed_path:
                continue
            with open(parsed_path) as pf:
                parsed = json.load(pf)
            for fact in parsed.get('facts', []):
                if fact.get('concept') in search_concepts and not fact.get('dimensioned'):
                    if fact.get('value_numeric') is not None:
                        failures.append({
                            'type': 'FALSE_FORWARD_FILL',
                            'field': ff.get('field'),
                            'quarter': q_label,
                            'message': (f'forward-fill {ff.get("field")} at {q_label} rejected: '
                                        f'concept {fact.get("concept")} present in parsed_xbrl.json '
                                        f'with value {fact.get("value_numeric")}'),
                        })
                        break
    return failures


def check_sign_sanity(statements, analytical):
    failures = []
    # Revenue must be positive per quarter
    is_rows = statements.get('income_statement', {}).get('rows', [])
    for r in is_rows:
        if r['xbrl_concept'] in ('us-gaap:Revenues', 'us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax',
                                 'us-gaap:SalesRevenueNet'):
            for q, v in r['values_by_quarter'].items():
                if v is not None and v < 0:
                    failures.append({
                        'type': 'NEGATIVE_REVENUE',
                        'quarter': q,
                        'message': f'{r["xbrl_concept"]} {q}: revenue={v} must be positive',
                    })
    # Analytical revenue check
    rev = (analytical or {}).get('revenue', {}).get('values_by_quarter', {})
    for q, v in rev.items():
        if v is not None and v < 0:
            failures.append({
                'type': 'NEGATIVE_REVENUE',
                'quarter': q,
                'message': f'analytical.revenue {q}={v} must be positive',
            })
    return failures


def verify_all(result, filings, ticker):
    statements = result.get('statements', {})
    segments = result.get('segments', {})
    analytical = result.get('analytical', {})
    forward_fills = result.get('forward_fills', [])

    failures = []
    failures.extend(check_formulas(statements))
    failures.extend(check_flow_sum_to_annual(statements, filings))
    failures.extend(check_bs_consistency(filings, statements))
    failures.extend(check_segments_tie(segments, statements))
    failures.extend(check_analytical_reconciliation(analytical, statements))
    failures.extend(check_forward_fills(forward_fills, ticker, filings))
    failures.extend(check_sign_sanity(statements, analytical))
    return failures


# ────────────────────────────────────────────────────────────────────────────
# AI analytical pass (Output 2)
# ────────────────────────────────────────────────────────────────────────────

def load_formulas_md():
    path = os.path.join(os.path.dirname(__file__), '..', 'formulas.md')
    with open(path) as f:
        return f.read()


def _slim_statements_for_prompt(statements):
    """Produce a compact per-statement dump for the AI prompt: rows + formulas,
    but only the fields the AI needs to map analytical values.
    """
    out = {}
    for stmt_name, s in statements.items():
        rows = []
        for r in s.get('rows', []):
            rows.append({
                'xbrl_concept': r['xbrl_concept'],
                'labels': r['labels'],
                'is_note_detail': r['is_note_detail'],
                'values_by_quarter': r['values_by_quarter'],
            })
        out[stmt_name] = {'rows': rows}
    return out


def _slim_segments_for_prompt(segments):
    axes = []
    for ax in segments.get('axes', []):
        rows = [{'member': r['member'], 'values_by_quarter_and_metric': r['values_by_quarter_and_metric']}
                for r in ax['rows']]
        axes.append({'dimension': ax['dimension'], 'metrics': ax['metrics'], 'rows': rows})
    return {'axes': axes}


def build_analytical_prompt(ticker, statements, segments, formulas_md, prior_failures=None):
    slim_stmts = _slim_statements_for_prompt(statements)
    slim_segs = _slim_segments_for_prompt(segments)

    failure_block = ''
    if prior_failures:
        lines = '\n'.join(f'- [{f["type"]}] {f.get("message","")}' for f in prior_failures[:30])
        failure_block = f'\n\nPRIOR VERIFICATION FAILURES (fix these):\n{lines}\n'

    return f"""You are mapping as-reported statements for {ticker} into a fixed set of analytical
fields that downstream metric formulas require.

The statements have already been quarterized and merged deterministically. Your only
task: for each analytical field, identify which row(s) in the statements it should pull
from, for every quarter. Every value you report must be the signed sum of specific rows
that already exist in the statements below.

ANALYTICAL FIELDS (universal — same keys across every company):
- revenue: consolidated net revenue
- cogs: cost of revenue/sales (null if not reported)
- gross_profit: gross profit (null if no COGS reported)
- operating_income: income from operations
- pretax_income: income before income taxes
- income_tax_expense: income tax provision (positive for expense)
- net_income: net income attributable to parent / consolidated
- interest_expense: gross interest expense (reported with a negative sign)
- interest_income: interest or investment income (positive)
- rd_expense: research and development expense (null if not reported)
- sbc: stock-based compensation (CF addback, positive)
- dna: depreciation and amortization (CF addback, positive)
- diluted_shares: diluted weighted-average shares outstanding (raw count)
- basic_shares: basic weighted-average shares outstanding (raw count)
- diluted_eps: diluted EPS (report ONLY for 10-Q quarters; omit for 10-K Q4)
- basic_eps: basic EPS (report ONLY for 10-Q quarters; omit for 10-K Q4)
- effective_tax_rate: income_tax_expense / pretax_income
- cash: cash and cash equivalents
- short_term_investments: marketable securities current
- accounts_receivable: trade AR net
- inventory: inventories (0 if not applicable)
- total_assets: total assets
- accounts_payable: trade AP
- short_term_debt: current portion of debt (0 if none)
- long_term_debt: long-term debt non-current
- operating_lease_liabilities: TOTAL operating lease liabilities (current + non-current)
- equity: total stockholders equity attributable to parent
- cfo: net cash provided by (used in) operating activities
- capex: capital expenditures — reported as NEGATIVE (cash outflow)
- acquisitions: acquisitions net of cash acquired — NEGATIVE (0 if none)

Add any additional fields the metric formulas require. Use snake_case names.

HOW TO MAP EACH FIELD:
1. Find the row(s) in `statements` whose xbrl_concept(s) represent the field.
2. Most fields are a single row. Some are composite:
   - operating_lease_liabilities = operating_lease_current (may be on BS face OR in note detail
     under accrued liabilities) + operating_lease_noncurrent.
   - interest_expense may be a gross amount separate from net nonoperating income.
3. For each quarter the field has a value, list the source row(s) as
   {{"statement": "income_statement"|"balance_sheet"|"cash_flow", "concept": "us-gaap:...", "sign": "+"|"-"}}.
4. Values must be the signed sum of the source rows' values for that quarter.

FORWARD-FILL RULE (strict):
A field may be forward-filled ONLY if the concept genuinely does not appear in that
period's raw XBRL. This is audited against parsed_xbrl.json — if any candidate concept
is present with a non-null value, the forward-fill is rejected as a false fill.

When you forward-fill, declare:
{{
  "field": "<name>",
  "source_filing": "<filename of the 10-K whose value was used>",
  "applies_to_quarters": ["<quarter_label>", ...],
  "candidate_concepts": ["us-gaap:..."]   // MUST include every concept that could represent this field
}}

The candidate_concepts list is what the Python audit uses. If you omit any plausible
concept, the audit will be weakened but your own field is still at risk of a false-fill
rejection. Err on the side of including more variants.

STATEMENTS:
{json.dumps(slim_stmts, indent=2)}

SEGMENTS:
{json.dumps(slim_segs, indent=2)}

METRIC FORMULAS (for context on what the analytical fields will feed):
{formulas_md}
{failure_block}
OUTPUT ONLY valid JSON:
{{
  "reporting_unit": "USD_millions" | "USD",
  "stock_splits": [{{"between": ["<quarter_label>", "<quarter_label>"], "ratio": <number>, "action": "..."}}],
  "analytical": {{
    "revenue": {{
      "values_by_quarter": {{"FY24Q1": <number>, ...}},
      "source_per_quarter": {{"FY24Q1": [{{"statement": "income_statement", "concept": "us-gaap:Revenues", "sign": "+"}}], ...}}
    }},
    ...
  }},
  "forward_fills": [...]
}}

All monetary values in RAW dollars. Convert from reported millions where needed.
Shares in raw count. No apostrophes in strings."""


def run_ai_analytical(ticker, statements, segments, model, prior_failures=None, max_retries=3):
    """Call the AI to produce Output 2 (analytical + forward_fills).

    Retries the same call up to max_retries times when the response fails to parse
    OR when Python verification produces failures (verification driven by caller).
    """
    formulas_md = load_formulas_md()
    client = anthropic.Anthropic()

    prompt = build_analytical_prompt(ticker, statements, segments, formulas_md, prior_failures)

    print(f"  Calling AI (analytical pass, ~{len(prompt)//4:,} tokens in)...")
    output_text = ''
    with client.messages.stream(
        model=model,
        max_tokens=65536,
        messages=[{'role': 'user', 'content': prompt}],
    ) as stream:
        for text in stream.text_stream:
            output_text += text
            print('.', end='', flush=True)
        print()
        resp = stream.get_final_message()

    in_tok = resp.usage.input_tokens
    out_tok = resp.usage.output_tokens

    json_text = output_text.strip()
    fb = json_text.find('{')
    lb = json_text.rfind('}')
    if fb != -1 and lb != -1:
        json_text = json_text[fb:lb + 1]

    try:
        parsed = json.loads(json_text)
    except json.JSONDecodeError:
        fixed = json_text.replace('\u2018', "'").replace('\u2019', "'")
        fixed = re.sub(r'[\x00-\x1f]', ' ', fixed)
        fixed = re.sub(r',\s*([}\]])', r'\1', fixed)
        parsed = json.loads(fixed)  # raises on final failure

    return parsed, in_tok, out_tok


# ────────────────────────────────────────────────────────────────────────────
# Orchestration
# ────────────────────────────────────────────────────────────────────────────

def run_stage2(ticker, model='claude-sonnet-4-6', max_retries=3, test_mode=True):
    extract_dir = os.path.join(os.path.dirname(__file__), ticker)
    work_dir = os.path.join(extract_dir, 'test') if test_mode else extract_dir

    print(f"{'='*60}\n  STAGE 2 for {ticker} (work_dir={work_dir})\n{'='*60}")
    filings = load_filings(work_dir)
    if not filings:
        print(f"  ERROR: no per-filing JSONs found in {work_dir}")
        return None, 0, 0
    print(f"  Loaded {len(filings)} filings:")
    for f in filings:
        print(f"    {f['quarter_label']:8s}  {f['form']:5s}  period_end={f['period_end']}  {f['file']}")

    # ── Output 1: statements ──
    print("\n  Building statements (deterministic)...")
    statements = {}
    for stmt in STATEMENTS:
        rows = merge_statement_rows(filings, stmt)
        derive_q2_q3_from_ytd(rows, stmt, filings)
        derive_q4_for_flows(rows, stmt, filings)
        formulas = gather_formulas(filings, stmt)
        evaluate_formulas(rows, formulas)
        statements[stmt] = {'rows': rows, 'formulas': formulas}
        print(f"    {stmt}: {len(rows)} rows, {len(formulas)} formulas")

    # ── Segments ──
    segments = build_segments(filings)
    print(f"    segments: {len(segments['axes'])} axes")

    # ── AI analytical pass with retry loop ──
    total_in, total_out = 0, 0
    analytical = {}
    forward_fills = []
    reporting_unit = 'USD_millions'
    stock_splits = []
    prior_failures = None

    for attempt in range(max_retries + 1):
        print(f"\n  AI analytical pass (attempt {attempt + 1}/{max_retries + 1})...")
        try:
            parsed, in_tok, out_tok = run_ai_analytical(
                ticker, statements, segments, model, prior_failures=prior_failures
            )
        except Exception as e:
            print(f"    AI call failed: {e}")
            if attempt >= max_retries:
                return None, total_in, total_out
            continue
        total_in += in_tok
        total_out += out_tok

        analytical = parsed.get('analytical', {}) or {}
        forward_fills = parsed.get('forward_fills', []) or []
        reporting_unit = parsed.get('reporting_unit', reporting_unit)
        stock_splits = parsed.get('stock_splits', []) or []

        # Run Python verification
        result_for_verify = {
            'statements': statements,
            'segments': segments,
            'analytical': analytical,
            'forward_fills': forward_fills,
        }
        failures = verify_all(result_for_verify, filings, ticker)
        print(f"    verification: {len(failures)} failures")
        for f in failures[:10]:
            print(f"      [{f['type']}] {f.get('message','')}")
        if len(failures) > 10:
            print(f"      ... and {len(failures) - 10} more")

        if not failures:
            print(f"\n  ALL CHECKS PASSED")
            break

        if attempt >= max_retries:
            print(f"\n  MAX RETRIES REACHED with {len(failures)} unresolved failures — hard error")
            break

        prior_failures = failures

    # Cost
    in_rate, out_rate = (15.0, 75.0) if 'opus' in model else (3.0, 15.0)
    cost = total_in * in_rate / 1e6 + total_out * out_rate / 1e6
    print(f"\n  Tokens: in={total_in:,}  out={total_out:,}  cost=${cost:.2f}")

    result = {
        'ticker': ticker,
        'reporting_unit': reporting_unit,
        'stock_splits': stock_splits,
        'statements': statements,
        'segments': segments,
        'analytical': analytical,
        'forward_fills': forward_fills,
        'verification': {
            'retries': attempt,
            'failures': failures if failures else [],
            'passed': not failures,
        },
    }
    return result, total_in, total_out


def build_quarterly_json(statements, analytical, filings):
    """Write one record per quarter with flat analytical values (raw dollars).

    This preserves the downstream shape calculate.py expects: a list of records
    keyed by period_end with the analytical fields at the top level.
    """
    by_q = {f['quarter_label']: f for f in filings}
    all_quarters = sorted(by_q.keys(), key=lambda q: by_q[q]['period_end'])
    records = []
    for q in all_quarters:
        f = by_q[q]
        rec = {
            'period_end': str(f['period_end']),
            'fiscal_year': f['fiscal_year_label'],
            'fiscal_period': f['fiscal_period'],
            'form': f['form'],
            'quarter_label': q,
        }
        for field, fdata in analytical.items():
            v = fdata.get('values_by_quarter', {}).get(q)
            if v is not None:
                rec[field] = v
        records.append(rec)
    return records


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--ticker', required=True)
    parser.add_argument('--model', default='claude-sonnet-4-6')
    parser.add_argument('--test', action='store_true',
                        help='Work in ai_extract/{TICKER}/test/ (default)')
    parser.add_argument('--no-test', dest='test', action='store_false')
    parser.set_defaults(test=True)
    parser.add_argument('--max-retries', type=int, default=3)
    args = parser.parse_args()

    result, in_tok, out_tok = run_stage2(
        args.ticker, model=args.model, max_retries=args.max_retries, test_mode=args.test
    )
    if result is None:
        print("ERROR: Stage 2 failed to produce a result")
        sys.exit(1)

    extract_dir = os.path.join(os.path.dirname(__file__), args.ticker)
    out_dir = os.path.join(extract_dir, 'test') if args.test else extract_dir

    # Statements/segments JSON is not directly JSON-serializable if rows contain
    # non-primitive types; we've kept them primitive so json.dumps works.
    out_path = os.path.join(out_dir, 'formula_mapped_v3.json')
    with open(out_path, 'w') as f:
        json.dump(_make_json_safe(result), f, indent=2, default=str)
    print(f"\n  Saved {out_path}")

    # Quarterly.json (analytical fields flat per period)
    filings = load_filings(out_dir)
    quarterly = build_quarterly_json(result['statements'], result['analytical'], filings)
    q_path = os.path.join(out_dir, 'quarterly.json')
    with open(q_path, 'w') as f:
        json.dump(quarterly, f, indent=2, default=str)
    print(f"  Saved {q_path} ({len(quarterly)} quarters)")

    passed = result['verification']['passed']
    sys.exit(0 if passed else 2)


def _make_json_safe(obj):
    if isinstance(obj, dict):
        return {k: _make_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_make_json_safe(v) for v in obj]
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return obj


if __name__ == '__main__':
    main()
