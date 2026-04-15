#!/usr/bin/env python3
"""Prepare a filing for AI extraction skill.

Finds the filing, cleans HTML, parses XBRL facts, writes both to
ai_extract/{TICKER}/prep_{ACCESSION}.json for the skill to read.

Usage:
    python3 .claude/skills/extract-filing/scripts/prepare_filing.py TICKER ACCESSION

Output JSON:
    { "meta": {...}, "html": "cleaned HTML", "xbrl_facts": [...], "stats": {...} }
"""
import json
import os
import re
import sys

def clean_html(html):
    """Strip CSS styling and layout noise from iXBRL HTML."""
    cleaned = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL)
    cleaned = re.sub(r'\s+style="[^"]*"', '', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned)
    return cleaned

def main():
    if len(sys.argv) != 3:
        print("Usage: prepare_filing.py TICKER ACCESSION", file=sys.stderr)
        sys.exit(1)

    ticker = sys.argv[1].upper()
    accession = sys.argv[2]

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))
    base_dir = os.path.join(project_root, 'data', 'filings', ticker, accession)

    # Load filing metadata
    meta_path = os.path.join(base_dir, 'filing_meta.json')
    if not os.path.exists(meta_path):
        print(f"Filing not found: {meta_path}", file=sys.stderr)
        sys.exit(1)
    with open(meta_path) as f:
        meta = json.load(f)

    # Parse XBRL facts
    sys.path.insert(0, os.path.join(project_root, 'ai_extract'))
    from parse_xbrl import parse_filing
    parsed = parse_filing(ticker, accession)
    xbrl_facts = [f for f in parsed['facts'] if not f['dimensioned']]

    # Format XBRL facts as readable text
    facts_text = ""
    seen = set()
    for f in xbrl_facts:
        key = (f['concept'], f['context_ref'])
        if key in seen:
            continue
        seen.add(key)
        if f['period']:
            if f['period']['type'] == 'duration':
                period_str = f"{f['period']['startDate']} to {f['period']['endDate']}"
            else:
                period_str = f"as of {f['period']['date']}"
        else:
            period_str = ''
        val = f['value_raw']
        unit = f['unit'] or ''
        if f['value_numeric'] is not None:
            if 'USD' in unit and 'shares' not in unit:
                val = f"${f['value_numeric']/1e6:,.0f}M"
            elif 'shares' in unit and 'USD' not in unit:
                val = f"{f['value_numeric']/1e6:,.0f}M shares"
            elif 'USD' in unit and 'shares' in unit:
                val = f"${f['value_numeric']}/share"
        facts_text += f"  {f['concept']} [{period_str}] ({unit}) = {val}\n"

    # Clean HTML
    html_path = os.path.join(base_dir, meta['html_filename'])
    with open(html_path) as f:
        html_raw = f.read()
    html_cleaned = clean_html(html_raw)

    # Write output
    out_dir = os.path.join(project_root, 'ai_extract', ticker)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f'prep_{accession}.json')

    output = {
        'meta': meta,
        'xbrl_facts_text': facts_text,
        'html_cleaned': html_cleaned,
        'stats': {
            'total_facts': parsed['total_facts'],
            'undimensioned_facts': len(xbrl_facts),
            'html_raw_chars': len(html_raw),
            'html_cleaned_chars': len(html_cleaned),
            'approx_tokens': len(html_cleaned) // 4 + len(facts_text) // 4,
        }
    }

    with open(out_path, 'w') as f:
        json.dump(output, f)

    print(f"Prepared: {out_path}")
    print(f"  Form: {meta['form']}, Report date: {meta['report_date']}")
    print(f"  XBRL: {len(xbrl_facts)} undimensioned facts")
    print(f"  HTML: {len(html_raw):,} -> {len(html_cleaned):,} chars (~{len(html_cleaned)//4:,} tokens)")
    print(f"  Total: ~{output['stats']['approx_tokens']:,} tokens")

if __name__ == '__main__':
    main()
