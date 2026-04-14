"""
Deterministic XBRL fact parser.

Parses an XBRL instance document (the _htm.xml file) and extracts all facts
into clean JSON. No AI, no judgment — just structured data.

Each fact includes: concept name, namespace, value, period, unit, decimals,
and whether it's dimensioned (segment-qualified).

Usage:
    python3 ai_extract/parse_xbrl.py --ticker NVDA --accession 0001045810-25-000116
"""

import argparse
import json
import os
import sys
from lxml import etree


def parse_contexts(root, ns):
    """Parse all xbrli:context elements into a lookup dict."""
    contexts = {}
    for ctx in root.findall('xbrli:context', ns):
        cid = ctx.get('id')
        period_elem = ctx.find('xbrli:period', ns)
        entity_elem = ctx.find('xbrli:entity', ns)
        segment_elem = entity_elem.find('xbrli:segment', ns) if entity_elem is not None else None

        # Period
        period = {}
        if period_elem is not None:
            instant = period_elem.find('xbrli:instant', ns)
            start = period_elem.find('xbrli:startDate', ns)
            end = period_elem.find('xbrli:endDate', ns)
            if instant is not None:
                period['type'] = 'instant'
                period['date'] = instant.text
            elif start is not None and end is not None:
                period['type'] = 'duration'
                period['startDate'] = start.text
                period['endDate'] = end.text

        # Dimensions (segment qualifiers)
        dimensions = []
        if segment_elem is not None:
            for member in segment_elem:
                dim_attr = member.get('dimension', '')
                dim_value = member.text or ''
                dimensions.append({
                    'dimension': dim_attr,
                    'member': dim_value.strip()
                })

        contexts[cid] = {
            'period': period,
            'dimensioned': len(dimensions) > 0,
            'dimensions': dimensions if dimensions else None
        }

    return contexts


def parse_units(root, ns):
    """Parse all xbrli:unit elements into a lookup dict."""
    units = {}
    for unit in root.findall('xbrli:unit', ns):
        uid = unit.get('id')
        measure = unit.find('xbrli:measure', ns)
        divide = unit.find('xbrli:divide', ns)

        if measure is not None:
            units[uid] = measure.text
        elif divide is not None:
            num = divide.find('xbrli:unitNumerator/xbrli:measure', ns)
            den = divide.find('xbrli:unitDenominator/xbrli:measure', ns)
            num_text = num.text if num is not None else '?'
            den_text = den.text if den is not None else '?'
            units[uid] = f'{num_text}/{den_text}'
        else:
            units[uid] = uid

    return units


def parse_facts(root, contexts, units):
    """Extract all facts (non-context, non-unit, non-schemaRef elements)."""
    facts = []
    skip_tags = {
        '{http://www.xbrl.org/2003/instance}context',
        '{http://www.xbrl.org/2003/instance}unit',
        '{http://www.xbrl.org/2003/linkbase}schemaRef',
    }

    for elem in root:
        if elem.tag in skip_tags:
            continue

        # Parse namespace and local name
        if elem.tag.startswith('{'):
            ns_uri, local_name = elem.tag[1:].split('}', 1)
        else:
            ns_uri, local_name = '', elem.tag

        # Determine namespace prefix
        prefix = ''
        for p, u in root.nsmap.items():
            if u == ns_uri and p is not None:
                prefix = p
                break

        context_ref = elem.get('contextRef')
        unit_ref = elem.get('unitRef')
        decimals = elem.get('decimals')
        value_raw = elem.text.strip() if elem.text else None

        # Resolve context and unit
        context = contexts.get(context_ref, {})
        unit = units.get(unit_ref) if unit_ref else None

        # Parse numeric value
        value_numeric = None
        if value_raw is not None and unit_ref is not None:
            try:
                value_numeric = float(value_raw)
            except ValueError:
                pass

        fact = {
            'concept': f'{prefix}:{local_name}' if prefix else local_name,
            'namespace': ns_uri,
            'value_raw': value_raw,
            'value_numeric': value_numeric,
            'unit': unit,
            'decimals': decimals,
            'context_ref': context_ref,
            'period': context.get('period'),
            'dimensioned': context.get('dimensioned', False),
            'dimensions': context.get('dimensions'),
        }
        facts.append(fact)

    return facts


def parse_filing(ticker, accession):
    """Parse a filing's XBRL instance document and return all facts."""
    base_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'filings', ticker, accession)
    meta_path = os.path.join(base_dir, 'filing_meta.json')

    with open(meta_path) as f:
        meta = json.load(f)

    xbrl_path = os.path.join(base_dir, meta['xbrl_filename'])
    tree = etree.parse(xbrl_path)
    root = tree.getroot()

    ns = {'xbrli': 'http://www.xbrl.org/2003/instance'}

    contexts = parse_contexts(root, ns)
    units = parse_units(root, ns)
    facts = parse_facts(root, contexts, units)

    return {
        'ticker': ticker,
        'accession': accession,
        'form': meta.get('form'),
        'report_date': meta.get('report_date'),
        'filing_date': meta.get('filing_date'),
        'total_contexts': len(contexts),
        'total_facts': len(facts),
        'facts': facts
    }


def main():
    parser = argparse.ArgumentParser(description='Parse XBRL instance document into structured JSON')
    parser.add_argument('--ticker', required=True)
    parser.add_argument('--accession', required=True)
    parser.add_argument('--output', help='Output file path (default: stdout)')
    parser.add_argument('--summary', action='store_true', help='Print summary stats instead of full JSON')
    args = parser.parse_args()

    result = parse_filing(args.ticker, args.accession)

    if args.summary:
        print(f"Ticker: {result['ticker']}")
        print(f"Form: {result['form']}")
        print(f"Report date: {result['report_date']}")
        print(f"Contexts: {result['total_contexts']}")
        print(f"Total facts: {result['total_facts']}")

        # Count by namespace prefix
        by_prefix = {}
        for f in result['facts']:
            prefix = f['concept'].split(':')[0] if ':' in f['concept'] else 'other'
            by_prefix[prefix] = by_prefix.get(prefix, 0) + 1
        print("\nFacts by namespace:")
        for prefix, count in sorted(by_prefix.items(), key=lambda x: -x[1]):
            print(f"  {prefix}: {count}")

        # Count dimensioned vs undimensioned
        dim = sum(1 for f in result['facts'] if f['dimensioned'])
        undim = sum(1 for f in result['facts'] if not f['dimensioned'])
        print(f"\nUndimensioned (consolidated): {undim}")
        print(f"Dimensioned (segment/member): {dim}")
    else:
        output = json.dumps(result, indent=2)
        if args.output:
            with open(args.output, 'w') as f:
                f.write(output)
            print(f"Written to {args.output}")
        else:
            print(output)


if __name__ == '__main__':
    main()
