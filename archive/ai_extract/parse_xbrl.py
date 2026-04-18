"""
Deterministic XBRL parser.

Parses an XBRL instance document (the _htm.xml file) and extracts all facts
into clean JSON. Also parses linkbase files (calculation, presentation,
definition) when available, extracting formula relationships, statement
structure, and dimension hierarchies.

No AI, no judgment — just structured data.

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


def _extract_concept(href):
    """Extract concept name from an xlink:href like '...#us-gaap_Revenues'."""
    if '#' not in href:
        return href
    fragment = href.split('#')[-1]
    # Convert first underscore to colon: us-gaap_Revenues -> us-gaap:Revenues
    if '_' in fragment:
        return fragment.replace('_', ':', 1)
    return fragment


def _build_loc_map(link_elem, ns):
    """Build a mapping from xlink:label -> concept name for all loc elements."""
    locs = {}
    for loc in link_elem.findall('link:loc', ns):
        label = loc.get('{http://www.w3.org/1999/xlink}label')
        href = loc.get('{http://www.w3.org/1999/xlink}href', '')
        locs[label] = _extract_concept(href)
    return locs


def parse_calculation_linkbase(cal_path):
    """Parse calculation linkbase into formula relationships grouped by role.

    Returns a list of sections, each with a role name and formulas.
    Each formula: parent concept = sum of children with signed weights.
    """
    if not os.path.exists(cal_path):
        return []

    tree = etree.parse(cal_path)
    root = tree.getroot()
    ns = {'link': 'http://www.xbrl.org/2003/linkbase',
          'xlink': 'http://www.w3.org/1999/xlink'}

    sections = []
    for clink in root.findall('.//link:calculationLink', ns):
        role = clink.get('{http://www.w3.org/1999/xlink}role', '')
        short_role = role.split('/role/')[-1] if '/role/' in role else role

        locs = _build_loc_map(clink, ns)

        # Group arcs by parent
        by_parent = {}
        for arc in clink.findall('link:calculationArc', ns):
            parent_label = arc.get('{http://www.w3.org/1999/xlink}from')
            child_label = arc.get('{http://www.w3.org/1999/xlink}to')
            weight = float(arc.get('weight', '1.0'))
            order = float(arc.get('order', '0'))

            parent = locs.get(parent_label, parent_label)
            child = locs.get(child_label, child_label)

            by_parent.setdefault(parent, []).append({
                'concept': child,
                'weight': weight,
                'order': order
            })

        # Sort children by order within each parent
        formulas = []
        for parent, children in by_parent.items():
            children.sort(key=lambda x: x['order'])
            formulas.append({
                'parent': parent,
                'children': [{'concept': c['concept'], 'weight': c['weight']}
                             for c in children]
            })

        sections.append({
            'role': short_role,
            'formulas': formulas
        })

    return sections


def parse_presentation_linkbase(pre_path):
    """Parse presentation linkbase into statement structure grouped by role.

    Returns a list of sections, each with a role name and a tree of
    parent->children relationships with display order.
    """
    if not os.path.exists(pre_path):
        return []

    tree = etree.parse(pre_path)
    root = tree.getroot()
    ns = {'link': 'http://www.xbrl.org/2003/linkbase',
          'xlink': 'http://www.w3.org/1999/xlink'}

    sections = []
    for plink in root.findall('.//link:presentationLink', ns):
        role = plink.get('{http://www.w3.org/1999/xlink}role', '')
        short_role = role.split('/role/')[-1] if '/role/' in role else role

        locs = _build_loc_map(plink, ns)

        # Group arcs by parent
        by_parent = {}
        for arc in plink.findall('link:presentationArc', ns):
            parent_label = arc.get('{http://www.w3.org/1999/xlink}from')
            child_label = arc.get('{http://www.w3.org/1999/xlink}to')
            order = float(arc.get('order', '0'))

            parent = locs.get(parent_label, parent_label)
            child = locs.get(child_label, child_label)

            by_parent.setdefault(parent, []).append({
                'concept': child,
                'order': order
            })

        # Sort children by order
        structure = []
        for parent, children in by_parent.items():
            children.sort(key=lambda x: x['order'])
            structure.append({
                'parent': parent,
                'children': [c['concept'] for c in children]
            })

        sections.append({
            'role': short_role,
            'structure': structure
        })

    return sections


def parse_definition_linkbase(def_path):
    """Parse definition linkbase into dimension hierarchies grouped by role.

    Returns a list of sections, each with a role name and dimension
    member relationships (axis -> members).
    """
    if not os.path.exists(def_path):
        return []

    tree = etree.parse(def_path)
    root = tree.getroot()
    ns = {'link': 'http://www.xbrl.org/2003/linkbase',
          'xlink': 'http://www.w3.org/1999/xlink'}

    sections = []
    for dlink in root.findall('.//link:definitionLink', ns):
        role = dlink.get('{http://www.w3.org/1999/xlink}role', '')
        short_role = role.split('/role/')[-1] if '/role/' in role else role

        locs = _build_loc_map(dlink, ns)

        by_parent = {}
        for arc in dlink.findall('link:definitionArc', ns):
            parent_label = arc.get('{http://www.w3.org/1999/xlink}from')
            child_label = arc.get('{http://www.w3.org/1999/xlink}to')
            arcrole = arc.get('{http://www.w3.org/1999/xlink}arcrole', '')
            order = float(arc.get('order', '0'))

            parent = locs.get(parent_label, parent_label)
            child = locs.get(child_label, child_label)

            by_parent.setdefault(parent, []).append({
                'concept': child,
                'arcrole': arcrole.split('/')[-1] if '/' in arcrole else arcrole,
                'order': order
            })

        # Sort children by order
        hierarchies = []
        for parent, children in by_parent.items():
            children.sort(key=lambda x: x['order'])
            hierarchies.append({
                'parent': parent,
                'children': [{'concept': c['concept'], 'arcrole': c['arcrole']}
                             for c in children]
            })

        if hierarchies:
            sections.append({
                'role': short_role,
                'hierarchies': hierarchies
            })

    return sections


def parse_filing(ticker, accession, write_output=False):
    """Parse a filing's XBRL instance document and linkbase files.

    Returns structured JSON with facts, calculations, presentation, and definitions.
    If write_output=True, also writes parsed_xbrl.json to the filing directory.
    """
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

    result = {
        'ticker': ticker,
        'accession': accession,
        'form': meta.get('form'),
        'report_date': meta.get('report_date'),
        'filing_date': meta.get('filing_date'),
        'total_contexts': len(contexts),
        'total_facts': len(facts),
        'facts': facts
    }

    # Parse linkbase files if available
    cal_file = meta.get('cal_filename')
    if cal_file:
        cal_path = os.path.join(base_dir, cal_file)
        result['calculations'] = parse_calculation_linkbase(cal_path)

    pre_file = meta.get('pre_filename')
    if pre_file:
        pre_path = os.path.join(base_dir, pre_file)
        result['presentation'] = parse_presentation_linkbase(pre_path)

    def_file = meta.get('def_filename')
    if def_file:
        def_path = os.path.join(base_dir, def_file)
        result['definitions'] = parse_definition_linkbase(def_path)

    if write_output:
        output_path = os.path.join(base_dir, 'parsed_xbrl.json')
        with open(output_path, 'w') as f:
            json.dump(result, f, indent=2)

    return result


def main():
    parser = argparse.ArgumentParser(description='Parse XBRL instance document into structured JSON')
    parser.add_argument('--ticker', required=True)
    parser.add_argument('--accession', required=True)
    parser.add_argument('--summary', action='store_true', help='Print summary stats instead of writing parsed_xbrl.json')
    args = parser.parse_args()

    result = parse_filing(args.ticker, args.accession, write_output=not args.summary)

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

        # Linkbase stats
        if 'calculations' in result:
            total_formulas = sum(len(s['formulas']) for s in result['calculations'])
            print(f"\nCalculation linkbase: {len(result['calculations'])} sections, {total_formulas} formulas")
            for s in result['calculations']:
                print(f"  {s['role']}: {len(s['formulas'])} formulas")

        if 'presentation' in result:
            print(f"\nPresentation linkbase: {len(result['presentation'])} sections")
            for s in result['presentation']:
                total_items = sum(len(st['children']) for st in s['structure'])
                print(f"  {s['role']}: {total_items} items")

        if 'definitions' in result:
            print(f"\nDefinition linkbase: {len(result['definitions'])} sections")
    else:
        base_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'filings', args.ticker, args.accession)
        output_path = os.path.join(base_dir, 'parsed_xbrl.json')
        print(f"Written to {output_path}")
        print(f"  {result['total_facts']} facts, {len(result.get('calculations', []))} calculation sections, "
              f"{len(result.get('presentation', []))} presentation sections, "
              f"{len(result.get('definitions', []))} definition sections")


if __name__ == '__main__':
    main()
