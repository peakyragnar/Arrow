"""
Step 1: Trivial deterministic code.
Parse raw XBRL XML into a clean JSON list of facts.
No mapping, no interpretation, no company-specific logic.
Just: what facts are in this filing?
"""

import xml.etree.ElementTree as ET
import json
import sys
import os
import re

def parse_xbrl_to_facts(xml_path):
    """Parse an XBRL instance document into a flat list of facts."""
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Extract all namespace prefixes
    ns_map = {}
    for event, elem in ET.iterparse(xml_path, events=['start-ns']):
        prefix, uri = elem
        if prefix:
            ns_map[prefix] = uri

    # Reverse map: URI -> prefix (for readable output)
    uri_to_prefix = {v: k for k, v in ns_map.items()}

    # Parse contexts
    xbrl_ns = 'http://www.xbrl.org/2003/instance'
    contexts = {}
    for ctx in root.findall(f'{{{xbrl_ns}}}context'):
        ctx_id = ctx.get('id')
        period_elem = ctx.find(f'{{{xbrl_ns}}}period')

        # Check for dimensions (segment)
        entity = ctx.find(f'{{{xbrl_ns}}}entity')
        segment = entity.find(f'{{{xbrl_ns}}}segment') if entity is not None else None
        dimensions = []
        if segment is not None:
            for member in segment:
                dim_tag = member.get('dimension', '')
                dim_value = member.text or ''
                dimensions.append(f"{dim_tag}={dim_value}")

        instant = period_elem.find(f'{{{xbrl_ns}}}instant')
        start = period_elem.find(f'{{{xbrl_ns}}}startDate')
        end = period_elem.find(f'{{{xbrl_ns}}}endDate')

        if instant is not None:
            contexts[ctx_id] = {
                'type': 'instant',
                'date': instant.text,
                'dimensions': dimensions
            }
        elif start is not None and end is not None:
            contexts[ctx_id] = {
                'type': 'duration',
                'start': start.text,
                'end': end.text,
                'dimensions': dimensions
            }

    # Parse units
    units = {}
    for unit in root.findall(f'{{{xbrl_ns}}}unit'):
        unit_id = unit.get('id')
        measure = unit.find(f'{{{xbrl_ns}}}measure')
        if measure is not None:
            units[unit_id] = measure.text

    # Extract facts - everything that isn't context, unit, or schemaRef
    skip_tags = {
        f'{{{xbrl_ns}}}context',
        f'{{{xbrl_ns}}}unit',
        '{http://www.xbrl.org/2003/linkbase}schemaRef',
    }

    facts = []
    for elem in root:
        if elem.tag in skip_tags:
            continue

        # Convert tag to readable form
        match = re.match(r'\{(.+?)\}(.+)', elem.tag)
        if not match:
            continue

        uri, local_name = match.groups()
        prefix = uri_to_prefix.get(uri, uri)
        concept = f"{prefix}:{local_name}"

        ctx_ref = elem.get('contextRef')
        unit_ref = elem.get('unitRef')
        decimals = elem.get('decimals')
        text = (elem.text or '').strip()

        if not ctx_ref or not text:
            continue

        context = contexts.get(ctx_ref, {})

        fact = {
            'concept': concept,
            'value': text,
            'unit': units.get(unit_ref, unit_ref),
            'decimals': decimals,
            'context_id': ctx_ref,
        }

        # Add period info
        if context.get('type') == 'instant':
            fact['period_type'] = 'instant'
            fact['date'] = context['date']
        elif context.get('type') == 'duration':
            fact['period_type'] = 'duration'
            fact['start_date'] = context['start']
            fact['end_date'] = context['end']

        # Add dimensions if any
        if context.get('dimensions'):
            fact['dimensions'] = context['dimensions']

        facts.append(fact)

    return facts


def main():
    if len(sys.argv) < 2:
        print("Usage: python parse_xbrl_facts.py <path_to_xbrl.xml> [output.json]")
        sys.exit(1)

    xml_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else None

    facts = parse_xbrl_to_facts(xml_path)

    if output_path:
        with open(output_path, 'w') as f:
            json.dump(facts, f, indent=2)
        print(f"Extracted {len(facts)} facts -> {output_path}")
    else:
        print(json.dumps(facts, indent=2))


if __name__ == '__main__':
    main()
