"""
AI-powered financial statement analysis.

Sends the full filing HTML + XBRL facts to Claude. AI does the extraction,
mapping, and cross-referencing. The only deterministic code is arithmetic
verification of the formulas the AI reports.

Usage:
    python3 ai_extract/analyze_statement.py --ticker NVDA --accession 0001045810-25-000116 --statement income
"""

import argparse
import json
import os
import sys

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

import anthropic

from parse_xbrl import parse_filing


def clean_html(html):
    """Strip CSS styling and layout noise from iXBRL HTML. Keeps all tags, text, and ix: elements."""
    import re
    # Strip <style> blocks
    cleaned = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL)
    # Strip inline style attributes
    cleaned = re.sub(r'\s+style="[^"]*"', '', cleaned)
    # Collapse whitespace
    cleaned = re.sub(r'\s+', ' ', cleaned)
    return cleaned


def check_fact_completeness(ai_result, xbrl_facts):
    """Compare XBRL facts sent to the AI against what it reported.

    Returns a list of unaccounted XBRL concepts (concept names that appear
    in the facts but not in line_items or xbrl_not_on_statement).
    """
    # Collect all concepts the AI reported
    reported = set()
    for stmt in ['income_statement', 'balance_sheet', 'cash_flow']:
        stmt_data = ai_result.get(stmt, {})
        for item in stmt_data.get('line_items', []):
            c = item.get('xbrl_concept')
            if c:
                reported.add(c)
        for item in stmt_data.get('xbrl_not_on_statement', []):
            c = item.get('concept')
            if c:
                reported.add(c)

    # Collect all undimensioned numeric concepts from the XBRL facts we sent
    sent = {}
    for f in xbrl_facts:
        if f['value_numeric'] is not None:
            concept = f['concept']
            if concept.startswith('us-gaap:') or ':' in concept:
                # Skip dei:, ecd:, srt: non-financial concepts
                prefix = concept.split(':')[0]
                if prefix in ('dei', 'ecd', 'srt'):
                    continue
                if concept not in sent:
                    sent[concept] = []
                val = f['value_numeric']
                period = f.get('period', {})
                sent[concept].append({'value': val, 'period': period})

    # Find gaps
    unaccounted = {}
    for concept, facts in sent.items():
        if concept not in reported:
            unaccounted[concept] = facts

    return unaccounted


def extract_note_html_for_concepts(html, unaccounted_concepts, calculations):
    """Extract HTML sections containing unaccounted XBRL concepts.

    Uses calculation linkbase role names to find which note section
    each concept belongs to, then extracts that HTML section.
    """
    import re

    # Map concepts to cal linkbase roles
    concept_to_roles = {}
    for section in (calculations or []):
        role = section['role']
        for formula in section['formulas']:
            parent = formula['parent']
            concept_to_roles.setdefault(parent, set()).add(role)
            for child in formula['children']:
                concept_to_roles.setdefault(child['concept'], set()).add(role)

    # Find which roles are needed
    needed_roles = set()
    for concept in unaccounted_concepts:
        if concept in concept_to_roles:
            needed_roles.update(concept_to_roles[concept])

    # Also find concepts directly in the HTML by their ix:nonFraction tags
    sections = []
    for concept in unaccounted_concepts:
        escaped = re.escape(concept)
        pattern = rf'<ix:nonFraction[^>]*name="{escaped}"'
        matches = list(re.finditer(pattern, html))
        if matches:
            for m in matches:
                start = max(0, m.start() - 3000)
                end = min(len(html), m.start() + 3000)
                sections.append(html[start:end])

    if sections:
        # Deduplicate overlapping sections
        unique = []
        seen_starts = set()
        for s in sections:
            sig = s[:100]
            if sig not in seen_starts:
                seen_starts.add(sig)
                unique.append(s)
        return '\n\n'.join(unique)

    return None


def extract_statement_html(html, presentation):
    """Extract just the financial statement sections from the filing HTML.

    Uses ix:nonFraction tag positions to find contiguous clusters, then
    matches clusters to statements using presentation linkbase concepts.
    Returns stripped HTML containing only the three financial statements.
    """
    import re

    # Find all ix:nonFraction tags and their positions
    tags = [(m.start(), m.group(1))
            for m in re.finditer(r'<ix:nonFraction[^>]*name="([^"]+)"', html)]
    if not tags:
        return html  # fallback to full HTML

    # Group into clusters (gap > 5000 chars = new cluster)
    clusters = []
    current = [tags[0]]
    for i in range(1, len(tags)):
        if tags[i][0] - tags[i-1][0] > 5000:
            clusters.append(current)
            current = [tags[i]]
        else:
            current.append(tags[i])
    clusters.append(current)

    # Get statement-face concepts from presentation linkbase
    stmt_concepts = {}
    stmt_keywords = {
        'IS': 'StatementsofIncome',
        'BS': 'BalanceSheets',
        'CF': 'StatementsofCashFlows',
    }
    for label, keyword in stmt_keywords.items():
        for section in (presentation or []):
            if keyword in section['role']:
                concepts = set()
                for entry in section['structure']:
                    for child in entry['children']:
                        concepts.add(child)
                stmt_concepts[label] = concepts
                break

    # Score each cluster by overlap with statement concepts
    stmt_clusters = []
    for cluster in clusters:
        cluster_concepts = set(t[1] for t in cluster)
        for label, concepts in stmt_concepts.items():
            overlap = len(cluster_concepts & concepts)
            if overlap >= 3:  # at least 3 matching concepts
                start = cluster[0][0]
                end = cluster[-1][0]
                # Expand to include surrounding HTML (table boundaries)
                # Go back to find the start of the containing table/div
                search_start = max(0, start - 2000)
                search_end = min(len(html), end + 2000)
                stmt_clusters.append((label, search_start, search_end))
                break

    if not stmt_clusters:
        return html  # fallback

    # Extract the HTML sections
    sections = []
    for label, start, end in sorted(stmt_clusters, key=lambda x: x[1]):
        section_html = html[start:end]
        sections.append(f"<!-- {label} STATEMENT -->\n{section_html}")

    return '\n\n'.join(sections)


def extract_targeted_html(html, concepts_needed):
    """Extract HTML sections containing specific XBRL concepts for retry.

    Given a list of concept names that need resolution, finds the HTML
    sections containing those concepts' ix:nonFraction tags.
    """
    import re

    sections = []
    for concept in concepts_needed:
        # Find all occurrences of this concept in the HTML
        pattern = rf'<ix:nonFraction[^>]*name="{re.escape(concept)}"'
        matches = list(re.finditer(pattern, html))
        if matches:
            # Extract surrounding context (the table containing this tag)
            pos = matches[0].start()
            start = max(0, pos - 3000)
            end = min(len(html), pos + 3000)
            section = html[start:end]
            sections.append(f"<!-- Context for {concept} -->\n{section}")

    return '\n\n'.join(sections) if sections else None


def format_linkbase_for_prompt(calculations, presentation, definitions):
    """Format parsed linkbase data into readable prompt text."""
    sections = []

    # Calculation relationships
    if calculations:
        cal_lines = ["CALCULATION RELATIONSHIPS (declared by the company in this filing):"]
        for section in calculations:
            cal_lines.append(f"\n  {section['role']}:")
            for formula in section['formulas']:
                parent = formula['parent'].split(':')[-1]
                children_str = ' '.join(
                    ('+' if c['weight'] > 0 else '-') + c['concept'].split(':')[-1]
                    for c in formula['children']
                )
                cal_lines.append(f"    {parent} = {children_str}")
        sections.append('\n'.join(cal_lines))

    # Presentation structure — just the three financial statements
    if presentation:
        pres_lines = ["PRESENTATION STRUCTURE (which concepts belong on each statement, in display order):"]
        stmt_keywords = ['StatementsofIncome', 'BalanceSheets', 'StatementsofCashFlows']
        for section in presentation:
            if any(kw in section['role'] for kw in stmt_keywords):
                pres_lines.append(f"\n  {section['role']}:")
                for entry in section['structure']:
                    parent_short = entry['parent'].split(':')[-1]
                    children_short = [c.split(':')[-1] for c in entry['children']]
                    pres_lines.append(f"    {parent_short} -> {', '.join(children_short)}")
        sections.append('\n'.join(pres_lines))

    # Dimension hierarchies — segment/geography/product related
    if definitions:
        dim_lines = ["DIMENSION HIERARCHIES (segment, geography, product dimensions in this filing):"]
        segment_keywords = ['Segment', 'Geographic', 'Revenue', 'Product', 'Market']
        for section in definitions:
            if any(kw.lower() in section['role'].lower() for kw in segment_keywords):
                dim_lines.append(f"\n  {section['role']}:")
                for entry in section['hierarchies']:
                    parent_short = entry['parent'].split(':')[-1]
                    children_short = [c['concept'].split(':')[-1] for c in entry['children']]
                    dim_lines.append(f"    {parent_short}: {', '.join(children_short)}")
        if len(dim_lines) > 1:
            sections.append('\n'.join(dim_lines))

    return '\n\n'.join(sections)


def build_prompt(html_content, xbrl_facts, statement_type, meta, segment_facts=None,
                 calculations=None, presentation=None, definitions=None):
    """Build the prompt for Claude."""

    # Format XBRL facts as a readable list
    facts_text = "XBRL TAGGED FACTS (all undimensioned facts from this filing):\n"
    seen = set()
    for f in xbrl_facts:
        key = (f['concept'], f['context_ref'])
        if key in seen:
            continue
        seen.add(key)

        period_str = ''
        if f['period']:
            if f['period']['type'] == 'duration':
                period_str = f"{f['period']['startDate']} to {f['period']['endDate']}"
            else:
                period_str = f"as of {f['period']['date']}"

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

    if statement_type == 'income':
        task = """You are a data extraction tool. You have two inputs:
1. The full HTML filing (find the income statement within it)
2. The XBRL-tagged facts (the raw structured data)

Output ONLY valid JSON with this structure:

{
  "line_items": [
    {
      "label": "Revenue",
      "indent_level": 0,
      "xbrl_concept": "us-gaap:Revenues",
      "values": {"2025-01-27_2025-04-27": 44062, "2024-01-29_2024-04-28": 26044},
      "unit": "USD_millions",
      "xbrl_match": true,
      "mapping_reason": "HTML table row text is 'Revenue'. The ix:nonFraction tag on this cell has name='us-gaap:Revenues' with contextRef='c-1' and value 44062 (scale=6). XBRL fact list confirms us-gaap:Revenues = $44,062M for this period."
    }
  ],
  "formulas": [
    {
      "formula": "Revenue - Cost of Revenue = Gross Profit",
      "components": ["Revenue", "Cost of Revenue", "Gross Profit"],
      "operation": "Revenue - Cost of Revenue",
      "result_label": "Gross Profit"
    }
  ],
  "xbrl_not_on_statement": [
    {
      "concept": "us-gaap:DepreciationDepletionAndAmortization",
      "value": 611,
      "period": "2025-01-27_2025-04-27",
      "reason": "D&A is tagged in XBRL but does not appear as a line item on the income statement. Likely disclosed in notes or cash flow statement."
    }
  ]
}

Rules:
- line_items: every line item on the income statement, in presentation order.
- indent_level: hierarchy depth (0=top level like Revenue, Gross Profit; 1=sub-item like R&D under Operating Expenses).
- xbrl_concept: the XBRL concept name. Get this from the ix:nonFraction tag in the HTML, and cross-reference against the XBRL facts list.
- values: XBRL values are the source of truth for sign and magnitude. Use the XBRL fact value, not the HTML display value. HTML may show parentheses or negative signs for presentation, but the XBRL taxonomy defines the canonical sign convention (e.g., expenses are positive debits). When you find a matching XBRL fact, use its value directly. Key is period start_end date range.
- unit: one of "USD_millions", "USD_per_share", or "shares_millions".
- xbrl_match: did the value from the ix:nonFraction tag in the HTML match the corresponding XBRL fact? true/false. If false, explain in mapping_reason.
- mapping_reason: explain HOW you matched this line item to this XBRL concept. What did you see in the HTML? What tag confirmed it? Did the XBRL fact value match? This is for audit — be specific.
- formulas: every subtotal relationship on the statement. The operation must use the correct arithmetic operator (+ or -) to make the formula work with the XBRL sign convention. For example, if Interest Expense is 63 (positive in XBRL) and it reduces income, the formula is "Operating Income - Interest Expense", not "Operating Income + Interest Expense". Do NOT compute the math — the verification script will do that independently.
- xbrl_not_on_statement: review the XBRL facts list and identify concepts that are income-statement-related (revenue, expenses, income, gains, losses, SBC, D&A, etc.) but do NOT appear as line items on the rendered income statement. For each, give the concept, value, period, and a brief reason explaining where this item likely lives (notes, cash flow, etc.). This helps identify items that are aggregated or hidden.
- Do NOT include analysis, observations, or commentary.
- Do NOT verify the math yourself — just report what you found."""

    elif statement_type == 'balance_sheet':
        task = """You are a data extraction tool. You have two inputs:
1. The full HTML filing (find the balance sheet within it)
2. The XBRL-tagged facts (the raw structured data)

Output ONLY valid JSON with this structure:

{
  "line_items": [
    {
      "label": "Cash and cash equivalents",
      "indent_level": 1,
      "xbrl_concept": "us-gaap:CashAndCashEquivalentsAtCarryingValue",
      "values": {"2025-04-27": 15176, "2025-01-26": 8495},
      "unit": "USD_millions",
      "xbrl_match": true,
      "mapping_reason": "HTML row text is 'Cash and cash equivalents'. The ix:nonFraction tag has name='us-gaap:CashAndCashEquivalentsAtCarryingValue' with contextRef='c-4' (instant 2025-04-27) and value 15176. XBRL fact confirms."
    }
  ],
  "formulas": [
    {
      "formula": "Total current assets + Total non-current assets = Total assets",
      "components": ["Total current assets", "Total non-current assets", "Total assets"],
      "operation": "Total current assets + Total non-current assets",
      "result_label": "Total assets"
    }
  ],
  "xbrl_not_on_statement": [
    {
      "concept": "us-gaap:OperatingLeaseLiabilityCurrent",
      "value": 200,
      "period": "2025-04-27",
      "reason": "Current operating lease liabilities are tagged in XBRL but do not appear as a separate balance sheet line item. Likely included in 'Accrued and other current liabilities' and disclosed in notes."
    }
  ]
}

Rules:
- line_items: every line item on the balance sheet, in presentation order.
- indent_level: hierarchy depth (0=section headers like Total assets; 1=sub-section like Current assets items; 2=sub-sub-items).
- xbrl_concept: the XBRL concept name. Get this from the ix:nonFraction tag in the HTML, and cross-reference against the XBRL facts list.
- values: XBRL values are the source of truth. Balance sheet items are instant (point-in-time), so use the date as key (e.g., "2025-04-27"), not a date range. Report in millions as integers.
- unit: "USD_millions" for all balance sheet items.
- xbrl_match: did the value from the ix:nonFraction tag in the HTML match the corresponding XBRL fact? true/false.
- mapping_reason: explain HOW you matched this line item to this XBRL concept. Be specific about what you saw in the HTML and what tag confirmed it.
- formulas: every subtotal relationship on the balance sheet. Include at minimum:
  - Current asset components summing to Total current assets
  - Non-current asset components summing to Total non-current assets (if shown)
  - Total current assets + non-current assets = Total assets
  - Current liability components summing to Total current liabilities
  - Non-current liability components summing to Total non-current liabilities (if shown)
  - Total liabilities + Total stockholders' equity = Total liabilities and stockholders' equity
  - The fundamental equation: Total assets = Total liabilities and stockholders' equity
  Do NOT compute the math — the verification script will do that independently.
- xbrl_not_on_statement: review the XBRL facts list and identify concepts that are balance-sheet-related but do NOT appear as line items on the rendered balance sheet. These are items aggregated into "other" buckets — for example, operating lease liabilities inside "accrued liabilities", or specific receivable types inside a broader line. For each, give the concept, value, period, and a brief reason explaining where it likely lives. This is critical for identifying what's hidden in aggregated line items.
- Do NOT include analysis, observations, or commentary.
- Do NOT verify the math yourself — just report what you found."""

    elif statement_type == 'cash_flow':
        task = """You are a data extraction tool. You have two inputs:
1. The full HTML filing (find the cash flow statement within it)
2. The XBRL-tagged facts (the raw structured data)

Output ONLY valid JSON with this structure:

{
  "line_items": [
    {
      "label": "Net income",
      "indent_level": 0,
      "xbrl_concept": "us-gaap:NetIncomeLoss",
      "values": {"2025-01-27_2025-04-27": 18775, "2024-01-29_2024-04-28": 14881},
      "unit": "USD_millions",
      "xbrl_match": true,
      "mapping_reason": "HTML row text is 'Net income'. ix:nonFraction tag has name='us-gaap:NetIncomeLoss'. XBRL fact confirms."
    }
  ],
  "formulas": [
    {
      "formula": "Sum of CFO components = Net cash provided by operating activities",
      "components": ["Net income", "Stock-based compensation", "...other adjustments...", "Net cash provided by operating activities"],
      "operation": "Net income + Stock-based compensation + ...other adjustments...",
      "result_label": "Net cash provided by operating activities"
    }
  ],
  "xbrl_not_on_statement": [
    {
      "concept": "us-gaap:SomeConceptNotOnStatement",
      "value": 100,
      "period": "2025-01-27_2025-04-27",
      "reason": "Explanation of where this item lives."
    }
  ]
}

Rules:
- line_items: every line item on the cash flow statement, in presentation order. This includes:
  - Operating activities: Net income, all adjustments (D&A, SBC, deferred taxes, etc.), all working capital changes, and the CFO total.
  - Investing activities: all items and the CFI total.
  - Financing activities: all items and the CFF total.
  - Effect of exchange rate changes (if shown).
  - Net change in cash.
  - Beginning and ending cash balances.
- indent_level: hierarchy depth (0=section totals like Net cash from operating activities; 1=items within a section like D&A, SBC, working capital changes).
- xbrl_concept: the XBRL concept name from the ix:nonFraction tag, cross-referenced against the XBRL facts list.
- values: Use the values as presented on the cash flow statement — positive means source of cash, negative means use of cash. This applies to ALL items including working capital changes, investing items, and financing items. The sign as shown on the statement is what matters. Cash flow items use duration periods, so key is date range (e.g., "2025-01-27_2025-04-27"). Report in millions as integers.
- unit: "USD_millions" for all cash flow items.
- xbrl_match: did the ix:nonFraction tag value match the XBRL fact? true/false.
- mapping_reason: explain HOW you matched this line item. Be specific about what HTML text and XBRL tag confirmed it.
- formulas: every subtotal relationship. The "operation" field MUST use exact label names from line_items connected by + ONLY. NEVER use minus signs in the operation.
  WHY: The values already contain the correct sign. For example, Accounts receivable = -933 means AR increased and used cash. The negative is IN the value. Purchases of marketable securities = -6546 means cash was spent. The negative is IN the value. Dividends paid = -244. The negative is IN the value. So the formula just adds all signed values: "Net income + SBC + D&A + ... + Accounts receivable + Inventories + ..." and the math works because -933 + 1258 + ... naturally nets out. If you write "- Accounts receivable" you are negating -933 to get +933, which is wrong.
  Include at minimum:
  - All CFO components summed = CFO total (all +, no -)
  - All CFI components summed = CFI total (all +, no -)
  - All CFF components summed = CFF total (all +, no -)
  - CFO + CFI + CFF = Net change in cash
  - Beginning cash + Net change = Ending cash (use the EXACT label names from line_items)
  Do NOT compute the math — the verification script will do that independently.
- xbrl_not_on_statement: XBRL facts that are cash-flow-related but do NOT appear as line items on the rendered statement. These may be sub-components disclosed in notes.
- Do NOT include analysis, observations, or commentary.
- Do NOT verify the math yourself — just report what you found."""

    elif statement_type == 'all':
        # Format linkbase data for prompt
        linkbase_text = ""
        if calculations or presentation or definitions:
            linkbase_text = format_linkbase_for_prompt(
                calculations or [], presentation or [], definitions or [])

        task = f"""You are a financial data extraction and verification tool. You have three inputs:
1. The full HTML filing (contains all three financial statements and notes)
2. The XBRL-tagged facts (the raw structured data)
3. The XBRL linkbase data (calculation relationships, presentation structure, and dimension hierarchies declared by the company in this filing)

{linkbase_text}

Output ONLY valid JSON with this structure:

{{
  "income_statement": {{
    "line_items": [...],
    "formulas": [...],
    "xbrl_not_on_statement": [...]
  }},
  "balance_sheet": {{
    "line_items": [...],
    "formulas": [...],
    "xbrl_not_on_statement": [...]
  }},
  "cash_flow": {{
    "line_items": [...],
    "formulas": [...],
    "xbrl_not_on_statement": [...]
  }},
  "cross_statement_checks": [...],
  "segment_data": [...]
}}

Each statement section follows the same format:

line_items: every line item on the statement, in presentation order.
- label: the line item text as shown in the filing
- indent_level: hierarchy depth (0=top level/totals, 1=sub-items, 2=sub-sub-items)
- xbrl_concept: the XBRL concept name from the ix:nonFraction tag, cross-referenced against XBRL facts
- values: use the PRECISE value from the HTML filing, not the XBRL fact value. XBRL facts may be rounded (e.g. decimals="-8" rounds to hundreds of millions). The HTML shows the exact reported number. Convert to millions as integers. Key format: for duration items use "startDate_endDate", for instant items use just the date.
- unit: one of "USD_millions", "USD_per_share", or "shares_millions"
- xbrl_match: did the ix:nonFraction tag value match the XBRL fact? true/false. If the XBRL fact is rounded to a different precision than the HTML display, note this in mapping_reason but still set to true.
- mapping_reason: explain HOW you matched this line item to the XBRL concept. Be specific about HTML text and tags.

formulas: every calculation relationship from the CALCULATION RELATIONSHIPS section above that applies to this statement. For each declared formula, report it using the exact line item labels from your line_items. Structure:
  {{"formula": "human readable description", "components": ["Label A", "Label B", "Label C"], "operation": "Label A + Label B", "result_label": "Label C"}}
  The "operation" field MUST be a math expression using exact label names from line_items connected by + and - operators. Use the weights from the calculation relationships to determine the sign (weight +1.0 = add, weight -1.0 = subtract). The verification script evaluates the operation string by substituting label names with values.

xbrl_not_on_statement: for every XBRL fact related to this statement that does NOT appear as a line item on the statement face, report the concept, value, period, and where it is classified. Nothing unaccounted for — every fact must be placed.

cross_statement_checks: identify every value that appears on more than one statement and verify they match. Report each as:
  {{"check": "description", "statement_1": "IS/BS/CF", "value_1": X, "statement_2": "IS/BS/CF", "value_2": X, "match": true/false}}

segment_data: extract all revenue and operating income disaggregation from the dimensioned XBRL facts. Use the DIMENSION HIERARCHIES above to identify what breakdowns exist. For each breakdown, report:
  {{"dimension": "axis name", "items": [{{"member": "member name", "values": {{"period": value}}}}], "total": X, "consolidated_total": X, "ties": true/false}}
  Each breakdown total MUST equal the corresponding consolidated total from the financial statements. If it does not, flag the discrepancy. Extract ALL periods reported (current + comparatives).

Rules:
- Extract ALL periods reported in the filing (current + comparatives).
- Do NOT verify the math yourself — just report what you found. The verification script checks independently.
- Do NOT include analysis, observations, or commentary.
- CRITICAL: Output must be valid JSON. In ALL string values, never use apostrophes or single quotes. Use "shareholders equity" not "shareholders' equity". Use "does not" not "doesn't". This applies to labels, mapping_reason, and all other string fields."""

    # Format dimensioned XBRL facts
    segment_text = ""
    if segment_facts:
        segment_text = "\n\nDIMENSIONED XBRL FACTS (segment, geography, product, and other dimensioned facts):\n"
        seen = set()
        for f in segment_facts:
            key = (f['concept'], f['context_ref'])
            if key in seen:
                continue
            seen.add(key)

            period_str = ''
            if f['period']:
                if f['period']['type'] == 'duration':
                    period_str = f"{f['period']['startDate']} to {f['period']['endDate']}"
                else:
                    period_str = f"as of {f['period']['date']}"

            dims_str = ', '.join(f"{d['dimension']}={d['member']}" for d in f.get('dimensions', []))
            val = f['value_raw']
            unit = f['unit'] or ''
            if f['value_numeric'] is not None:
                if 'USD' in unit and 'shares' not in unit:
                    val = f"${f['value_numeric']/1e6:,.0f}M"
                elif f['value_numeric'] is not None and isinstance(f['value_numeric'], float) and f['value_numeric'] < 1:
                    val = f"{f['value_numeric']:.2%}"

            segment_text += f"  {f['concept']} [{period_str}] ({dims_str}) = {val}\n"

    prompt = f"""You are analyzing a {meta['form']} filing for {meta['ticker']}.
Report date: {meta['report_date']}. Filing date: {meta['filing_date']}.

{task}

---
{facts_text}{segment_text}
---
FULL FILING HTML:
{html_content}
"""
    return prompt


def verify_formulas(ai_result):
    """
    Pure arithmetic verification of the formulas the AI reported.
    No XBRL lookups, no concept matching — just math on the AI's own numbers.
    """
    line_items = ai_result.get('line_items', [])
    formulas = ai_result.get('formulas', [])

    # Build lookup: label -> values
    label_values = {}
    for item in line_items:
        label_values[item['label']] = item.get('values', {})

    formula_checks = []
    for formula in formulas:
        operation = formula.get('operation', '')
        result_label = formula.get('result_label', '')

        result_values = label_values.get(result_label, {})
        periods_checked = {}
        all_pass = True

        for period in result_values:
            stated = result_values[period]
            computed = _evaluate_formula(operation, label_values, period)
            match = computed is not None and computed == stated
            if not match:
                all_pass = False
            periods_checked[period] = {
                'computed': computed,
                'stated': stated,
                'computation': _format_computation(operation, label_values, period),
            }

        formula_checks.append({
            'formula': formula.get('formula', ''),
            'pass': all_pass,
            'periods': periods_checked,
        })

    formulas_pass = sum(1 for f in formula_checks if f['pass'])
    return {
        'formula_checks': formula_checks,
        'formulas_pass': formulas_pass,
        'formulas_total': len(formula_checks),
    }


def _evaluate_formula(operation, label_values, period):
    """Evaluate a formula string like 'Revenue - Cost of Revenue' using extracted values."""
    try:
        expr = operation
        labels_by_length = sorted(label_values.keys(), key=len, reverse=True)
        for label in labels_by_length:
            if label in expr:
                val = label_values.get(label, {}).get(period)
                if val is None:
                    return None
                expr = expr.replace(label, str(val))
        result = eval(expr)
        return int(result) if isinstance(result, float) and result == int(result) else result
    except Exception:
        return None


def _format_computation(operation, label_values, period):
    """Format a computation showing the actual numbers."""
    expr = operation
    labels_by_length = sorted(label_values.keys(), key=len, reverse=True)
    for label in labels_by_length:
        if label in expr:
            val = label_values.get(label, {}).get(period, '?')
            expr = expr.replace(label, f"{val}")
    return expr


def main():
    parser = argparse.ArgumentParser(description='AI-powered financial statement analysis')
    parser.add_argument('--ticker', required=True)
    parser.add_argument('--accession', required=True)
    parser.add_argument('--statement', default='income', choices=['income', 'balance_sheet', 'cash_flow', 'all'])
    parser.add_argument('--output', help='Save output to file')
    parser.add_argument('--model', default='claude-sonnet-4-6', help='Model to use (claude-sonnet-4-6, claude-opus-4-6, gemini-3-flash-preview, gpt-5, etc.)')
    parser.add_argument('--test', action='store_true', help='Write output to test/ subdirectory instead of main directory')
    parser.add_argument('--full-html', action='store_true', help='Send full HTML instead of stripped statements (auto-selected for small filings)')
    args = parser.parse_args()

    # Step 1: Parse XBRL facts
    print(f"Parsing XBRL facts for {args.ticker} / {args.accession}...")
    parsed = parse_filing(args.ticker, args.accession)
    print(f"  {parsed['total_facts']} total facts, {parsed['total_contexts']} contexts")

    # Step 2: Load full HTML
    base_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'filings', args.ticker, args.accession)
    meta_path = os.path.join(base_dir, 'filing_meta.json')
    with open(meta_path) as f:
        meta = json.load(f)

    html_path = os.path.join(base_dir, meta['html_filename'])
    with open(html_path) as f:
        html_content = f.read()
    html_cleaned = clean_html(html_content)
    print(f"  Full HTML: {len(html_content):,} chars -> cleaned: {len(html_cleaned):,} chars (~{len(html_cleaned)//4:,} tokens)")

    # Decide HTML strategy based on size
    # Full HTML under 500K tokens total prompt → send everything, no retry needed
    # Over 500K → strip to statements, use completeness retry for notes
    html_statements = extract_statement_html(html_content, parsed.get('presentation', []))
    html_statements = clean_html(html_statements)

    # Estimate total prompt with full HTML vs stripped
    full_html_tokens = len(html_cleaned) // 4
    stripped_tokens = len(html_statements) // 4
    use_full_html = args.full_html or full_html_tokens < 150000  # ~150K token threshold for HTML portion

    if use_full_html:
        html_for_prompt = html_cleaned
        print(f"  Using FULL HTML: {len(html_cleaned):,} chars (~{full_html_tokens:,} tokens)")
    else:
        html_for_prompt = html_statements
        print(f"  Using STRIPPED HTML: {len(html_statements):,} chars (~{stripped_tokens:,} tokens) [full would be ~{full_html_tokens:,} tokens]")

    # Step 3: Filter XBRL facts
    xbrl_facts = [f for f in parsed['facts'] if not f['dimensioned']]
    print(f"  {len(xbrl_facts)} undimensioned XBRL facts")

    # Include all dimensioned facts for segment extraction
    dim_facts = [f for f in parsed['facts'] if f['dimensioned']]
    print(f"  {len(dim_facts)} dimensioned XBRL facts")

    # Linkbase data
    calculations = parsed.get('calculations', [])
    presentation = parsed.get('presentation', [])
    definitions = parsed.get('definitions', [])
    if calculations:
        total_formulas = sum(len(s['formulas']) for s in calculations)
        print(f"  {len(calculations)} calculation sections, {total_formulas} formulas")
    if presentation:
        print(f"  {len(presentation)} presentation sections")
    if definitions:
        print(f"  {len(definitions)} definition sections")

    # Step 4: Build prompt and call model
    prompt = build_prompt(html_for_prompt, xbrl_facts, args.statement, meta, dim_facts,
                          calculations, presentation, definitions)
    print(f"\nTotal prompt size: ~{len(prompt)//4} tokens")
    print(f"Sending to {args.model}...\n")

    output_text = ""
    input_tokens = 0
    output_tokens = 0

    if args.model.startswith('gemini'):
        from google import genai
        gemini_client = genai.Client(api_key=os.environ.get('GEMINI_API_KEY'))
        response = gemini_client.models.generate_content(
            model=args.model,
            contents=prompt,
            config=genai.types.GenerateContentConfig(max_output_tokens=32768),
        )
        output_text = response.text
        input_tokens = response.usage_metadata.prompt_token_count
        output_tokens = response.usage_metadata.candidates_token_count
    elif args.model.startswith('gpt'):
        from openai import OpenAI
        oai_client = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
        response = oai_client.chat.completions.create(
            model=args.model,
            max_completion_tokens=32768,
            messages=[{"role": "user", "content": prompt}],
        )
        output_text = response.choices[0].message.content
        input_tokens = response.usage.prompt_tokens
        output_tokens = response.usage.completion_tokens
    else:
        client = anthropic.Anthropic()
        with client.messages.stream(
            model=args.model,
            max_tokens=32768,
            messages=[{"role": "user", "content": prompt}],
        ) as stream:
            for text in stream.text_stream:
                output_text += text
                print(".", end="", flush=True)
            print()
            resp = stream.get_final_message()
            input_tokens = resp.usage.input_tokens
            output_tokens = resp.usage.output_tokens

    # Parse the JSON from the AI response
    json_text = output_text.strip()
    first_brace = json_text.find('{')
    last_brace = json_text.rfind('}')
    if first_brace != -1 and last_brace != -1:
        json_text = json_text[first_brace:last_brace + 1]

    try:
        ai_result = json.loads(json_text)
    except json.JSONDecodeError:
        # Try fixing common issues: smart quotes, unescaped control chars
        import re
        fixed = json_text
        fixed = fixed.replace('\u2018', "'").replace('\u2019', "'")  # smart single quotes
        fixed = fixed.replace('\u201c', '"').replace('\u201d', '"')  # smart double quotes
        fixed = re.sub(r'[\x00-\x1f]', ' ', fixed)  # control characters
        try:
            ai_result = json.loads(fixed)
        except json.JSONDecodeError:
            ai_result = None

    if ai_result is None:
        # Last resort: try to repair by finding valid JSON subset
        print("WARNING: Attempting JSON repair...")
        import re
        repaired = re.sub(r',\s*([}\]])', r'\1', json_text)
        try:
            ai_result = json.loads(repaired)
        except json.JSONDecodeError as e:
            print(f"ERROR: AI returned invalid JSON: {e}")
            print("Raw output:")
            print(output_text)
            sys.exit(1)

    # === DETERMINISTIC VERIFICATION ===
    if args.statement == 'all':
        # Verify each statement separately
        all_verification = {}
        total_formulas_pass = 0
        total_formulas = 0
        total_xbrl_match = 0
        total_xbrl = 0

        for stmt_name in ['income_statement', 'balance_sheet', 'cash_flow']:
            stmt_data = ai_result.get(stmt_name, {})
            v = verify_formulas(stmt_data)
            all_verification[stmt_name] = v
            total_formulas_pass += v['formulas_pass']
            total_formulas += v['formulas_total']
            total_xbrl_match += sum(1 for i in stmt_data.get('line_items', []) if i.get('xbrl_match') is True)
            total_xbrl += sum(1 for i in stmt_data.get('line_items', []) if i.get('xbrl_match') is not None)

        # === CF SECTION RETRY: if any section (CFO/CFI/CFF) doesn't balance ===
        cf_v = all_verification.get('cash_flow', {})
        cf_items = ai_result.get('cash_flow', {}).get('line_items', [])

        # Define the three sections to check
        cf_sections = [
            {
                'name': 'CFO',
                'total_concept': 'NetCashProvidedByUsedInOperatingActivities',
                'description': 'operating activity',
                'boundary': 'between Net income and Net cash provided by operating activities',
                'result_key': 'cfo_retry',
            },
            {
                'name': 'CFI',
                'total_concept': 'NetCashProvidedByUsedInInvestingActivities',
                'description': 'investing activity',
                'boundary': 'in the investing activities section',
                'result_key': 'cfi_retry',
            },
            {
                'name': 'CFF',
                'total_concept': 'NetCashProvidedByUsedInFinancingActivities',
                'description': 'financing activity',
                'boundary': 'in the financing activities section',
                'result_key': 'cff_retry',
            },
        ]

        for section in cf_sections:
            # Check if this section's formula failed
            section_needs_retry = False
            for check in cf_v.get('formula_checks', []):
                if not check['pass']:
                    for period, detail in check['periods'].items():
                        if detail['computed'] is not None and detail['stated'] is not None and detail['computed'] != detail['stated']:
                            section_needs_retry = True
                            break
                if section_needs_retry:
                    break

            if not section_needs_retry:
                continue

            # Find the section total
            section_total_vals = {}
            for item in cf_items:
                concept = item.get('xbrl_concept') or ''
                if section['total_concept'] in concept:
                    section_total_vals = item.get('values') or {}
                    break

            if not section_total_vals:
                continue

            # Build component summary
            component_summary = []
            for item in cf_items:
                vals = item.get('values') or {}
                if not vals:
                    continue
                first_val = list(vals.values())[0]
                component_summary.append(f"  {item['label']}: {first_val}")

            component_text = "\n".join(component_summary)
            first_total = list(section_total_vals.values())[0]

            retry_prompt = f"""You extracted cash flow {section['description']} components for {args.ticker} but they do not sum to the stated {section['name']} total.

Your extracted components (full CF statement):
{component_text}

Stated {section['name']} total: {first_total}

The components must sum EXACTLY to {section['name']}. Rules:
- ALL values already contain the correct sign (positive = source of cash, negative = use of cash)
- ALL components added together (using +) must equal the section total
- Search the XBRL facts for ANY concept you may have missed — companies sometimes use numbered variants (e.g., PaymentsToAcquireBusinessTwoNetOfCashAcquired) or custom extension concepts for individual transactions

Go back to the cash flow statement in the filing and:
1. List EVERY line item {section['boundary']}
2. Include the value with correct sign
3. Also search the XBRL facts list for any {section['description']}-related concepts not on the statement face
4. Verify the sum equals {section['name']} exactly

Output ONLY valid JSON:
{{"corrected_components": [{{"label": "...", "value": X, "xbrl_concept": "..."}}], "section_total": X, "sum_check": X, "items_added": ["list of items that were missing from original extraction"], "items_corrected": ["list of items with sign changes"]}}

CRITICAL: Output must be valid JSON. No apostrophes in strings."""

            print(f"{section['name']} formula mismatch detected — retrying to find missing components...")

            # Call the API
            retry_text = ""
            retry_in = 0
            retry_out = 0
            if args.model.startswith('gemini'):
                from google import genai
                gemini_client = genai.Client(api_key=os.environ.get('GEMINI_API_KEY'))
                retry_resp = gemini_client.models.generate_content(
                    model=args.model,
                    contents=retry_prompt + "\n\nFILING HTML:\n" + html_cleaned,
                    config=genai.types.GenerateContentConfig(max_output_tokens=8192),
                )
                retry_text = retry_resp.text
                retry_in = retry_resp.usage_metadata.prompt_token_count
                retry_out = retry_resp.usage_metadata.candidates_token_count
            elif args.model.startswith('gpt'):
                from openai import OpenAI
                oai_client = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
                retry_resp = oai_client.chat.completions.create(
                    model=args.model,
                    max_completion_tokens=8192,
                    messages=[{"role": "user", "content": retry_prompt + "\n\nFILING HTML:\n" + html_cleaned}],
                )
                retry_text = retry_resp.choices[0].message.content
                retry_in = retry_resp.usage.prompt_tokens
                retry_out = retry_resp.usage.completion_tokens
            else:
                client = anthropic.Anthropic()
                with client.messages.stream(
                    model=args.model,
                    max_tokens=8192,
                    messages=[{"role": "user", "content": retry_prompt + "\n\nFILING HTML:\n" + html_cleaned}],
                ) as stream:
                    for text in stream.text_stream:
                        retry_text += text
                        print(".", end="", flush=True)
                    print()
                    resp = stream.get_final_message()
                    retry_in = resp.usage.input_tokens
                    retry_out = resp.usage.output_tokens

            input_tokens += retry_in
            output_tokens += retry_out

            # Parse retry result
            retry_json = retry_text.strip()
            fb = retry_json.find('{')
            lb = retry_json.rfind('}')
            if fb != -1 and lb != -1:
                retry_json = retry_json[fb:lb + 1]
            try:
                retry_result = json.loads(retry_json)
            except json.JSONDecodeError:
                import re
                retry_json = retry_json.replace('\u2018', "'").replace('\u2019', "'")
                retry_json = re.sub(r'[\x00-\x1f]', ' ', retry_json)
                retry_json = re.sub(r',\s*([}\]])', r'\1', retry_json)
                try:
                    retry_result = json.loads(retry_json)
                except json.JSONDecodeError:
                    retry_result = None

            if retry_result:
                ai_result[section['result_key']] = retry_result
                print(f"{section['name']} retry: sum_check={retry_result.get('sum_check')}, total={retry_result.get('section_total')}")
                if retry_result.get('items_added'):
                    print(f"  Items added: {retry_result['items_added']}")
                if retry_result.get('items_corrected'):
                    print(f"  Items corrected: {retry_result['items_corrected']}")

                corrected = retry_result.get('corrected_components', [])
                if corrected:
                    corrected_sum = sum(c.get('value', 0) for c in corrected)
                    section_total = retry_result.get('section_total', 0)
                    if corrected_sum == section_total:
                        print(f"  VERIFIED: corrected components sum to {section['name']} ({corrected_sum} = {section_total})")

                        # Apply corrected values back to cash_flow.line_items
                        corrected_by_label = {c['label']: c['value'] for c in corrected}
                        cf_line_items = ai_result.get('cash_flow', {}).get('line_items', [])
                        applied = 0
                        for item in cf_line_items:
                            if item['label'] in corrected_by_label:
                                old_vals = item.get('values', {})
                                for period in old_vals:
                                    old_val = old_vals[period]
                                    new_val = corrected_by_label[item['label']]
                                    if old_val != new_val:
                                        old_vals[period] = new_val
                                        applied += 1
                        if applied > 0:
                            print(f"  Applied {applied} corrected values to cash_flow.line_items")
                    else:
                        print(f"  STILL MISMATCHED: {corrected_sum} vs {section_total}")
                        ai_result[section['result_key'] + '_unresolved'] = True

        # === XBRL FACT COMPLETENESS CHECK ===
        unaccounted = check_fact_completeness(ai_result, xbrl_facts)
        print(f"\nXBRL fact completeness: {len(xbrl_facts) - len(unaccounted)} accounted, {len(unaccounted)} unaccounted")

        if unaccounted and use_full_html:
            # Already sent full HTML — AI had everything and still missed these.
            # Log them but don't retry with the same context.
            print(f"  Full HTML was sent — logging {len(unaccounted)} unaccounted facts (no retry)")
            ai_result['unaccounted_facts'] = list(unaccounted.keys())

        elif unaccounted:
            # Extract targeted HTML sections for the missing concepts
            note_html = extract_note_html_for_concepts(html_content, unaccounted.keys(), calculations)
            note_html_cleaned = clean_html(note_html) if note_html else None

            if note_html_cleaned:
                print(f"  Targeted notes HTML: {len(note_html_cleaned):,} chars (~{len(note_html_cleaned)//4:,} tokens)")

                # Build retry prompt with the missing concepts and targeted HTML
                missing_list = []
                for concept, facts in sorted(unaccounted.items()):
                    vals = []
                    for f in facts[:2]:  # show up to 2 values per concept
                        v = f['value']
                        p = f['period']
                        if p.get('type') == 'instant':
                            vals.append(f"{v} as of {p.get('date')}")
                        elif p.get('type') == 'duration':
                            vals.append(f"{v} for {p.get('startDate')} to {p.get('endDate')}")
                    missing_list.append(f"  {concept}: {'; '.join(vals)}")

                missing_text = '\n'.join(missing_list)

                completeness_prompt = f"""You previously extracted financial statements for {meta['ticker']} but did not account for the following XBRL facts.

For each fact below, determine where it belongs:
- Which financial statement or note section is it related to?
- Is it a sub-component of a line item already on the statement face?
- Where is it classified (e.g., "included in Accrued and other current liabilities")?

UNACCOUNTED XBRL FACTS:
{missing_text}

RELEVANT FILING HTML (notes sections):
{note_html_cleaned}

Output ONLY valid JSON — a list of items:
[
  {{
    "concept": "us-gaap:ConceptName",
    "value": {{"period_key": value}},
    "statement": "income_statement/balance_sheet/cash_flow/notes",
    "classified_in": "description of where this item lives",
    "reason": "brief explanation"
  }}
]

CRITICAL: Output must be valid JSON. No apostrophes in strings."""

                print(f"  Completeness retry: {len(unaccounted)} concepts, sending targeted HTML...")

                retry_text = ""
                if args.model.startswith('gemini'):
                    from google import genai
                    gemini_client = genai.Client(api_key=os.environ.get('GEMINI_API_KEY'))
                    retry_resp = gemini_client.models.generate_content(
                        model=args.model,
                        contents=completeness_prompt,
                        config=genai.types.GenerateContentConfig(max_output_tokens=16384),
                    )
                    retry_text = retry_resp.text
                    input_tokens += retry_resp.usage_metadata.prompt_token_count
                    output_tokens += retry_resp.usage_metadata.candidates_token_count
                elif args.model.startswith('gpt'):
                    from openai import OpenAI
                    oai_client = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
                    retry_resp = oai_client.chat.completions.create(
                        model=args.model,
                        max_completion_tokens=16384,
                        messages=[{"role": "user", "content": completeness_prompt}],
                    )
                    retry_text = retry_resp.choices[0].message.content
                    input_tokens += retry_resp.usage.prompt_tokens
                    output_tokens += retry_resp.usage.completion_tokens
                else:
                    client = anthropic.Anthropic()
                    with client.messages.stream(
                        model=args.model,
                        max_tokens=16384,
                        messages=[{"role": "user", "content": completeness_prompt}],
                    ) as stream:
                        for text in stream.text_stream:
                            retry_text += text
                            print(".", end="", flush=True)
                        print()
                        resp = stream.get_final_message()
                        input_tokens += resp.usage.input_tokens
                        output_tokens += resp.usage.output_tokens

                # Parse retry result
                retry_json = retry_text.strip()
                fb = retry_json.find('[')
                lb = retry_json.rfind(']')
                if fb != -1 and lb != -1:
                    retry_json = retry_json[fb:lb + 1]
                try:
                    completeness_items = json.loads(retry_json)
                except json.JSONDecodeError:
                    import re as re2
                    retry_json = retry_json.replace('\u2018', "'").replace('\u2019', "'")
                    retry_json = re2.sub(r'[\x00-\x1f]', ' ', retry_json)
                    retry_json = re2.sub(r',\s*([}\]])', r'\1', retry_json)
                    try:
                        completeness_items = json.loads(retry_json)
                    except json.JSONDecodeError:
                        completeness_items = None

                if completeness_items and isinstance(completeness_items, list):
                    # Merge into xbrl_not_on_statement for the appropriate statements
                    added = 0
                    for item in completeness_items:
                        stmt = item.get('statement', 'notes')
                        concept = item.get('concept', '')
                        if stmt in ('income_statement', 'balance_sheet', 'cash_flow'):
                            not_on = ai_result.get(stmt, {}).setdefault('xbrl_not_on_statement', [])
                            not_on.append({
                                'concept': concept,
                                'value': item.get('value'),
                                'period': 'see value keys',
                                'reason': item.get('classified_in', '') + ' — ' + item.get('reason', ''),
                            })
                            added += 1
                        else:
                            # Notes-only items — add to balance_sheet xbrl_not_on_statement as default
                            not_on = ai_result.get('balance_sheet', {}).setdefault('xbrl_not_on_statement', [])
                            not_on.append({
                                'concept': concept,
                                'value': item.get('value'),
                                'period': 'see value keys',
                                'reason': item.get('classified_in', '') + ' — ' + item.get('reason', ''),
                            })
                            added += 1

                    print(f"  Completeness retry: added {added} items to xbrl_not_on_statement")

                    # Store the completeness results for audit
                    ai_result['completeness_retry'] = completeness_items
                else:
                    print("  Completeness retry: failed to parse response")
            else:
                print(f"  No HTML found for {len(unaccounted)} unaccounted concepts (not in filing HTML)")

        # Display each statement
        print("=" * 80)
        print(f"AI EXTRACTION: ALL STATEMENTS")
        print(f"{args.ticker} | {meta['form']} | {meta['report_date']}")
        print("=" * 80)

        for stmt_name, stmt_title in [('income_statement', 'INCOME STATEMENT'), ('balance_sheet', 'BALANCE SHEET'), ('cash_flow', 'CASH FLOW')]:
            stmt_data = ai_result.get(stmt_name, {})
            v = all_verification[stmt_name]

            print(f"\n{'=' * 40}")
            print(f"  {stmt_title}")
            print(f"{'=' * 40}")

            print("\n--- LINE ITEMS ---\n")
            for item in stmt_data.get('line_items', []):
                indent = "  " * item.get('indent_level', 0)
                vals = item.get('values') or {}
                val_str = " | ".join(f"{v}" for v in vals.values())
                xbrl_ok = "✓" if item.get('xbrl_match') else "✗"
                print(f"{indent}{item['label']}: {val_str}  {xbrl_ok}")

            print("\n--- FORMULAS ---\n")
            for check in v['formula_checks']:
                status = "PASS" if check['pass'] else "FAIL"
                print(f"[{status}] {check['formula']}")
                for period, detail in check['periods'].items():
                    print(f"  {detail['computation']} = {detail['computed']} vs {detail['stated']}")

            not_on = stmt_data.get('xbrl_not_on_statement', [])
            if not_on:
                print("\n--- NOT ON STATEMENT ---\n")
                for item in not_on:
                    print(f"  {item.get('concept')}: {item.get('value')} [{item.get('period')}]")
                    print(f"    {item.get('reason', '')}")

        # Cross-statement checks
        cross = ai_result.get('cross_statement_checks', [])
        if cross:
            print(f"\n{'=' * 40}")
            print(f"  CROSS-STATEMENT CHECKS")
            print(f"{'=' * 40}\n")
            for check in cross:
                status = "✓" if check.get('match') else "✗"
                print(f"  {status} {check.get('check')}")
                for k, v in check.items():
                    if k not in ('check', 'match'):
                        print(f"      {k}: {v}")

        # Calculation components
        calc = ai_result.get('calculation_components', {})
        if calc:
            print(f"\n{'=' * 40}")
            print(f"  CALCULATION COMPONENTS")
            print(f"{'=' * 40}\n")
            for comp_name, comp_data in calc.items():
                print(f"  {comp_name}:")
                if isinstance(comp_data, dict):
                    for k, v in comp_data.items():
                        print(f"    {k}: {v}")
                else:
                    print(f"    {comp_data}")
                print()

        print(f"\n{'=' * 80}")
        print(f"Tokens: {input_tokens} in, {output_tokens} out")
        if args.model.startswith('gpt-5.4'):
            in_rate, out_rate = 2.5, 15.0
        elif args.model.startswith('gpt-5'):
            in_rate, out_rate = 0.63, 5.0
        elif args.model.startswith('gemini-3') and 'flash' in args.model:
            in_rate, out_rate = 0.5, 3.0
        elif args.model.startswith('gemini'):
            in_rate, out_rate = 0.3, 2.5
        elif 'opus' in args.model:
            in_rate, out_rate = 15.0, 75.0
        else:
            in_rate, out_rate = 3.0, 15.0  # Sonnet default
        input_cost = input_tokens * in_rate / 1_000_000
        output_cost = output_tokens * out_rate / 1_000_000
        print(f"Cost: ${input_cost:.2f} input + ${output_cost:.2f} output = ${input_cost + output_cost:.2f}")
        print(f"Formulas: {total_formulas_pass}/{total_formulas} pass")
        print(f"XBRL matches (AI-reported): {total_xbrl_match}/{total_xbrl}")
        print("=" * 80)

        full_output = {
            'ai_extraction': ai_result,
            'formula_verification': all_verification,
        }
    else:
        verification = verify_formulas(ai_result)

        # Display
        print("=" * 80)
        print(f"AI EXTRACTION: {args.statement.upper()} STATEMENT")
        print(f"{args.ticker} | {meta['form']} | {meta['report_date']}")
        print("=" * 80)

        print("\n--- LINE ITEMS (from AI) ---\n")
        for item in ai_result.get('line_items', []):
            indent = "  " * item.get('indent_level', 0)
            vals = item.get('values', {})
            val_str = " | ".join(f"{v}" for v in vals.values())
            unit = item.get('unit', '')
            xbrl_ok = item.get('xbrl_match', '?')
            print(f"{indent}{item['label']}: {val_str}  [{unit}] xbrl_match={xbrl_ok}")
            print(f"{indent}  concept: {item.get('xbrl_concept', '?')}")
            print(f"{indent}  reason: {item.get('mapping_reason', 'none given')}")
            print()

        print("--- FORMULA VERIFICATION (deterministic arithmetic) ---\n")
        for check in verification['formula_checks']:
            status = "PASS" if check['pass'] else "FAIL"
            print(f"[{status}] {check['formula']}")
            for period, detail in check['periods'].items():
                print(f"  {period}: {detail['computation']} = {detail['computed']} vs stated {detail['stated']}")
            print()

        not_on_stmt = ai_result.get('xbrl_not_on_statement', [])
        if not_on_stmt:
            print("--- XBRL FACTS NOT ON STATEMENT (from AI) ---\n")
            for item in not_on_stmt:
                print(f"  {item.get('concept')}: {item.get('value')} [{item.get('period')}]")
                print(f"    {item.get('reason', '')}")
                print()

        print("=" * 80)
        print(f"Tokens: {input_tokens} in, {output_tokens} out")
        if args.model.startswith('gpt-5.4'):
            in_rate, out_rate = 2.5, 15.0
        elif args.model.startswith('gpt-5'):
            in_rate, out_rate = 0.63, 5.0
        elif args.model.startswith('gemini-3') and 'flash' in args.model:
            in_rate, out_rate = 0.5, 3.0
        elif args.model.startswith('gemini'):
            in_rate, out_rate = 0.3, 2.5
        elif 'opus' in args.model:
            in_rate, out_rate = 15.0, 75.0
        else:
            in_rate, out_rate = 3.0, 15.0  # Sonnet default
        input_cost = input_tokens * in_rate / 1_000_000
        output_cost = output_tokens * out_rate / 1_000_000
        print(f"Cost: ${input_cost:.2f} input + ${output_cost:.2f} output = ${input_cost + output_cost:.2f}")
        v = verification
        print(f"Formulas: {v['formulas_pass']}/{v['formulas_total']} pass")
        xbrl_matches = sum(1 for i in ai_result.get('line_items', []) if i.get('xbrl_match') is True)
        xbrl_total = sum(1 for i in ai_result.get('line_items', []) if i.get('xbrl_match') is not None)
        print(f"XBRL matches (AI-reported): {xbrl_matches}/{xbrl_total}")
        print("=" * 80)

        full_output = {
            'ai_extraction': ai_result,
            'formula_verification': verification,
        }

    if args.output:
        with open(args.output, 'w') as f:
            json.dump(full_output, f, indent=2)
        print(f"\nSaved to {args.output}")

        # Update mapped.json — organize data by period, overwrite if amendment
        update_mapped_json(full_output, args.ticker, os.path.basename(args.output))


def update_mapped_json(full_output, ticker, source_filename):
    """
    Update mapped.json with data from a per-filing extraction.
    Organizes all line items by period. Later filings overwrite earlier ones
    for the same period (handles amendments and restated comparatives).
    """
    extract_dir = os.path.join(os.path.dirname(__file__), ticker)
    mapped_path = os.path.join(extract_dir, 'mapped.json')

    # Load existing mapped.json or start fresh
    if os.path.exists(mapped_path):
        with open(mapped_path) as f:
            existing = json.load(f)
        # Convert list to dict keyed by period for easy lookup
        mapped = {rec['period']: rec for rec in existing}
    else:
        mapped = {}

    ai = full_output.get('ai_extraction', {})

    for stmt_name in ['income_statement', 'balance_sheet', 'cash_flow']:
        stmt = ai.get(stmt_name, {})
        items = stmt.get('line_items', [])
        formulas = stmt.get('formulas', [])

        if not items:
            continue

        # Get all periods from the first item
        first_vals = items[0].get('values', {})
        periods = list(first_vals.keys())

        for period in periods:
            if period not in mapped:
                mapped[period] = {
                    'period': period,
                    'income_statement': {'line_items': [], 'formulas': []},
                    'balance_sheet': {'line_items': [], 'formulas': []},
                    'cash_flow': {'line_items': [], 'formulas': []},
                    'calculation_components': {},
                    'source_filings': [],
                }

            rec = mapped[period]

            # Build line items for this period
            period_items = []
            for item in items:
                val = item.get('values', {}).get(period)
                if val is not None:
                    period_items.append({
                        'label': item.get('label', ''),
                        'xbrl_concept': item.get('xbrl_concept', ''),
                        'value': val,
                        'unit': item.get('unit', 'USD_millions'),
                        'indent_level': item.get('indent_level', 0),
                    })

            if period_items:
                # Overwrite this statement for this period (later filing wins)
                rec[stmt_name]['line_items'] = period_items
                rec[stmt_name]['formulas'] = formulas

            if source_filename not in rec['source_filings']:
                rec['source_filings'].append(source_filename)

    # Calculation components and segment data — attach to the current period
    calc = ai.get('calculation_components', {})
    seg = ai.get('segment_data', {})
    if calc or seg:
        is_items = ai.get('income_statement', {}).get('line_items', [])
        if is_items:
            current_periods = list(is_items[0].get('values', {}).keys())
            if current_periods:
                current = current_periods[0]
                if current in mapped:
                    if calc:
                        mapped[current]['calculation_components'] = calc
                    if seg:
                        mapped[current]['segment_data'] = seg

    # Sort by period and write
    sorted_periods = sorted(mapped.keys())
    result = [mapped[p] for p in sorted_periods]

    with open(mapped_path, 'w') as f:
        json.dump(result, f, indent=2)

    print(f"\nUpdated {mapped_path} ({len(result)} periods)")


if __name__ == '__main__':
    main()
