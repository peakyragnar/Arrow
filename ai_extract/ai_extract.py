"""
AI-powered financial data extraction.

Step 1: Trivial code parses XBRL -> JSON facts
Step 2: Claude reads the facts and maps them to 24 standard fields
Step 3: Trivial code derives quarterly values from YTD and validates

No per-company scripts. No concept mapping. The AI reads.
"""

import json
import sys
import os
import time
from dotenv import load_dotenv
import anthropic
from parse_xbrl_facts import parse_xbrl_to_facts

# Load .env from project root
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

# The 24 target fields - same as extract.py
TARGET_FIELDS = {
    # Income Statement (duration)
    "revenue_q": "Total revenue (positive number)",
    "cogs_q": "Cost of goods/services sold (positive number)",
    "operating_income_q": "Operating income/loss (positive = profit)",
    "rd_expense_q": "Research and development expense (positive number)",
    "income_tax_expense_q": "Income tax expense (positive = tax paid)",
    "pretax_income_q": "Income before taxes (positive = profit)",
    "net_income_q": "Net income/loss (positive = profit)",
    "interest_expense_q": "Interest expense (NEGATIVE number, e.g. -53000000)",

    # Balance Sheet (instant - as of period end)
    "equity_q": "Total stockholders' equity",
    "short_term_debt_q": "Short-term debt / current portion of long-term debt (0 if none)",
    "long_term_debt_q": "Long-term debt (non-current)",
    "operating_lease_liabilities_q": "Total operating lease liabilities (current + non-current)",
    "cash_q": "Cash and cash equivalents",
    "short_term_investments_q": "Short-term / marketable securities (0 if none)",
    "accounts_receivable_q": "Accounts receivable, net",
    "inventory_q": "Inventory (0 if none)",
    "accounts_payable_q": "Accounts payable",
    "total_assets_q": "Total assets",

    # Cash Flow Statement (duration)
    "cfo_q": "Net cash from operating activities (positive = cash inflow)",
    "capex_q": "Capital expenditures (NEGATIVE number, e.g. -298000000)",
    "dna_q": "Depreciation and amortization (positive number)",
    "acquisitions_q": "Cash paid for acquisitions (NEGATIVE number, 0 if none)",
    "sbc_q": "Stock-based compensation expense (positive number)",
    "diluted_shares_q": "Weighted average diluted shares outstanding",
}

# Fields that are duration-based and may need YTD derivation
INCOME_STMT_FIELDS = [
    "revenue_q", "cogs_q", "operating_income_q", "rd_expense_q",
    "income_tax_expense_q", "pretax_income_q", "net_income_q", "interest_expense_q",
]
CASH_FLOW_FIELDS = ["cfo_q", "capex_q", "dna_q", "acquisitions_q", "sbc_q"]
SHARES_FIELDS = ["diluted_shares_q"]
DURATION_FIELDS = INCOME_STMT_FIELDS + CASH_FLOW_FIELDS + SHARES_FIELDS

# Balance sheet fields (instant, no derivation needed)
BALANCE_SHEET_FIELDS = [
    "equity_q", "short_term_debt_q", "long_term_debt_q", "operating_lease_liabilities_q",
    "cash_q", "short_term_investments_q", "accounts_receivable_q", "inventory_q",
    "accounts_payable_q", "total_assets_q",
]

SYSTEM_PROMPT = """You are a financial data extraction engine. You receive XBRL facts from an SEC filing.
Your job: map facts to standard financial fields for the REPORTING PERIOD in the filing.

CRITICAL RULES:

1. PERIOD SELECTION:
   - The filing reports data for a specific period. A 10-Q covers a quarter (and sometimes YTD).
     A 10-K covers the full fiscal year.
   - For INCOME STATEMENT items: Extract values for the SHORTEST duration ending at the report
     date. In a Q1 10-Q this is the quarter. In a Q2 10-Q, there may be a 3-month and 6-month
     duration — use the 3-MONTH one (discrete quarter). In a Q3 10-Q, use the 3-month, not 9-month.
     In a 10-K, there is typically only the full-year duration — extract that and set
     "period_is_annual": true.
   - For CASH FLOW items: 10-Qs report YTD cash flows (not discrete quarter). A Q2 10-Q has
     6-month CFO, a Q3 has 9-month. A 10-K has 12-month. Extract whatever duration is available
     (use the LONGEST duration ending at the report date for cash flows). Set "cash_flow_ytd_months"
     to the number of months covered (3, 6, 9, or 12).
   - For BALANCE SHEET items: Use the INSTANT context at the report date (period end).
   - For DILUTED SHARES: Use the SHORTEST duration ending at the report date (discrete quarter).
     If only YTD/annual available, extract that and note it.

2. DIMENSIONS: Only use facts WITHOUT dimensions (no segment/axis). Undimensioned = consolidated.

3. SIGNS:
   - interest_expense_q: NEGATIVE (e.g., -53000000)
   - capex_q: NEGATIVE (e.g., -298000000)
   - acquisitions_q: NEGATIVE (or 0 if none)
   - All others: natural sign

4. CUSTOM EXTENSIONS: Company-specific tags are valid. Map to closest standard field by name.

5. MISSING DATA: Use 0 for stock/flow items (debt, inventory, investments, acquisitions), null otherwise.

6. VALUES: Use raw numbers from the facts. Already scaled.

Return a JSON object with:
- The 24 field keys with numeric values
- "period_is_annual": true/false (true only for 10-K where income statement is full year)
- "cash_flow_ytd_months": number (3, 6, 9, or 12)
- "fiscal_year": from DEI DocumentFiscalYearFocus
- "fiscal_period": from DEI DocumentFiscalPeriodFocus (Q1, Q2, Q3, FY)
- "period_end": the report end date (YYYY-MM-DD)

Return ONLY valid JSON. No explanation, no markdown fences.
"""


def filter_financial_facts(facts):
    """Remove noise — keep only financially relevant facts."""
    filtered = []
    skip_prefixes = ['srt:', 'country:']

    # Relevant concept keywords for aggressive filtering on large filings
    relevant_keywords = [
        'revenue', 'sales', 'cost', 'gross', 'operating', 'income', 'loss',
        'research', 'development', 'tax', 'pretax', 'net', 'interest',
        'equity', 'stockhold', 'debt', 'borrow', 'lease', 'cash', 'invest',
        'receivable', 'inventory', 'payable', 'asset', 'liabilit',
        'depreci', 'amortiz', 'acqui', 'compensat', 'share', 'earning',
        'dividend', 'capital', 'expenditure', 'purchase', 'property',
        'document', 'fiscal', 'entity', 'period', 'amendment', 'filer',
        'current', 'noncurrent', 'longterm', 'shortterm',
    ]

    for fact in facts:
        concept = fact['concept']

        if any(concept.startswith(p) for p in skip_prefixes):
            continue

        # Skip text-only facts (no unit = text blocks), except DEI
        if fact.get('unit') is None and not concept.startswith('dei:'):
            continue

        # Skip dimensioned facts (we want consolidated totals)
        if fact.get('dimensions'):
            continue

        filtered.append(fact)

    return filtered


def build_prompt(facts, filing_meta):
    """Build the extraction prompt with filtered facts."""
    filtered = filter_financial_facts(facts)

    prompt = f"""Extract financial data from this SEC filing.

Filing: {filing_meta.get('ticker', '?')} | {filing_meta.get('form', '?')} | filed {filing_meta.get('filing_date', '?')} | report date {filing_meta.get('report_date', '?')}

{len(filtered)} XBRL facts (undimensioned, numeric + DEI):

{json.dumps(filtered, indent=2)}

Extract these 24 fields:
{json.dumps(TARGET_FIELDS, indent=2)}
"""
    return prompt


def extract_filing(xml_path, meta_path, client):
    """Run AI extraction on a single filing."""
    facts = parse_xbrl_to_facts(xml_path)
    print(f"Parsed {len(facts)} raw facts from {os.path.basename(xml_path)}")

    with open(meta_path) as f:
        meta = json.load(f)

    prompt = build_prompt(facts, meta)
    filtered_count = len(filter_financial_facts(facts))
    print(f"Sending {filtered_count} filtered facts to Claude...")

    # Retry up to 2 times on parse failures
    for attempt in range(3):
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4000,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}]
        )

        response_text = response.content[0].text.strip()

        # Strip markdown code fences if present
        if response_text.startswith('```'):
            lines = response_text.split('\n')
            # Remove first line (```json) and last line (```)
            lines = [l for l in lines[1:] if l.strip() != '```']
            response_text = '\n'.join(lines)

        try:
            result = json.loads(response_text)
            break
        except json.JSONDecodeError:
            if attempt < 2:
                print(f"  JSON parse failed (attempt {attempt+1}), retrying...")
                time.sleep(1)
            else:
                print(f"  Response text: {response_text[:200]}...")
                raise

    # Add metadata
    result['ticker'] = meta.get('ticker')
    result['form'] = meta.get('form')
    result['filing_date'] = meta.get('filing_date')
    result['report_date'] = meta.get('report_date')
    result['accession'] = meta.get('accession')

    return result


def derive_quarterly_values(results):
    """Derive discrete quarterly values from YTD cash flows and annual income statements.

    This is the trivial deterministic step — just subtraction.
    """
    # Sort by fiscal_year and fiscal_period
    period_order = {'Q1': 1, 'Q2': 2, 'Q3': 3, 'FY': 4, 'Q4': 4}

    sorted_results = sorted(results, key=lambda r: (
        int(r.get('fiscal_year', 0) or 0),
        period_order.get(str(r.get('fiscal_period', '')), 0)
    ))

    # Build lookup by (fiscal_year, fiscal_period) — normalize types
    lookup = {}
    for r in sorted_results:
        fy = str(r.get('fiscal_year', ''))
        fp = str(r.get('fiscal_period', ''))
        lookup[(fy, fp)] = r

    derived = []
    for r in sorted_results:
        out = dict(r)
        fy = str(r.get('fiscal_year', ''))
        fp = str(r.get('fiscal_period', ''))
        ytd_months = r.get('cash_flow_ytd_months', 3)
        if isinstance(ytd_months, str):
            ytd_months = int(ytd_months)

        # --- Cash flow derivation (always YTD in filings) ---
        if ytd_months == 6:
            q1 = lookup.get((fy, 'Q1'))
            if q1:
                for field in CASH_FLOW_FIELDS:
                    ytd_val = r.get(field)
                    q1_val = q1.get(field)
                    if ytd_val is not None and q1_val is not None:
                        out[field] = ytd_val - q1_val

        elif ytd_months == 9:
            q2_raw = lookup.get((fy, 'Q2'))
            if q2_raw:
                for field in CASH_FLOW_FIELDS:
                    ytd_val = r.get(field)
                    q2_ytd_val = q2_raw.get(field)
                    if ytd_val is not None and q2_ytd_val is not None:
                        out[field] = ytd_val - q2_ytd_val

        elif ytd_months == 12:
            q3_raw = lookup.get((fy, 'Q3'))
            if q3_raw:
                for field in CASH_FLOW_FIELDS:
                    annual_val = r.get(field)
                    q3_ytd_val = q3_raw.get(field)
                    if annual_val is not None and q3_ytd_val is not None:
                        out[field] = annual_val - q3_ytd_val

        # --- Income statement derivation for 10-K (annual -> Q4) ---
        if r.get('period_is_annual'):
            q3_raw = lookup.get((fy, 'Q3'))
            q2_raw = lookup.get((fy, 'Q2'))
            q1 = lookup.get((fy, 'Q1'))

            # Calculate Q1+Q2+Q3 sum for income stmt
            prior_quarters = []
            if q1:
                prior_quarters.append(q1)
            if q2_raw and not q2_raw.get('period_is_annual'):
                prior_quarters.append(q2_raw)
            if q3_raw and not q3_raw.get('period_is_annual'):
                prior_quarters.append(q3_raw)

            # For 10-K: derive Q4 = Annual - (Q1 + Q2 + Q3)
            # But Q2/Q3 income statement values should already be discrete quarter
            # (the AI picks shortest duration for income stmt)
            if len(prior_quarters) == 3:
                for field in INCOME_STMT_FIELDS:
                    annual_val = r.get(field)
                    if annual_val is None:
                        continue
                    prior_sum = sum(q.get(field, 0) or 0 for q in prior_quarters)
                    out[field] = annual_val - prior_sum

            out['fiscal_period'] = 'Q4'

        derived.append(out)

    return derived


def validate(result, expected=None):
    """Basic validation checks."""
    issues = []

    # Sign checks
    if result.get('interest_expense_q') and result['interest_expense_q'] > 0:
        issues.append(f"interest_expense_q should be negative, got {result['interest_expense_q']:,}")
    if result.get('capex_q') and result['capex_q'] > 0:
        issues.append(f"capex_q should be negative, got {result['capex_q']:,}")
    if result.get('acquisitions_q') and result['acquisitions_q'] > 0:
        issues.append(f"acquisitions_q should be negative, got {result['acquisitions_q']:,}")

    # Compare to expected if provided
    if expected:
        for field in TARGET_FIELDS:
            ai_val = result.get(field)
            exp_val = expected.get(field)
            if exp_val is None and ai_val is None:
                continue
            if exp_val is None or ai_val is None:
                issues.append(f"{field}: AI={ai_val}, expected={exp_val}")
                continue
            if exp_val == 0 and ai_val == 0:
                continue
            if exp_val != 0:
                pct_diff = abs(ai_val - exp_val) / abs(exp_val) * 100
                if pct_diff > 0.1:
                    issues.append(f"{field}: AI={ai_val:,}, expected={exp_val:,} (diff={pct_diff:.2f}%)")

    return issues


def main():
    import argparse
    parser = argparse.ArgumentParser(description='AI-powered financial extraction')
    parser.add_argument('--ticker', required=True, help='Ticker symbol')
    parser.add_argument('--accession', help='Specific accession number')
    parser.add_argument('--compare', help='Path to expected output JSON for comparison')
    parser.add_argument('--all', action='store_true', help='Process all filings')
    parser.add_argument('--raw', action='store_true', help='Show raw (pre-derivation) values')
    args = parser.parse_args()

    filings_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'filings', args.ticker)
    if not os.path.exists(filings_dir):
        print(f"No filings found for {args.ticker}")
        sys.exit(1)

    # Load expected data
    expected_data = {}
    if args.compare:
        with open(args.compare) as f:
            for q in json.load(f):
                expected_data[q['accession']] = q

    # Get accessions
    if args.accession:
        accessions = [args.accession]
    elif args.all:
        accessions = sorted(os.listdir(filings_dir))
    else:
        accessions = sorted(os.listdir(filings_dir))[:1]

    client = anthropic.Anthropic()
    raw_results = []

    for acc in accessions:
        acc_dir = os.path.join(filings_dir, acc)
        if not os.path.isdir(acc_dir):
            continue

        meta_path = os.path.join(acc_dir, 'filing_meta.json')
        if not os.path.exists(meta_path):
            continue

        xml_files = [f for f in os.listdir(acc_dir) if f.endswith('_htm.xml')]
        if not xml_files:
            print(f"  No XBRL XML found in {acc}")
            continue

        xml_path = os.path.join(acc_dir, xml_files[0])

        print(f"\n{'='*60}")
        print(f"Processing {args.ticker} - {acc}")
        print(f"{'='*60}")

        try:
            result = extract_filing(xml_path, meta_path, client)
            raw_results.append(result)

            fp = result.get('fiscal_period', '?')
            fy = result.get('fiscal_year', '?')
            ytd = result.get('cash_flow_ytd_months', '?')
            annual = result.get('period_is_annual', False)
            print(f"  FY{fy} {fp} | CF YTD months: {ytd} | Annual: {annual}")

        except Exception as e:
            print(f"  ERROR: {e}")
            import traceback
            traceback.print_exc()

    if not raw_results:
        print("No results to process.")
        return

    # Step 3: Derive quarterly values
    print(f"\n{'='*60}")
    print(f"DERIVING QUARTERLY VALUES")
    print(f"{'='*60}")

    if args.raw:
        derived = raw_results
        print("  (showing raw pre-derivation values)")
    else:
        derived = derive_quarterly_values(raw_results)
        print(f"  Derived {len(derived)} quarters from {len(raw_results)} filings")

    # Evaluate
    total_fields = 0
    total_match = 0
    total_close = 0
    total_mismatch = 0
    mismatch_details = []

    for result in derived:
        acc = result.get('accession')
        expected = expected_data.get(acc)
        fp = result.get('fiscal_period', '?')
        fy = result.get('fiscal_year', '?')

        if expected:
            issues = validate(result, expected)
            status = "PASS" if not issues else f"FAIL ({len(issues)} issues)"
            print(f"\n  FY{fy} {fp}: {status}")

            for field in TARGET_FIELDS:
                ai_val = result.get(field)
                exp_val = expected.get(field)
                if exp_val is None and ai_val is None:
                    total_match += 1
                    total_fields += 1
                    continue
                if exp_val is not None and ai_val is not None:
                    total_fields += 1
                    if exp_val == ai_val:
                        total_match += 1
                    elif exp_val != 0 and abs(ai_val - exp_val) / abs(exp_val) < 0.001:
                        total_close += 1
                    else:
                        total_mismatch += 1
                        pct = abs(ai_val - exp_val) / abs(exp_val) * 100 if exp_val != 0 else 999
                        mismatch_details.append(f"    FY{fy} {fp} {field}: AI={ai_val:,} vs exp={exp_val:,} ({pct:.1f}%)")
                else:
                    total_fields += 1
                    total_mismatch += 1
                    mismatch_details.append(f"    FY{fy} {fp} {field}: AI={ai_val} vs exp={exp_val}")

        else:
            print(f"\n  FY{fy} {fp}: (no expected data)")
            for field in TARGET_FIELDS:
                val = result.get(field)
                if val is not None and isinstance(val, (int, float)) and abs(val) >= 1000:
                    print(f"    {field}: {val:>20,.0f}")

    # Summary
    if expected_data:
        print(f"\n{'='*60}")
        print(f"ACCURACY SUMMARY")
        print(f"{'='*60}")
        print(f"  Total fields: {total_fields}")
        print(f"  Exact:        {total_match}")
        print(f"  Close:        {total_close}")
        print(f"  Mismatch:     {total_mismatch}")
        if total_fields > 0:
            print(f"  Accuracy:     {(total_match + total_close) / total_fields * 100:.1f}%")

        if mismatch_details:
            print(f"\n  Mismatches:")
            for d in mismatch_details:
                print(d)

    # Save
    output_path = os.path.join(os.path.dirname(__file__), f'{args.ticker.lower()}_ai_extract.json')
    with open(output_path, 'w') as f:
        json.dump(derived, f, indent=2)
    print(f"\nResults saved to {output_path}")


if __name__ == '__main__':
    main()
