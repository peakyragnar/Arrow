# Extraction Schema

## Output JSON Structure

```json
{
  "income_statement": {
    "line_items": [...],
    "formulas": [...]
  },
  "balance_sheet": {
    "line_items": [...],
    "formulas": [...]
  },
  "cash_flow": {
    "line_items": [...],
    "formulas": [...]
  },
  "cross_statement_checks": [...],
  "calculation_components": {...},
  "verification": {
    "is_pass": true,
    "bs_pass": true,
    "cf_pass": true,
    "cross_pass": true,
    "details": [...]
  }
}
```

## Line Item Format

```json
{
  "label": "Revenue",
  "indent_level": 0,
  "xbrl_concept": "us-gaap:Revenues",
  "values": {"2025-01-27_2025-04-27": 44062, "2024-01-29_2024-04-28": 26044},
  "unit": "USD_millions",
  "xbrl_match": true,
  "mapping_reason": "HTML row text is 'Revenue'. ix:nonFraction tag confirms us-gaap:Revenues = 44,062M."
}
```

- **label**: Exact line item text from the statement.
- **indent_level**: Hierarchy depth (0 = top-level/totals, 1 = sub-items).
- **xbrl_concept**: XBRL concept from ix:nonFraction tag, cross-referenced against XBRL facts.
- **values**: Key = period. Duration items: "startDate_endDate". Instant items: just date. Values in millions as integers.
- **unit**: "USD_millions", "USD_per_share", or "shares_millions".
- **xbrl_match**: Did ix:nonFraction tag value match XBRL fact? true/false/null (null for headers).
- **mapping_reason**: Explain how you matched the line item to the XBRL concept.

## Formula Format

```json
{
  "formula": "Revenue - Cost of revenue = Gross profit",
  "components": ["Revenue", "Cost of revenue", "Gross profit"],
  "operation": "Revenue - Cost of revenue",
  "result_label": "Gross profit"
}
```

The `operation` field must be a math expression using EXACT label names connected by `+` and `-`.

## Sign Conventions

### Income Statement
- Revenue, income: POSITIVE
- Expenses (COGS, R&D, SGA, tax): POSITIVE (they are costs)
- Formulas use subtraction: Revenue - COGS = Gross Profit

### Balance Sheet
- All items: POSITIVE (assets, liabilities, equity)
- Formulas use addition: Total Liabilities + Total Equity = Total Assets

### Cash Flow — CRITICAL

**Use the sign AS DISPLAYED on the cash flow statement.** Every value should have the sign that, when all components are ADDED together with `+`, produces the section total.

- Net income: POSITIVE
- Non-cash add-backs (SBC, D&A): POSITIVE
- Working capital changes: USE THE CF PRESENTATION SIGN
  - If AR increased (use of cash), the CF shows it as NEGATIVE → store NEGATIVE
  - If inventory decreased (source of cash), CF shows POSITIVE → store POSITIVE
  - DO NOT use the raw XBRL sign if it differs from the CF presentation
- Section totals (CFO, CFI, CFF): as reported (CFI and CFF are typically negative)
- All CF formulas use `+` ONLY — signs are in the values

**Why this matters**: The formula `NI + SBC + D&A + ... + AR change + Inv change + ... = CFO` must balance using `+` for every term. If you store AR change as positive when the CF shows negative, the sum will be wrong and verification will fail.

## Required Formulas

### Income Statement
1. Revenue - COGS = Gross Profit
2. R&D + SGA (+ any other opex) = Total Operating Expenses
3. Gross Profit - Total Operating Expenses = Operating Income
4. All other income/expense items summed = Total Other Income/Expense
5. Operating Income + Total Other Income/Expense = Pretax Income
6. Pretax Income - Income Tax Expense = Net Income

### Balance Sheet
1. Sum of current asset items = Total Current Assets
2. Total Current Assets + sum of non-current items = Total Assets
3. Sum of current liability items = Total Current Liabilities
4. Total Current Liabilities + sum of non-current items = Total Liabilities
5. Sum of equity components = Total Equity
6. Total Liabilities + Total Equity = Total Liabilities and Equity
7. Total Assets = Total Liabilities and Equity

### Cash Flow
1. All CFO components (NI + adjustments + WC changes) summed with `+` = CFO total
2. All CFI components summed with `+` = CFI total
3. All CFF components summed with `+` = CFF total
4. CFO + CFI + CFF = Net Change in Cash
5. Beginning Cash + Net Change = Ending Cash

### Cross-Statement
1. Net Income on IS = Net Income starting CF
2. Ending Cash on CF = Cash on BS (current period)
3. Beginning Cash on CF = Cash on BS (prior period, if available)

## Calculation Components

After extracting the three statements, search the FULL filing for these items:

1. **OPERATING LEASES**: Current + non-current operating lease liabilities. Current portion often hidden in "Accrued liabilities." Check XBRL tag OperatingLeaseLiabilityCurrentStatementOfFinancialPositionExtensibleList.
   Output: `{"current": X, "noncurrent": X, "total": X, "current_location": "where found"}`

2. **D&A BREAKDOWN**: May be split into depreciation, amortization of intangibles, etc.
   Output: `{"total": X, "components": [{"label": "...", "value": X}], "is_single_line": true/false}`

3. **ACCOUNTS PAYABLE**: Must be PURE trade AP, not combined with accrued.
   Output: `{"value": X, "is_pure": true/false, "note_breakout": X or null}`

4. **CAPEX**: PP&E and intangible asset purchases.
   Output: `{"cf_value": X, "includes_intangibles": true/false}`

5. **ACQUISITIONS**: Sum all acquisition lines.
   Output: `{"total": X, "items": [{"concept": "...", "value": X}]}`

6. **SHORT-TERM DEBT**: Current portion of LT debt, commercial paper, etc. Confirm zero if none.
   Output: `{"value": X, "confirmed_zero": true/false}`

7. **SBC**: Stock-based compensation from CF addback.
   Output: `{"cf_value": X}`

8. **INTEREST EXPENSE**: GROSS interest, not net of income.
   Output: `{"gross": X, "income": X, "net": X}`

9. **TAX RATE**: Effective rate = tax expense / pretax income.
   Output: `{"tax_expense": X, "pretax_income": X, "effective_rate": X}`

10. **INVENTORY**: Breakdown if disclosed.
    Output: `{"total": X, "raw_materials": X or null, "wip": X or null, "finished_goods": X or null}`

## JSON Rules

- ALL string values: never use apostrophes or single quotes. Use "shareholders equity" not "shareholders' equity".
- Output ONLY valid JSON. No commentary outside the JSON structure.
- All monetary values in millions as integers.
- All periods reported in the filing (current + comparatives).
