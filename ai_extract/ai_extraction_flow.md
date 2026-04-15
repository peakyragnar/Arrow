# AI Extraction Flow

Three stages. Each stage has one job. No stage does another stage's work.

## Stage 1 — Extract and Verify (`analyze_statement.py`)

**Input**: Raw filing (HTML + XBRL) downloaded by `fetch.py`

**What the AI does**:
1. Reads the full filing (HTML + undimensioned XBRL facts + segment/geography dimensioned XBRL facts)
2. Extracts every line item from all three financial statements (IS, BS, CF)
3. Extracts segment data: revenue by business segment, geography, product/service, and customer concentration. Segment revenue totals must match total revenue from the IS.
4. Verifies using the financial statements' own math:
   - Component items sum to reported subtotals (e.g., sum of current assets = reported Total Current Assets)
   - IS flows: Revenue → Gross Profit → Operating Income → Pretax → Net Income
   - BS balances: Total Assets = Total Liabilities + Total Equity
   - CF reconciles: CFO + CFI + CFF = Net Change in Cash, Beginning + Change = Ending
   - Cross-statement: Net Income on IS = Net Income starting CF, Ending Cash on CF = Cash on BS
5. If any check fails, the AI loops — re-examines the filing, finds what it missed, fixes it
6. Does NOT output until all checks pass, or times out with a failure log

**CF sign convention**: The Python verification code checks each CF section (CFO/CFI/CFF) by summing components and comparing to the section total. If the sum doesn't match, a retry API call asks the AI to correct the signs using CF presentation convention (positive = source of cash, negative = use of cash). **The corrected values from the retry are applied back to `cash_flow.line_items`** — they replace the original values in the output. If the retry does not verify (corrected sum still doesn't match total), the values are NOT overwritten and the filing is flagged as unresolved.

The retry data (with correction reasoning) is also stored in `cfo_retry`, `cfi_retry`, `cff_retry` keys alongside the corrected `line_items` for training purposes — the error + correction pairs teach a post-trained model to self-verify.

**Outputs**:
- **Per-filing JSON** (`ai_extract/{TICKER}/q1_fy26_10q.json`): Exact extraction output with corrected CF signs, used as training data for post-trained model. Contains both the corrected `line_items` and the retry logs.
- **`mapped.json`** (`ai_extract/{TICKER}/mapped.json`): Same data organized by period. If the filing is an amendment (10-Q/A, 10-K/A) or a 10-K with restated comparatives, the amended periods get overwritten. This is the running record of the best-known values for each period.

**What this stage does NOT do**: No field renaming, no analytical mapping, no metric calculations. Just the verified financial statements + segment data stored two ways.

## Stage 2 — Standardized Field Mapping (`ai_formula.py`)

**Input**: Per-filing JSONs (each filing has IS + BS + CF + calculation_components together)

**What the AI does**:
1. Reads each per-filing extraction
2. Maps ALL line items to standardized field names (~70 fields covering complete IS, BS, CF)
3. Resolves ambiguities in 9 fields that require judgment:
   - **interest_expense_q**: Must be GROSS, not net. Find gross in notes if IS only shows net.
   - **income_tax_expense_q**: Can be a benefit (negative). Compute effective rate, flag if nonsensical.
   - **net_income_q**: Must be CONSOLIDATED (same as CF starting line), not net income to common if NCI exists.
   - **accounts_payable_q**: Must be PURE trade AP. If BS combines AP with accrued, find breakout in notes.
   - **short_term_debt_q**: May not be a separate BS line. Check notes for current portion of LT debt. Confirm 0 if none.
   - **operating_lease_liabilities_q**: TOTAL (current + non-current). Current portion often hidden in accrued liabilities.
   - **equity_q**: Parent-only stockholders equity, not total equity including NCI.
   - **capex_q**: May include intangibles, may be split across lines. Sum all capex lines.
   - **acquisitions_q**: May be multiple acquisition lines. Sum all.
4. Uses CF PRESENTATION signs (as shown on the statement, not XBRL raw signs). The sign that makes all components sum to the section total using + only.
5. Validates every formula:
   - IS: Revenue - COGS = Gross Profit, GP - Opex = OI, Pretax - Tax = NI
   - BS: Total Assets = Total L&E
   - CF: Sum of all CFO items = CFO, Sum of all CFI items = CFI, Sum of all CFF items = CFF
   - CF: CFO + CFI + CFF = Net Change in Cash
   - Cross: Net income on IS = Net income on CF
6. Outputs ~70 standardized fields with validation results

**Why Stage 2 maps ALL fields (not just analytical)**: The varying line item labels across filings (e.g., "Operating income" vs "Income from operations", 7+ variants of gains/losses labels) make it impossible to reliably subtract YTD periods using raw labels. Stage 2 normalizes everything to consistent field names so Stage 3's YTD-to-quarterly subtraction works correctly. This was learned the hard way — attempting to do label matching in the CSV export script produced wrong quarterly values that appeared to balance but had inverted signs and label mismatches.

**Output**: `formula_mapped.json` (`ai_extract/{TICKER}/formula_mapped.json`) — one record per filing with all fields in raw dollars, effective tax rate, validation results, and notes on ambiguity resolutions. Cost: ~$0.13/filing.

## Stage 3 — Quarterly Derivation (`ai_formula.py --from-mapped`)

**Input**: `formula_mapped.json` (all standardized fields per filing)

**What this stage does** (pure arithmetic, no AI):
- **Q1 10-Q**: All fields already quarterly. Pass through.
- **Q2/Q3 10-Q**: IS fields are already quarterly (use as-is). CF fields are YTD — subtract prior period's YTD to get standalone quarter. This works correctly because Stage 2 normalized all labels to consistent field names.
- **10-K**: IS and CF fields are annual — subtract Q1+Q2+Q3 to get Q4. Shares/EPS use annual value directly (weighted averages can't be derived by subtraction).
- **BS fields**: Point-in-time snapshots. No derivation needed.

**Output**: `quarterly.json` (`ai_extract/{TICKER}/quarterly.json`) — one record per quarter with ~70 standalone quarterly fields. This is the single source of truth for all downstream consumers.

## Downstream — Metric Calculations (`calculate.py`)

**Input**: `quarterly.json`

Computes all metrics from `formulas.md`: ROIC, ROIIC, reinvestment rate, gross profit growth, revenue growth, incremental margins, NOPAT margin, CFO/NOPAT, FCF/NOPAT, accruals ratio, CCC, SBC % revenue, diluted share count growth, net debt, interest coverage, working capital intensity, DSO/DIO/DPO, unlevered FCF.

**Output**: `dashboard/data/{ticker}.json`

## Verification CSV Export

`quarterly.json` is also the source for the verification CSV (`nvda_fy24_fy26_full_check.csv`). The CSV contains:
- Complete IS with formula checks (GP = Rev - COGS, OI = GP - Opex, NI = Pretax - Tax)
- Complete BS with formula checks (TCA, TA, TCL, TL, Assets = L+E)
- Complete CF with every line item and formula checks (CFO items sum = CFO, CFI items sum = CFI, CFF items sum = CFF, CFO+CFI+CFF = net change, NI on IS = NI on CF)
- All calculated metrics (TTM building blocks, tax rate, NOPAT, invested capital, ROIC, ROIIC, margins, growth, incremental margins, cash quality, CCC, leverage, working capital, reinvestment rate, FCF) — all as cell-reference formulas

Every formula check cell should be 0. If not, the extraction or derivation has a bug.

## File Summary

```
ai_extract/{TICKER}/
  q1_fy26_10q.json      — Stage 1: per-filing extraction (training data, corrected CF signs)
  q2_fy26_10q.json
  q3_fy26_10q.json
  q4_fy26_10k.json
  mapped.json            — Stage 1: all periods organized by period, updated on amendments
  formula_mapped.json    — Stage 2: ~70 standardized fields per filing
  quarterly.json         — Stage 3: ~70 fields per standalone quarter (single source of truth)
```

## Known Issues and Fixes

**CF sign inversion (fixed)**: The Stage 1 AI extraction sometimes uses XBRL raw signs instead of CF presentation signs. The Python verification detects this (CFO items don't sum to CFO total), triggers a retry API call, and the corrected values are now applied back to `cash_flow.line_items`. Previously the corrected values were stored in `cfo_retry` but never applied — this was fixed by adding writeback logic after verified retries.

**Label variation across filings**: NVIDIA's filings use different labels for the same item across quarters (e.g., "Losses on investments in non-affiliates" vs "Gains on non-marketable equity securities and publicly-held equity securities, net" — 7 variants across 12 filings). Stage 2 normalizes all variants to one consistent field name. Attempting to match labels directly (bypassing Stage 2) produces incorrect quarterly derivations.
