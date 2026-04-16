# AI Extraction Pipeline — Full Flow

## Principles

1. **Tell it what must be true, not what to find.** The AI extracts what the filing contains. No prescribed formulas, no prescribed items, no prescribed segment categories.
2. **Math proves correctness.** Every subtotal must tie to its components. Every cross-statement value must match. The verification script checks independently.
3. **Nothing unaccounted for.** Every XBRL-tagged fact must be placed — on a statement face or identified as hidden/aggregated. The AI cannot skip items.
4. **Disaggregation discovered, not prescribed.** Extract all revenue and operating income breakdowns the filing reports. Each must sum to the consolidated total.
5. **No customer concentration.** It's a percentage disclosure, not a dollar amount. Doesn't fit the pipeline.
6. **Format is specified, content is not.** The prompt defines JSON structure and field names. It does NOT define which labels, formulas, or concepts should appear.
7. **Retry until it ties.** If formulas don't balance, cross-statement values don't match, or XBRL facts are unaccounted for — the AI keeps working.

---

## Step 1: Download

```bash
python3 fetch.py --cik 0001045810 --ticker NVDA
```

Downloads per filing from SEC EDGAR:

| File | Purpose |
|------|---------|
| `nvda-20250427.htm` | Filing HTML (iXBRL) |
| `nvda-20250427_htm.xml` | XBRL instance document — all tagged facts |
| `nvda-20250427_cal.xml` | Calculation linkbase — all declared formulas with weights |
| `nvda-20250427_pre.xml` | Presentation linkbase — concept-to-statement mapping with display order |
| `nvda-20250427_def.xml` | Definition linkbase — dimension hierarchies (segments, geography, products) |
| `filing_meta.json` | Filing metadata (CIK, ticker, dates, filenames) |

Stored: `data/filings/{TICKER}/{ACCESSION}/`

---

## Step 2: Deterministic Parse

```bash
python3 ai_extract/parse_xbrl.py --ticker NVDA --accession {ACCESSION}
```

Reads all 5 files from Step 1, outputs one structured JSON:

**`parsed_xbrl.json`** containing:
- **facts**: every XBRL-tagged value (concept, period, value, unit, dimensions)
- **calculations**: every declared formula with signed weights, grouped by section (IS, BS, CF, and all note-level decompositions like accrued liabilities breakdown, inventory components, operating lease split, segment reconciliation)
- **presentation**: every concept mapped to its statement or note section, in display order
- **definitions**: dimension hierarchies (segment members, geography members, product members)

Stored: `data/filings/{TICKER}/{ACCESSION}/parsed_xbrl.json`

No AI. No judgment. Pure parsing.

---

## Step 3: AI Extraction + Verification

```bash
python3 ai_extract/analyze_statement.py --ticker NVDA --accession {ACCESSION} --statement all
```

### Inputs to the AI

`analyze_statement.py` reads `parsed_xbrl.json` and the filing HTML, then formats the linkbase data into prompt sections:

1. **CALCULATION RELATIONSHIPS** — every formula from the cal linkbase, readable format:
   ```
   INCOME STATEMENT:
     GrossProfit = +Revenues -CostOfRevenue
     OperatingIncomeLoss = +GrossProfit -OperatingExpenses
     ...
   ACCRUED LIABILITIES NOTE DETAIL:
     AccruedLiabilitiesCurrent = +OperatingLeaseLiabilityCurrent +EmployeeRelated +8 others
     ...
   ```

2. **PRESENTATION STRUCTURE** — which concepts belong to which statement, in order

3. **DIMENSION HIERARCHIES** — segment/geography/product members from def linkbase

4. **XBRL FACTS** — all tagged values (existing)

5. **FILING HTML (stripped)** — only the financial statement tables, not the full filing. The `extract_statement_html()` function uses ix:nonFraction tag positions and the presentation linkbase to identify the IS, BS, and CF sections in the HTML. This reduces HTML from ~122K tokens to ~27K tokens (78% reduction).

### Prompt Instructions (principle-based)

- Extract every line item with its value from all three statements
- Verify every declared calculation relationship
- Where XBRL precision is insufficient (`decimals=-8`), read the exact value from the HTML
- Account for every XBRL fact — if it exists, it must be placed
- Extract all disaggregation data, verify each breakdown sums to the consolidated total
- Report anything that doesn't tie

### Incremental HTML Retry

If formulas don't balance on the first pass (stripped HTML), the retry sends the **full filing HTML** so the AI can search the notes for missing components. The cal linkbase role names (e.g., `BalanceSheetComponentsScheduleofAccruedandOtherCurrentLiabilitiesDetails`) identify which note section contains the needed data — a future optimization can send only the targeted note section instead of the full filing.

Typical flow:
1. **First pass**: stripped HTML (~27K tokens) — cheap, handles most filings
2. **Verification**: formulas tie? Cross-statement checks pass?
3. **If yes**: done ($1.41 for NVDA Q1 FY26 on Sonnet)
4. **If no**: retry with full HTML (~122K tokens) targeting the specific sections that failed

### Outputs

| File | Purpose |
|------|---------|
| `q1_fy26_10q.json` | Per-filing extraction (immutable training data) |
| `mapped.json` | All periods by period, amendment rules applied, prior periods overwritten |

Stored: `ai_extract/{TICKER}/`

Output format unchanged: `line_items`, `formulas`, `xbrl_not_on_statement`, `segment_data` per statement.

---

## Step 4: Cross-Filing Normalization (Stage 2)

```bash
python3 ai_extract/ai_formula.py --ticker NVDA
```

Reads `mapped.json` (all periods from all filings).

AI does:
- Normalize labels across filings (XBRL concepts help — same concept = same item regardless of display label)
- Decompose aggregated items using note-level data extracted in Step 3
- Verify no double counting
- Handle cases where XBRL tagging changed between filings

Outputs: `formula_mapped.json`

---

## Step 5: Quarterly Derivation (Stage 3)

```bash
python3 ai_extract/ai_formula.py --ticker NVDA --from-mapped
```

Pure arithmetic. No AI needed.

- Q1: pass-through (already standalone quarter)
- Q2/Q3: CF and IS use YTD subtraction (Q2 standalone = Q2 YTD - Q1)
- Q4: annual minus Q1+Q2+Q3
- BS: snapshots at period end, no derivation needed
- Segments: same logic as IS fields

Outputs: `quarterly.json` (standalone quarterly values — single source of truth)

---

## Step 6: Metrics

```bash
python3 calculate.py --ticker NVDA
```

Reads `quarterly.json`, computes ROIC, margins, growth, and other metrics from `formulas.md`. Outputs dashboard data.

---

## What the AI Does vs. Doesn't Do

| Task | Before (template) | After (linkbase + principles) |
|------|-------------------|-------------------------------|
| Discover statement structure | AI figures it out from HTML | Presentation linkbase provides it |
| Discover formula relationships | AI figures it out (or prompt prescribes them) | Calculation linkbase provides them |
| Find hidden items (e.g. leases in accrued) | Prompt tells AI exactly where to look | Cal linkbase declares the decomposition |
| Find segment structure | Prompt prescribes 4 categories | Def linkbase provides dimension hierarchy |
| Read precise values | AI reads HTML | AI reads HTML (still needed where XBRL rounds) |
| Verify math | AI + verification script | AI + verification script (formulas from linkbase) |
| Cross-filing label normalization | AI in Stage 2 | AI in Stage 2 (XBRL concepts reduce the work) |
| YTD to quarterly | Arithmetic in Stage 3 | Arithmetic in Stage 3 (unchanged) |

---

## File Map

```
fetch.py                              — Step 1: download filings + linkbases from EDGAR

ai_extract/
  parse_xbrl.py                       — Step 2: deterministic parse → parsed_xbrl.json
  analyze_statement.py                — Step 3: AI extraction + verification → per-filing .json + mapped.json
  ai_formula.py                       — Step 4 + 5: normalization + quarterly derivation
  ai_extraction_flow_full.md          — This document

data/filings/{TICKER}/{ACCESSION}/
  *.htm                               — Filing HTML
  *_htm.xml                           — XBRL instance (facts)
  *_cal.xml                           — Calculation linkbase (formulas)
  *_pre.xml                           — Presentation linkbase (structure)
  *_def.xml                           — Definition linkbase (dimensions)
  filing_meta.json                    — Filing metadata
  parsed_xbrl.json                    — Deterministic parse output (Step 2)

ai_extract/{TICKER}/
  q*_fy*_10*.json                     — Per-filing extractions (immutable training data)
  mapped.json                         — All periods, amendment-aware
  formula_mapped.json                 — Normalized fields per filing
  quarterly.json                      — Standalone quarterly values (single source of truth)

calculate.py                          — Step 6: metrics from quarterly.json
dashboard/                            — Chart.js app served locally
```
