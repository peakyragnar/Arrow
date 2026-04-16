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

5. **FILING HTML (auto-sized)** — the system automatically selects full or stripped HTML based on filing size:
   - **Full HTML** (< 150K tokens): sends the entire cleaned filing. No completeness retry needed — the AI sees everything. Used for most 10-Qs.
   - **Stripped HTML** (>= 150K tokens): sends only the financial statement tables, identified by ix:nonFraction tag positions and presentation linkbase. Used for large 10-Ks and filings that would exceed context limits.
   - **`--full-html` flag**: forces full HTML regardless of size.

### Prompt Instructions (principle-based)

- Extract every line item with its value from all three statements
- Verify every declared calculation relationship
- Where XBRL precision is insufficient (`decimals=-8`), read the exact value from the HTML
- Account for every XBRL fact — if it exists, it must be placed
- Extract all disaggregation data, verify each breakdown sums to the consolidated total
- Report anything that doesn't tie

### Verification + Retry Pipeline

After the AI returns its extraction, three deterministic checks run in sequence:

**1. Formula verification** — every formula the AI reported is checked arithmetically. If any formula fails, the AI already gets another attempt via the existing retry mechanism.

**2. CF section retry** — if CFO, CFI, or CFF component sums don't match the section total, a targeted retry sends the full HTML to find missing cash flow components.

**3. XBRL fact completeness check** — `check_fact_completeness()` compares every XBRL fact we sent to the AI against what it reported in `line_items` + `xbrl_not_on_statement`. Any concept in the input but not in the output is a gap. This is not a hardcoded list — it comes from the filing's own XBRL, different for every company and filing.

   - **If stripped HTML was used**: the unaccounted concepts are sent back to the AI with targeted HTML sections extracted by their ix:nonFraction tag positions. The AI places each one (which statement it belongs to, where it's classified). Results merge into `xbrl_not_on_statement`.
   - **If full HTML was used**: the AI already had everything. Unaccounted facts are logged for audit but not retried with the same context.

Typical flow for a 10-Q (full HTML, under threshold):
1. **First pass**: full HTML — AI sees everything
2. **Formula verification**: pass/fail
3. **Completeness check**: log any gaps (no retry since full HTML was sent)
4. **Done**

Typical flow for a 10-K (stripped HTML, over threshold):
1. **First pass**: stripped HTML (statements only)
2. **Formula verification**: pass/fail, CF retry if needed
3. **Completeness check**: finds unaccounted notes-level facts
4. **Completeness retry**: sends targeted notes HTML for the gaps
5. **Done**

### Outputs

| File | Purpose |
|------|---------|
| `q1_fy26_10q.json` | Per-filing extraction (immutable training data) |
| `mapped.json` | All periods by period, amendment rules applied, prior periods overwritten |

Stored: `ai_extract/{TICKER}/`

Output format unchanged: `line_items`, `formulas`, `xbrl_not_on_statement`, `segment_data` per statement.

---

## Step 4: All-Periods Normalization + Quarterly Derivation (Stage 2)

```bash
python3 ai_extract/ai_formula.py --ticker NVDA --v3
```

One AI call sees all filings at once. Input: slimmed per-filing extractions (~96K tokens for 12 NVDA filings) + `formulas.md`.

AI does:
- Read all filings to understand this company's reporting structure
- For each analytical input the metric formulas need, determine where it comes from across all periods
- Normalize field names consistently across all periods
- Handle stock splits (normalize pre-split shares to post-split basis)
- Forward-fill annual-only values (e.g., operating lease liabilities disclosed only in 10-K) with explicit flags
- Handle reporting changes between filings (renamed items, new segments)

Verification (deterministic, runs after AI returns):
1. **Quarterly derivation** — Q1 pass-through, Q2/Q3 CF YTD subtraction, Q4 = annual - Q1-Q2-Q3. This IS the primary verification: wrong period values produce impossible results.
2. **Sanity checks** — revenue must be positive, no impossible sign flips
3. **Formula checks** — revenue - cogs = gross_profit, pretax - tax = net_income
4. **Field presence** — every standard field in every period
5. **Split normalization** — diluted shares within 2x range across all quarters
6. **Forward-fill audit** — verify flagged items genuinely don't exist in that filing's XBRL
7. **Continuity** — no 5x jumps between consecutive quarters

If any check fails, the failures are sent back to the AI for retry (max 3). The retry prompt includes the specific errors.

Outputs:
- `formula_mapped_v3.json` — company mapping + analytical fields per period
- `quarterly.json` — standalone quarterly values (single source of truth)

---

## Step 5: Metrics

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
| Cross-filing normalization | AI in separate Stage 2 (per-filing) | AI in Stage 2 (all periods in one call) |
| YTD to quarterly | Separate Stage 3 | Embedded in Stage 2 as verification |
| Stock split normalization | Not handled | AI normalizes in Stage 2 (sees all periods) |
| Annual-only forward fill | Hardcoded in calculate.py | AI flags and fills in Stage 2 |

---

## File Map

```
fetch.py                              — Step 1: download filings + linkbases from EDGAR

ai_extract/
  parse_xbrl.py                       — Step 2: deterministic parse → parsed_xbrl.json
  analyze_statement.py                — Step 3: AI extraction + verification → per-filing .json + mapped.json
  ai_formula.py                       — Step 4: all-periods normalization + quarterly derivation
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
  formula_mapped_v3.json              — Analytical mapping + quarterly derivation output
  quarterly.json                      — Standalone quarterly values (single source of truth)

calculate.py                          — Step 6: metrics from quarterly.json
dashboard/                            — Chart.js app served locally
```
