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

## Step 4: Stage 2 — Statements + Normalized Buckets

```bash
python3 ai_extract/ai_formula.py --ticker NVDA --test
```

Stage 2 produces two layers of output, both verified by math. Every output
matches the schema described here; nothing is invented for a specific company.

### Layer 1 — As-reported statements (deterministic)

For each of IS / BS / CF:

- **Rows** — merged across all Stage 1 filings by `xbrl_concept`. Multiple
  label variants are collected into a joined `labels` list (one row per
  concept). Both `line_items` and `xbrl_not_on_statement` (note detail) are
  captured; rows tagged `is_note_detail: true` when they came from the notes.
- **Values per quarter** — selected deterministically from each filing's
  current-period column per period-type rule:
  - 10-Q IS: shortest duration ending at filing period_end (the 3-month column).
  - 10-Q CF: longest duration ending at filing period_end (YTD).
  - 10-K IS/CF: 12-month annual.
  - BS (any form): instant at filing period_end.
- **Q2/Q3 CF derivation** — Python: quarterly = YTD − prior YTD.
- **Q4 derivation (flows)** — Python: Q4 = annual − Q1 − Q2 − Q3. Linear
  derivation preserves formula ties.
- **Formulas** — Stage 1's declared formulas are carried through (components
  translated from labels to xbrl_concepts) and evaluated per quarter. Each
  formula is scoped to the quarters of its source filing (a 10-Q's formula
  applies to that 10-Q's quarter; a 10-K's formula applies to the annual and
  — by linearity — to the derived Q4). Evaluated only where applicable, so no
  spurious breaks when component structure drifts across filings.

No AI in Layer 1. Purely Python from Stage 1 output.

### Layer 2 — Normalized buckets (AI judgment)

The universal bucket lists are declared in `canonical_buckets.md` (same for
every company, every statement). The AI is given the Layer 1 statements +
`canonical_buckets.md` and assigns each as-reported row (including note
detail) to exactly one **detail bucket**. Subtotals (`gross_profit`,
`operating_income`, `total_assets`, `cfo`, etc.) are not assigned — they are
computed arithmetically from the detail buckets per `canonical_buckets.md`.

Per bucket, the AI outputs `source_concepts` (the xbrl_concepts feeding it)
so every bucket value traces to specific as-reported rows.

The AI also handles:
- **Stock splits** — detect and normalize shares across pre/post-split periods.
- **Forward-fills** — annual-only items may be forward-filled from the most
  recent 10-K to the following Q1–Q3, but only when the concept is genuinely
  absent from those quarters' raw XBRL. Forward-fills carry a receipt
  listing `candidate_concepts` that the verifier re-checks against raw
  `parsed_xbrl.json`. False fills are rejected.

### Segments

Segment data is collected from Stage 1's `segment_data`, quarterized
(same period-type rule), and organized per axis. Each axis carries a
`consolidated_by_quarter_and_metric` map; member values must sum to that
consolidated total every quarter.

### Verification battery (deterministic, runs after AI)

1. **As-reported formula ties** — every Stage-1 formula ties in every quarter
   where it applies. Scoped per source filing. (Informational; real
   correctness signal is #2.)
2. **Normalized formula ties** — every subtotal in `canonical_buckets.md` ties
   in every quarter using bucket values. Hard requirement. No plugs.
3. **Q1+Q2+Q3+Q4 = annual** — for every flow bucket and every fiscal year
   where all five exist. Exact match.
4. **Cross-statement invariants**:
   - `total_assets == total_liabilities_and_equity` per quarter.
   - `net_change_in_cash == cash_eop − cash_bop` per quarter (cash sourced
     from BS instants).
   - `income_statement.net_income == cash_flow.net_income_start` per quarter.
5. **BS consistency across filings** — any period-end date appearing in
   multiple filings must have identical BS instants across those filings.
6. **Segment ties** — `sum(members) == consolidated_total` per axis, metric,
   and quarter.
7. **Analytical reconciliation** — every bucket value equals the signed sum
   of its source rows' values for that quarter. No orphan values.
8. **Forward-fill audit** — every flagged forward-fill re-checked against the
   target period's raw `parsed_xbrl.json`. Any candidate concept present with
   a non-null value fails the fill.

### Retry loop

Failures are echoed back to the AI with specific concepts, quarters, and
deltas. Retries up to 3. If failures persist, Stage 2 hard-errors (exit
code 2). No silent accept. No plugs.

### Outputs

| File | Purpose |
|------|---------|
| `formula_mapped_v3.json` | Full structure: statements (rows + buckets + formulas), segments, analytical, verification report |
| `quarterly.json` | Flat per-quarter bucket values for `calculate.py` |
| `{ticker}_full_check.csv` | Universal audit CSV: as-reported rows, normalized buckets, formulas, segments per statement |

Outputs land in `ai_extract/{TICKER}/test/` during iteration (`--test` flag),
or `ai_extract/{TICKER}/` in production.

---

## Step 5: R&D History (optional, deterministic)

```bash
python3 ai_extract/extract_rd_history.py --ticker NVDA
```

Standalone, no AI. Reads every downloaded filing's XBRL instance doc,
extracts `us-gaap:ResearchAndDevelopmentExpense` with period-type filtering
(3-month for 10-Qs, 12-month for 10-Ks, Q4 derived from annual − Q1 − Q2 − Q3),
and writes `ai_extract/{TICKER}/rd_history.json` — one record per quarter.

Used only when fewer than 20 quarters have been run through Stage 2.
`calculate.py` reads `quarterly.json` first and gap-fills older quarters
from `rd_history.json` as needed for the 20-quarter capitalization schedule.
See `rd_capitalization_reference.md`.

---

## Step 6: Metrics

```bash
python3 calculate.py --ticker NVDA
```

Reads `quarterly.json` (and `rd_history.json` when needed), computes ROIC,
margins, growth, and other metrics per `formulas.md`. Field names map 1:1
to bucket names declared in `canonical_buckets.md` — no translation layer.
Outputs dashboard data.

---

## What the AI Does vs. Doesn't Do

| Task | Deterministic | AI (judgment) |
|------|---------------|---------------|
| Statement structure | Presentation linkbase | — |
| Formula relationships (Stage 1) | Calculation linkbase | — |
| Hidden-item decomposition | Cal linkbase declares it | — |
| Segment/dimension structure | Def linkbase | — |
| Read precise values where XBRL rounds | — | AI reads HTML (Stage 1) |
| Stage 1 fact completeness | — | AI accounts for every XBRL fact |
| Stage 2 row merge across filings | By xbrl_concept; labels joined | — |
| Stage 2 value selection per quarter | Period-type rule | — |
| Stage 2 Q4 + Q2/Q3 derivations | Pure arithmetic | — |
| Stage 2 bucket assignment | `canonical_buckets.md` fixes the names | AI assigns rows → buckets |
| Stage 2 subtotals + invariants | Computed from bucket values | — |
| Stock splits | — | AI normalizes across periods |
| Annual-only forward-fill | Verifier audits against raw XBRL | AI flags, declares candidate concepts |
| R&D history (pre-Stage-1) | Standard XBRL concept, deterministic | — |

---

## File Map

```
fetch.py                              — Step 1: download filings + linkbases from EDGAR (5 yrs)

ai_extract/
  parse_xbrl.py                       — Step 2: deterministic parse → parsed_xbrl.json
  analyze_statement.py                — Step 3: Stage 1 AI extraction → per-filing .json + mapped.json
  ai_formula.py                       — Step 4: Stage 2 statements + buckets
  export_full_check_csv.py            — Step 4: universal audit CSV renderer
  extract_rd_history.py               — Step 5: deterministic R&D history (optional)
  canonical_buckets.md                — Universal IS/BS/CF bucket lists + invariants
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
  q*_fy*_10*.json                     — Per-filing Stage 1 extractions (immutable training data)
  mapped.json                         — All periods, amendment-aware index
  formula_mapped_v3.json              — Stage 2: statements + buckets + segments + analytical
  quarterly.json                      — Flat per-quarter bucket values (consumed by calculate.py)
  rd_history.json                     — Deterministic R&D history for 20-quarter lookback
  {ticker}_full_check.csv             — Universal audit CSV
  test/                               — Active workspace while iterating

calculate.py                          — Step 6: metrics from quarterly.json + rd_history.json
dashboard/                            — Chart.js app served locally
formulas.md                           — Metric dictionary (references canonical_buckets.md)
rd_capitalization_reference.md        — 20-quarter straight-line, real quarters only
```
