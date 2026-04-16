# CLAUDE.md

Arrow is a financial data extraction and synthesis system. It extracts structured financial data from SEC filings using AI, computes analytical metrics, and will ultimately generate forward estimates with reasoning.

## Principles

- **The AI replaces the analyst.** It reads the filing, verifies every formula relationship, reads precise values from the HTML, and accounts for every tagged fact — the same work a human analyst does manually. It is not given a template of fields to fill in.
- **Linkbase data is the foundation.** The SEC filing includes calculation, presentation, and definition linkbase files that declare every formula, statement structure, and dimension hierarchy. These are parsed deterministically and fed to the AI as structured input. The AI verifies — it does not discover structure.
- **Math proves correctness.** Financial statements are a closed system. Every subtotal must tie to its components, every cross-statement relationship must hold. If the math passes, the extraction is proven correct. No golden eval needed for the three statements — the math IS the eval.
- **Nothing unaccounted for.** Every XBRL-tagged fact must be placed — either on the statement face or identified as hidden/aggregated. The AI cannot skip items.
- **Extract as reported, normalize downstream.** Stage 1 extracts exactly what the filing says. Stage 2 does the analytical work (decomposing aggregated items, normalizing labels across filings). Stage 3 is pure arithmetic.
- **No forced mappings.** The AI is never given a list of fields to look for. It reads the data and figures out what's there. This applies to financial statements, segments, and all disaggregation data.
- **Per-filing extractions are training data.** Every extraction is stored immutably for training a post-trained model to replace API costs.

## Pipeline

```bash
# 1. Download filings + XBRL linkbases from SEC EDGAR
python3 fetch.py --cik 0001045810 --ticker NVDA

# 2. Deterministic parse: facts + linkbases → structured JSON
python3 ai_extract/parse_xbrl.py --ticker NVDA --accession <ACCESSION>

# 3. Stage 1: AI verifies formulas, reads precise values, extracts statements + segments
python3 ai_extract/analyze_statement.py --ticker NVDA --accession <ACCESSION> --statement all

# 4. Stage 2: AI normalizes labels + decomposes aggregated items across filings
python3 ai_extract/ai_formula.py --ticker NVDA

# 5. Stage 3: Pure arithmetic — YTD to quarterly derivation
python3 ai_extract/ai_formula.py --ticker NVDA --from-mapped

# 6. Calculate financial metrics (ROIC, growth, margins, etc.)
python3 calculate.py --ticker NVDA

# 7. Serve dashboard
python3 -m http.server 8080 --directory dashboard
```

## File Map

```
fetch.py                    — Downloads 10-Q/10-K/amendments + XBRL linkbases from SEC EDGAR
calculate.py                — Computes metrics from quarterly.json, writes dashboard data
formulas.md                 — Canonical metric dictionary (ROIC, margins, growth, etc.)
rd_capitalization_reference.md — R&D amortization schedule formula reference
golden_eval.xlsx            — Manually verified financial data (source of truth)
dashboard/                  — Single-file HTML app (Chart.js), served locally

ai_extract/
  parse_xbrl.py             — Deterministic parser: XBRL facts + linkbases → parsed_xbrl.json
  analyze_statement.py      — Stage 1: AI extraction + verification (reads parsed_xbrl.json + HTML)
  ai_formula.py             — Stage 2: analytical mapping + Stage 3: quarterly derivation
  ai_extraction_flow_full.md — Full pipeline design doc (detailed)
  ai_extraction_flow.md     — Original pipeline design doc (reference)
  {TICKER}/
    q*_fy*_10*.json         — Per-filing extractions (immutable, training data)
    mapped.json             — All periods by period, handles amendments/restatements
    formula_mapped.json     — Normalized fields per filing
    quarterly.json          — Standalone quarterly values (single source of truth)

data/filings/{TICKER}/{ACCESSION}/  — Downloaded filings (gitignored)
  *.htm                     — Filing HTML (iXBRL)
  *_htm.xml                 — XBRL instance document (all tagged facts)
  *_cal.xml                 — Calculation linkbase (declared formulas with weights)
  *_pre.xml                 — Presentation linkbase (concept-to-statement mapping)
  *_def.xml                 — Definition linkbase (dimension hierarchies)
  filing_meta.json          — Filing metadata
  parsed_xbrl.json          — Deterministic parse output (Step 2)

deterministic-flow/         — Archived deterministic pipeline (extract.py, eval.py, etc.)
```

## Pipeline Detail

See `ai_extract/ai_extraction_flow_full.md` for the complete design.

**Step 1 — Download** (`fetch.py`): Downloads filing HTML, XBRL instance document, and three XBRL linkbase files (calculation, presentation, definition) from SEC EDGAR.

**Step 2 — Deterministic Parse** (`parse_xbrl.py`): Parses all XBRL files into `parsed_xbrl.json`. Extracts every tagged fact, every declared formula with signed weights, every concept-to-statement mapping, and every dimension hierarchy. No AI — pure parsing.

**Step 3 — AI Extraction** (`analyze_statement.py`): AI receives the parsed linkbase data + stripped filing HTML (just the three financial statement tables, not the full filing — 78% token reduction). The linkbase data tells it the structure — which concepts belong on which statement, what the formula relationships are, what dimensions exist. The AI's job is to verify every formula ties, read precise values from HTML where XBRL rounds, account for every tagged fact, and extract all disaggregation data. If verification fails on the first pass, retries with full HTML to resolve missing items from the notes. Outputs per-filing JSON (training data) + updates mapped.json.

**Step 4 — Cross-Filing Normalization** (`ai_formula.py`): AI reads extractions across filings. Normalizes labels (XBRL concept names are consistent across filings, reducing this work). Decomposes aggregated items using note-level data from Step 3. Verifies no double counting.

**Step 5 — Quarterly Derivation** (`ai_formula.py --from-mapped`): Pure arithmetic. Q1 pass-through, Q2/Q3 YTD subtraction, Q4 = annual minus Q1+Q2+Q3. BS snapshots, no derivation. Segments follow same IS logic.

**Step 6 — Metrics** (`calculate.py`): Reads quarterly.json, computes ROIC, margins, growth per formulas.md.

## What the AI Does vs. Doesn't Do

| Task | Deterministic (linkbase) | AI (judgment) |
|------|--------------------------|---------------|
| Statement structure | Presentation linkbase provides it | — |
| Formula relationships | Calculation linkbase declares them | — |
| Hidden items (e.g. leases in accrued) | Cal linkbase declares decomposition | — |
| Segment/dimension structure | Def linkbase provides hierarchy | — |
| Precise values where XBRL rounds | — | Reads HTML for exact numbers |
| Math verification | — | Confirms all formulas tie |
| Fact completeness | — | Accounts for every XBRL fact |
| Cross-filing label normalization | XBRL concepts reduce the work | Handles edge cases |
| YTD to quarterly | Pure arithmetic (Stage 3) | — |

## Architecture: 4 Layers

- **Layer 1 — Financial Data**: AI extraction from SEC XBRL filings (built, validated for NVDA)
- **Layer 2 — Qualitative Data**: MD&A, earnings transcripts, news (planned)
- **Layer 3 — Market Data**: Stock prices from price APIs (planned)
- **Layer 4 — Synthesis**: Frontier model generates forward estimates (planned)

## Current Status

- NVDA: 12 filings extracted, 264/264 fields match golden eval (template-based prompt, tagged as `v1-template-prompt`)
- Linkbase-based Stage 1 prompt: tested on NVDA Q1 FY26, IS/BS match verified exactly. Cost: $1.41/filing (Sonnet, stripped HTML). No retries needed.
- Linkbase download + parse working for all 9 companies (24 NVDA filings, 169 total across all tickers)
- Deterministic pipeline archived in `deterministic-flow/` (9 companies validated, 0 mismatches)
- Stage 2 prompt rewrite pending
- `calculate.py` reads from deterministic output — switchover to `quarterly.json` pending
