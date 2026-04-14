# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Arrow is a financial data extraction and synthesis system. It collects structured financial data, qualitative text, and market data, then uses a frontier model (Claude) to generate forward revenue/earnings estimates with reasoning.

**Current status**: Layer 1 (financial data extraction) is built and working for NVIDIA, Dell, Palantir, Palo Alto Networks, Union Pacific, Freeport-McMoRan, LyondellBasell, Symbotic, and Microsoft. Metric calculations and a web dashboard are built on top. Layers 2-4 are planned but not yet implemented. Storage is JSON files per company for now; PostgreSQL later.

## Architecture: 4 Layers

**Layer 1 — Financial Data Extraction**: Extracts quarterly component values (revenue, COGS, operating income, etc.) from SEC XBRL data. Uses a master script for universal extraction plus per-company scripts for company-specific quirks. Common fixes get promoted from per-company scripts into the master script over time. Evaluated against a gold audit spreadsheet.

**Layer 2 — Qualitative Data**: Three sub-pipelines with trained models for curation:
- **2A Filing text**: MD&A, risk factors from XBRL/HTML
- **2B Earnings transcripts**: From SEC 8-K exhibits or transcript APIs, strip boilerplate, keep substantive commentary
- **2C News**: From news APIs, classify materiality, filter signal from noise

**Layer 3 — Market Data**: Stock prices (OHLCV) from price APIs (Polygon, Yahoo Finance). Straightforward ingestion, no AI.

**Layer 4 — Synthesis**: Frontier model takes all Layer 1-3 data, formats into a prompt, generates forward estimates with reasoning.

## Pipeline Commands

```bash
# 1. Download filings from SEC EDGAR (XBRL + HTML)
python3 fetch.py --cik 0001045810 --ticker NVDA

# 2. Extract 24 quarterly components from local XBRL files
python3 extract.py --ticker NVDA

# 3. Compute R&D capitalization + employee count
python3 compute.py --ticker NVDA

# 4. Evaluate against golden data
python3 eval.py --ticker NVDA --verbose

# 5. Calculate financial metrics (ROIC, growth, margins, etc.)
python3 calculate.py --ticker NVDA
python3 calculate.py --all

# 6. Serve dashboard (then open http://localhost:8080)
python3 -m http.server 8080 --directory dashboard
```

`fetch.py` downloads 6 fiscal years of filings (~20-24 quarters). `extract.py` outputs all derived quarters by default. `compute.py` uses the last 20 extracted quarters for R&D capitalization (20-quarter amortization schedule with actual quarterly R&D, no annual/4 estimates). Company scripts can override bad R&D quarters via `fix_rd_series()`.

## File Structure

- `fetch.py` — Downloads 10-Q/10-K filings (XBRL XML + iXBRL HTML) from SEC EDGAR
- `extract.py` — Parses local XBRL instance documents, extracts 24 components per quarter
- `compute.py` — Post-extraction: R&D capitalization (20-quarter schedule) and employee count from 10-K HTML
- `eval.py` — Compares extracted data against golden eval (28 fields per quarter)
- `calculate.py` — Computes 20+ financial metrics (ROIC, growth, margins, etc.) from extracted data
- `dashboard/index.html` — Single-file web dashboard (HTML + JS + Chart.js) for viewing metrics
- `dashboard/data/` — Generated metric JSON files (gitignored, regenerate with `calculate.py --all`)
- `companies/{ticker}.py` — Per-company concept overrides and post-processing
- `golden/{ticker}.json` — Golden eval data (created from confirmed extraction output once accuracy is verified against `golden_eval.xlsx`)
- `golden_eval.xlsx` — Source of truth for evaluation (manually verified data, strict OOXML format — requires custom parser, openpyxl cannot read it)
- `extraction_logic.md` — Master extraction logic: fiscal year detection, fetch/output windows, derivation, restatements
- `company_specific_types.md` — Catalog of per-company issue types and fix patterns
- `formulas.md` — Canonical metric dictionary (ROIC, reinvestment rate, etc.)
- `rd_capitalization_reference.md` — R&D amortization schedule formula reference
- `ai_extract/` — Experimental AI-powered extraction (see AI Extraction Framework below). Fully isolated from the core deterministic pipeline.
- `gemini-extract-design.md` — Design document for XBRL extraction architecture (taxonomy resolution, linkbase traversal, prompt design)
- `data/filings/{TICKER}/{ACCESSION}/` — Downloaded filings (gitignored)
- `output/{ticker}.json` — Extraction output (gitignored)

## Extraction Details

See `extraction_logic.md` for the full extraction logic: XBRL parsing, DEI-based fiscal year detection, context classification, quarterly derivation, R&D capitalization (20-quarter amortization using actual quarterly data), restatement overrides, stock split handling, employee count extraction, and output filtering.

See `company_specific_types.md` for the catalog of per-company issue types and fix patterns (alternate concepts, dimensioned contexts, DEI tagging errors, restatements, spurious XBRL tags, CF line item breakouts, bad R&D quarters, etc.).

## Evaluation

Golden eval (`golden_eval.xlsx`) contains manually verified data. Eval checks 28 fields per quarter:
- 24 extracted components (revenue, COGS, operating income, R&D, tax, pretax income, net income, interest expense, equity, short/long-term debt, lease liabilities, cash, short-term investments, AR, inventory, AP, total assets, CFO, capex, D&A, acquisitions, SBC, diluted shares)
- 3 R&D capitalization fields (amortization, asset, OI adjustment)
- 1 employee count

"Close" match = within 0.1%, typically $1M from Q4 YTD rounding. All companies currently at 0 mismatches. Run `python3 eval.py --ticker <TICKER> --verbose` for live results.

**Golden eval source**: `golden_eval.xlsx` has three tabs: `manual_audit_entry_v1` (the 24 extracted components + employee count), `researchanddevelopment` (R&D capitalization inputs), and `restatements`. The spreadsheet uses strict OOXML format — openpyxl cannot read it, requires a custom XML parser (see `eval.py` pattern). Golden JSON files (`golden/{ticker}.json`) are created from extracted data once extraction accuracy is confirmed, not parsed from the spreadsheet.

**Golden eval window vs extraction output**: The golden eval covers an arbitrary 12-quarter window per company (chosen during manual verification). This is a validation window, not an output constraint. `extract.py` outputs **all** derived quarters from downloaded filings — the eval compares only the overlapping quarters. More extraction output is better: downstream consumers (calculate.py, dashboard, future Layer 4 synthesis) benefit from longer history (TTM needs 4 quarters, YoY needs 8, ROIIC needs more). Once all 20 target companies are validated, the golden eval becomes a regression test — new filings get extracted automatically without golden data, and the eval proves the extraction logic hasn't regressed on known-good data.

## Adding a New Company

1. `fetch.py --cik <CIK> --ticker <TICKER>` — download filings
2. `extract.py --ticker <TICKER>` — run master extraction
3. `compute.py --ticker <TICKER>` — compute R&D capitalization + employee count
4. Compare extraction output against `golden_eval.xlsx` manually (parse the spreadsheet with the custom XML parser)
5. Fix mismatches — check `company_specific_types.md` for known issue patterns. Add `companies/{ticker}.py` if needed.
6. For R&D: build the 20-quarter R&D tab in the spreadsheet, verify amort/asset/OI match compute.py output, copy totals to the main eval tab.
7. Once all fields match: create `golden/{ticker}.json` from the confirmed extraction output, run `eval.py --ticker <TICKER> --verbose` to confirm 0 mismatches.

## Financial Metrics (calculate.py)

Reads `output/{ticker}.json`, computes all metrics from `formulas.md`, writes enriched JSON to `dashboard/data/{ticker}.json`. Metrics include ROIC, ROIIC, reinvestment rate, revenue/gross profit growth, margins, cash quality ratios, CCC, net debt, interest coverage, and more. See `formulas.md` for the full canonical metric dictionary.

**Stock split normalization**: Diluted share counts in extraction output are stored as-reported (pre-split for historical periods). `calculate.py` detects splits by looking for >=1.5x (forward) or <=0.67x (reverse) jumps between adjacent quarters, then normalizes all values to the most recent basis in a `diluted_shares_split_adjusted_q` field. Raw extraction data is never modified. This was a deliberate design decision — keep extraction faithful to filings, normalize in the calculations layer.

**Metric availability**: TTM metrics need 4 quarters of history, YoY metrics need 8. With 6 fiscal years of fetched filings, all derived quarters are available for metric computation. The more history, the earlier full metrics become available.

## Dashboard

Single-file HTML app (`dashboard/index.html`) with Chart.js. Company selector, metrics table grouped by section, click any row to chart it over time. Served via `python3 -m http.server 8080 --directory dashboard`. The `dashboard/data/` directory is gitignored — run `calculate.py --all` to regenerate after a fresh clone.

## Key Principles

- Deterministic code owns: fetching, parsing, period math, storage, lineage
- Trained models own: qualitative curation (text extraction, transcript filtering, news classification)
- Frontier model owns: synthesis, forward estimation, thesis generation
- Per-company work is unavoidable for financial extraction — capture it and reuse it
- Master script concept lists should include every standard US-GAAP concept for a line item. If it's a standard concept for the same component, it goes in master — not in a company override. Company scripts are for truly bespoke issues (dimensioned contexts, sign flips, summation quirks). The goal is for company #7 through #100 to work without per-company scripts wherever possible.
- Don't trust AI output without verified inputs — Layers 1-3 exist to give the frontier model data you trust
- Measure everything — gold audit for financials, field-by-field scoring for extraction models

## AI Extraction Framework

**Status: Testing. Fully isolated in `ai_extract/`. Must NEVER modify or impact the core deterministic pipeline (`extract.py`, `compute.py`, `eval.py`, `calculate.py`, `companies/`, `golden/`, `output/`).**

AI-powered extraction replaces per-company scripts and XBRL concept mappings. A frontier model (Sonnet today, post-trained open source model later) reads the full filing and extracts all three financial statements in a single API call. The model is stateless — it extracts exactly what the filing says. All merging, restatement tracking, and analytical logic is handled by deterministic code downstream.

### Three-Stage Pipeline

**Stage 1 — Per-Filing Extraction** (`analyze_statement.py` → `ai_extract/{TICKER}/q1_fy26_10q.json`)
- One JSON file per filing. Immutable after creation.
- The model reads full filing HTML + XBRL facts and extracts every line item from IS, BS, CF with values, XBRL concept mappings, hierarchy, formulas, and cross-statement checks.
- Also extracts calculation components (operating leases, D&A breakdown, pure AP/AR, capex, acquisitions, short-term debt, SBC, gross interest expense, tax rate, inventory breakdown).
- Formula verification proves correctness. If any CF section (CFO/CFI/CFF) doesn't balance, automatic retry finds missing items.
- Extracts ALL periods reported — current and comparatives. Cost: ~$1.70-2.90/filing.
- **This is training data** for post-training an open source model.

**Stage 2 — Formula Field Mapping** (`ai_formula.py` → `ai_extract/{TICKER}/formula_mapped.json`)
- A second AI pass that reads the extraction JSON (not the filing) and maps each line item to standardized formula field names (`revenue_q`, `cogs_q`, `equity_q`, etc.).
- The model does the semantic understanding — no XBRL concept lists, no label pattern matching, no lookup tables. It knows "Cost of revenue" is `cogs_q` regardless of the XBRL concept or company terminology.
- Handles sign conventions, unit conversions (millions to raw), and flags whether CF values are YTD.
- Uses `calculation_components` for items that need special handling (operating lease totals, segmented capex, multiple acquisition lines, hidden short-term debt).
- One result per filing. Cost: ~$0.10/filing.

**Stage 3 — Quarterly Derivation** (`ai_formula.py --from-mapped` → `ai_extract/{TICKER}/quarterly.json`)
- Pure arithmetic. No AI, no pattern matching. Takes the per-filing formula mappings and derives quarterly values:
  - Q1 10-Q: values are already quarterly, use as-is.
  - Q2/Q3 10-Q: IS is quarterly (use as-is), CF is YTD (subtract prior quarter's YTD).
  - 10-K: IS and CF are annual (subtract Q1+Q2+Q3 to get Q4). Shares/EPS use annual value directly (can't be derived by subtraction — they're weighted averages).
- Smart merge: when multiple filings report the same period, only overwrites if the value is **different** (restatement). Same or absent values preserved. Changes logged in `restatements` array.
- **This is analytical data.** Feeds `calculate.py`, the dashboard, and Layer 4 synthesis.

### How It Works

1. **Download**: `fetch.py` downloads the filing (shared with deterministic pipeline). Downloads 10-Q, 10-K, 10-Q/A, and 10-K/A filings.
2. **Clean HTML**: Strip CSS/styling from iXBRL HTML (~57% size reduction). Keeps all tags, text, and ix:nonFraction elements.
3. **Parse XBRL**: `ai_extract/parse_xbrl.py` extracts all facts from the XBRL instance document into structured data.
4. **Extract** (`analyze_statement.py`): Send cleaned HTML + XBRL facts to the model. Extracts every line item from IS, BS, CF with values, XBRL concept mappings, hierarchy, formulas, calculation components, and cross-statement checks. If any CF section doesn't balance, automatic retry finds missing items.
5. **Map** (`ai_formula.py`): Second AI pass reads the extraction JSON and assigns standardized formula field names. No lookup tables — the model does the semantic mapping.
6. **Derive** (`ai_formula.py --from-mapped`): Pure arithmetic derives quarterly values from YTD/annual data. Smart merge handles restatements across filings.

### Verification Model

Financial statements are a closed system. The math proves correctness.
- IS: Revenue - COGS = Gross Profit, flows down to Net Income. All subtotals verified.
- BS: Assets = Liabilities + Equity. All component sums verified.
- CF: CFO + CFI + CFF = Change in Cash. Beginning + Change = Ending. All section sums verified.
- Cross-statement: Net income, cash, and retained earnings must tie across all three.

If all formulas pass and cross-statement checks pass, the extraction is mathematically proven correct. No golden eval needed for the three financial statements — the math IS the eval.

### Amendments and Restatements

The model doesn't need to understand amendments. It extracts what the filing says — period.

- **Regular 10-K with restated comparatives** (e.g., Dell restates prior years after a disposition): The model extracts all columns including restated values. The mapper updates those periods with the restated values.
- **10-K/A or 10-Q/A** (explicit amendment filing): The model extracts the corrected statements. The mapper updates the affected periods.
- **Stock splits**: Newer filings restate prior-period share counts to post-split basis. The model extracts the restated values. The mapper records the change.

In all cases: the model is stateless, the mapper is stateful.

**Smart merge rule**: When multiple filings report the same period, the mapper only overwrites a field if the new value is **different** from the existing value. Same or absent values are preserved. This prevents incomplete prior-period data (e.g., a later filing missing the current portion of operating leases) from overwriting complete data from the original filing. Changes are logged in a `restatements` array on the record.

### File Structure

```
ai_extract/
  analyze_statement.py    — Stage 1: extraction (--statement all, --model, --ticker, --accession)
  ai_formula.py           — Stage 2+3: formula mapping + quarterly derivation
  parse_xbrl.py           — Deterministic XBRL fact parser
  map_to_extract.py       — Legacy concept-based mapper (kept for deterministic pipeline comparison)
  map_by_label.py         — Deprecated label-based mapper (superseded by ai_formula.py)
  view_extraction.py      — Renders extraction JSON as readable table
  export_for_review.py    — Exports extraction to CSV for human review
  prompt_layer2_draft.md  — Design document for Layer 2 calculation components
  {TICKER}/
    q1_fy26_10q.json      — Per-filing extraction (immutable, training data)
    q2_fy26_10q.json
    q3_fy26_10q.json
    q4_fy26_10k.json
    formula_mapped.json   — Per-filing formula field mappings (AI output)
    quarterly.json        — Quarterly records (derived, analytical data)
    mapped.json           — Legacy concept-based mapped records
```

### Cost

- Stage 1 (extraction): ~$1.70-2.90 per filing with Sonnet (varies with filing size).
- Stage 2 (formula mapping): ~$0.10 per filing with Sonnet.
- Total: ~$2-3 per filing end-to-end.
- 100 companies × 4 quarters/year = ~$800-1,200/year with Sonnet.
- Post-trained open source model: target is near-zero marginal cost at scale.

### Tested Results

- NVDA FY24-FY26 (12 filings, 12 quarters): **264/264 fields match golden eval exactly (0 mismatch, 0 missing)**. All Q1-Q4 derivations correct. All formula verifications pass. No lookup tables or per-company code needed.
- FCX Q1 CY24: 18/18 formulas pass, 93/93 XBRL matches, 24/24 fields match deterministic extraction exactly for current period.

### Relationship to Core Pipeline

The AI extraction is an alternative path to the same output. Both coexist:
- **Deterministic pipeline**: Proven, free to run, requires per-company scripts and manual golden eval. 24 fields per quarter.
- **AI extraction**: Costs per filing, no per-company scripts needed, formula-verified automatically. ~70 fields per quarter. Training data for post-trained model.
- During development, the 24-field overlap between both pipelines is compared to validate accuracy.
- Long-term: the AI extraction replaces the deterministic pipeline once validated across all target companies. The post-trained open source model replaces Sonnet to eliminate per-filing costs.
