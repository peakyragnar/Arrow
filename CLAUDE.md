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
- `ai_extract/` — Experimental AI-powered extraction: deterministic XBRL→JSON parser + Claude maps facts to 24 standard fields (no per-company scripts)
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
