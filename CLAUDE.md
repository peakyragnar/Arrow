# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Arrow is a financial data extraction and synthesis system. It collects structured financial data, qualitative text, and market data, then uses a frontier model (Claude) to generate forward revenue/earnings estimates with reasoning.

**Current status**: Layer 1 (financial data extraction) is built and working for NVIDIA. Layers 2-4 are planned but not yet implemented. Storage is JSON files per company for now; PostgreSQL later.

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
python3 fetch.py --cik 0001045810 --ticker NVDA --fy-start 2024 --fy-end 2026

# 2. Extract 24 quarterly components from local XBRL files
python3 extract.py --ticker NVDA --fy-start 2024 --fy-end 2026

# 3. Compute R&D capitalization + employee count
python3 compute.py --ticker NVDA --fy-start 2024 --fy-end 2026

# 4. Evaluate against golden data
python3 eval.py --ticker NVDA --verbose
```

## File Structure

- `fetch.py` — Downloads 10-Q/10-K filings (XBRL XML + iXBRL HTML) from SEC EDGAR
- `extract.py` — Parses local XBRL instance documents, extracts 24 components per quarter
- `compute.py` — Post-extraction: R&D capitalization (20-quarter schedule) and employee count from 10-K HTML
- `eval.py` — Compares extracted data against golden eval (28 fields per quarter)
- `companies/{ticker}.py` — Per-company concept overrides and post-processing
- `golden/{ticker}.json` — Golden eval data exported from `golden_eval.xlsx`
- `golden_eval.xlsx` — Source of truth for evaluation (manually verified data)
- `formulas.md` — Canonical metric dictionary (ROIC, reinvestment rate, etc.)
- `rd_capitalization_reference.md` — R&D amortization schedule formula reference
- `data/filings/{TICKER}/{ACCESSION}/` — Downloaded filings (gitignored)
- `output/{ticker}.json` — Extraction output (gitignored)

## XBRL Extraction Details

We parse actual XBRL instance documents from each filing, NOT the SEC companyfacts aggregation API. The companyfacts API has rounding issues from YTD value aggregation.

**Flow item handling by statement type:**
- **Income statement** (IS): Q1-Q3 have discrete quarterly values in their 10-Q XBRL. Use them directly.
- **Cash flow** (CF): Only YTD cumulative values exist in 10-Q XBRL. Derive quarterly: Q2 = H1_YTD - Q1, Q3 = 9M_YTD - H1_YTD.
- **Q4 for both IS and CF**: Derived from FY (10-K) minus 9M YTD (Q3 10-Q). Introduces ~$1M rounding.
- **Balance sheet**: Instant (point-in-time) values, no derivation needed.
- **Diluted shares**: Per-period metric. Use discrete quarterly entry; fall back to FY context for Q4.

**XBRL parsing notes:**
- Contexts can be `instant` (BS) or `duration` (IS/CF), with or without dimension segments
- Only use non-dimensioned contexts for consolidated totals
- iXBRL HTML files contain duplicate fact entries for the same context — parser deduplicates
- Classify contexts by period length: ~90 days = quarterly, ~180 = H1, ~270 = 9M, ~365 = FY
- The prior-period 10-K must be fetched for R&D annual history and employee count baseline

## R&D Capitalization (compute.py)

20-quarter straight-line amortization schedule:
- **Amort(t)** = sum(R&D(t-j) for j=0..19) / 20
- **Asset(t)** = sum(R&D(t-j) × (20-j)/20 for j=0..19)
- **OI Adjustment(t)** = R&D(t) - Amort(t)

Data source: 3 prior fiscal years of annual R&D from the 10-K before our extraction window (each divided by 4 for quarterly estimates) + 12 actual quarters = 24-quarter series. Missing quarters in the lookback window are treated as 0 (denominator stays 20).

## Employee Count (compute.py)

Not available in XBRL structured data. Extracted from 10-K HTML by finding the largest number matching the pattern `N employees`. Carried forward from the most recent 10-K until the next annual filing.

## Company Script Pattern

Master script handles ~70-80% of components for any company. Per-company scripts in `companies/{ticker}.py` handle:
- **Concept name overrides**: Companies use different XBRL concept names (e.g., NVIDIA changed CapEx concept between fiscal years)
- **Post-processing**: Fix edge cases like multiple acquisition lines (NVIDIA Groq), concept reclassifications
- Over time, common fixes get promoted into the master script

See `companies/nvda.py` for the reference implementation.

## Evaluation

Golden eval (`golden_eval.xlsx`) contains manually verified data. Eval checks 28 fields per quarter:
- 24 extracted components (revenue, COGS, operating income, R&D, tax, pretax income, net income, interest expense, equity, short/long-term debt, lease liabilities, cash, short-term investments, AR, inventory, AP, total assets, CFO, capex, D&A, acquisitions, SBC, diluted shares)
- 3 R&D capitalization fields (amortization, asset, OI adjustment)
- 1 employee count

"Close" match = within 1%, typically $1M from Q4 YTD rounding. Current NVDA: 336/336 fields, 0 mismatches.

## Key Principles

- Deterministic code owns: fetching, parsing, period math, storage, lineage
- Trained models own: qualitative curation (text extraction, transcript filtering, news classification)
- Frontier model owns: synthesis, forward estimation, thesis generation
- Per-company work is unavoidable for financial extraction — capture it and reuse it
- Don't trust AI output without verified inputs — Layers 1-3 exist to give the frontier model data you trust
- Measure everything — gold audit for financials, field-by-field scoring for extraction models
