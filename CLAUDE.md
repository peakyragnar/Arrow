# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Arrow is a financial data extraction and synthesis system. It collects structured financial data, qualitative text, and market data, then uses a frontier model (Claude) to generate forward revenue/earnings estimates with reasoning.

**Storage**: PostgreSQL (tables: `financials`, `filing_text`, `transcripts`, `news`, `prices`).

## Architecture: 4 Layers

**Layer 1 — Financial Data Extraction**: Extracts quarterly component values (revenue, COGS, operating income, etc.) from SEC XBRL data. Uses a master script for universal extraction (~70-80% accuracy) plus per-company scripts for company-specific quirks. Common fixes get promoted from per-company scripts into the master script over time. Output: `component_observation` table (ticker, quarter, component_id, value). Evaluated against a gold audit spreadsheet.

**Layer 2 — Qualitative Data**: Three sub-pipelines with trained models for curation:
- **2A Filing text**: MD&A, risk factors from XBRL/HTML (AlphaOmega extracts at 99.3%)
- **2B Earnings transcripts**: From SEC 8-K exhibits or transcript APIs, strip boilerplate, keep substantive commentary
- **2C News**: From news APIs, classify materiality, filter signal from noise

**Layer 3 — Market Data**: Stock prices (OHLCV) from price APIs (Polygon, Yahoo Finance). Straightforward ingestion, no AI.

**Layer 4 — Synthesis**: Frontier model takes all Layer 1-3 data (pulled via SQL), formats into a prompt, generates forward estimates with reasoning. Review the reasoning and assumptions, not just the numbers.

## Key Principles

- Deterministic code owns: fetching, parsing, period math, storage, lineage
- Trained models own: qualitative curation (text extraction, transcript filtering, news classification)
- Frontier model owns: synthesis, forward estimation, thesis generation
- Per-company work is unavoidable for financial extraction — capture it and reuse it
- Don't trust AI output without verified inputs — Layers 1-3 exist to give the frontier model data you trust
- Measure everything — gold audit for financials, field-by-field scoring for extraction models
