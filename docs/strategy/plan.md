> Superseded for architecture. Current source of truth is `docs/architecture/system.md`.
> Keep this file only as an older strategy snapshot.

The System: 4 Layers

  Layer 1: Financial Data Extraction

  Goal: Clean quarterly component values (revenue, COGS, operating income, etc.) for any company, stored in PostgreSQL.

  Approach:
  - Master script: First-pass extraction. Fetches XBRL data, generates candidates, applies universal resolution rules. Gets 70-80% of components correct for any company.
  - Per-company script: Spawns when you process a specific company. Handles that company's quirks (reporting style, concept choices, period structure). Persists and runs on future filings.
  - Over time: Common fixes from per-company scripts get promoted into the master script. Master script gets stronger, per-company scripts get thinner.

  Inputs: SEC XBRL data (you already have the fetching/parsing pipeline)
  Output: component_observation table in PostgreSQL — ticker, quarter, component_id, value
  Evaluation: Gold audit spreadsheet (DELL, NVDA, PLTR verified) + formulas.md metrics

  Layer 2: Qualitative Data

  Goal: Curated text from filings, transcripts, and news — noise removed, signal stored.

  Three sub-pipelines, same pattern for each:

  A. Filing text (MD&A, risk factors)
  - Source: XBRL/HTML filing sections (AlphaOmega already extracts at 99.3%)
  - Process: Extract text blocks, curate to remove boilerplate, store key items
  - Train a model (Crucible approach) to automate the curation over time

  B. Earnings transcripts
  - Source: SEC 8-K exhibits or transcript APIs
  - Process: Download full transcript, strip introductions/boilerplate/irrelevant Q&A, keep substantive commentary
  - Train a model to identify and extract material statements

  C. News
  - Source: News API (NewsAPI, Polygon, etc.)
  - Process: Ingest headlines/articles, classify materiality, store relevant items
  - Train a model to filter signal from noise

  Output: Text stored in PostgreSQL — ticker, date, source_type, curated_text

  Layer 3: Market Data

  Goal: Stock prices and basic market data.

  - Source: Price API (Polygon, Yahoo Finance, etc.)
  - Straightforward ingestion, no AI needed
  - Output: Prices table in PostgreSQL — ticker, date, OHLCV

  Layer 4: Synthesis (Frontier Model)

  Goal: Forward revenue and earnings estimates with reasoning.

  - Input: Everything from Layers 1-3 for a given company, pulled via SQL queries
  - Engine: Claude or equivalent frontier model
  - Process: Format all data into one prompt, ask for forward estimates with reasoning
  - Your role: Review the reasoning, not the number. Check: did it use the right inputs? Are the assumptions sound?
  - No fine-tuning here — frontier model's broad reasoning is the strength

  ---
  Storage

  PostgreSQL — not markdown, not vector databases, not RAG.

  Tables:
  - financials — quarterly components from Layer 1
  - filing_text — curated filing sections from Layer 2A
  - transcripts — curated transcript excerpts from Layer 2B
  - news — material news items from Layer 2C
  - prices — daily prices from Layer 3

  At query time: SQL pulls everything for a company, formats it into a prompt for the frontier model. Simple.

  ---
  Key Principles

  1. Deterministic code owns: fetching, parsing, period math, storage, lineage
  2. Trained models own: qualitative curation (text extraction, transcript filtering, news classification)
  3. Frontier model owns: synthesis, forward estimation, thesis generation
  4. Per-company work is unavoidable for financial extraction — capture it and reuse it
  5. Don't trust AI output without verified inputs — the whole point of Layers 1-3 is giving the frontier model data you trust
  6. Measure everything — gold audit for financials, field-by-field scoring for extraction models
