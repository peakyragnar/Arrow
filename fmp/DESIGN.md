# Arrow Plan

## Transition Note

This document is the current architecture direction for Arrow.

It marks the largest architecture change in the repo so far:
- old direction: SEC/XBRL-first extraction pipeline centered on `ai_extract/`
- new direction: FMP-first historical ingest, SEC fast-path for fresh filings, PostgreSQL as system of record, search-first analyst workflow, and point-in-time-aware data model

`ai_extract/` should be treated as the last full design iteration before the FMP pivot.
It remains valuable as archived reference and benchmark context, but it is no longer the active system design.

`deterministic-flow/` is archived as well and should not guide new implementation.

## Goal

Build Arrow into a searchable, replayable, time-aware company-intelligence system.

Core outcome:
- trusted financial data
- trusted company documents
- trusted market data
- trusted macroeconomic data
- trusted event history
- frontier-model analyst on top
- future post-training data generated as a byproduct of normal operation

Not:
- naive RAG
- MCP-heavy plumbing
- training infra now

Do:
- design so post-training later is clean and high-value

## Core Architecture Rules

- PostgreSQL = system of record
- no `pgvector` in v1
- no MCP in core system
- search-first retrieval
- FMP = primary source for historical ingest and normal operation
- SEC direct = narrow low-latency path only for newly dropped filings
- Massive = planned options vendor later; deferred until paid
- macro data is first-class and time-aligned to company data
- raw responses cached and replayable
- artifacts immutable
- facts derived from artifacts
- ingestion agent and analyst agent separate
- point-in-time correctness is universal, not macro-only — applies to financials (restatements) and to any data a vendor may revise

## Foundational Schema Rule: Two Clocks Always

Every relevant object must preserve both:

### 1. Fiscal truth
Company-reported reality.

Store:
- `fiscal_year`
- `fiscal_quarter`
- `fiscal_period_label`
- `period_end`

Example:
- MSFT quarter ending June 30 = `FY2024 Q4`

This is canonical.
This is how company filings, transcripts, guidance, and management commentary are actually expressed.

### 2. Calendar normalization
Cross-company and macro comparison frame.

Store:
- `calendar_year`
- `calendar_quarter`
- `calendar_period_label`

Example:
- MSFT `FY2024 Q4` ending June 30 also maps to `CY2024 Q2`

Purpose:
- compare companies on same real-world quarter
- align with macro, news, prices, options, and events
- support cross-company screens and synthesis

Rule:
- fiscal truth always preserved
- calendar normalization always added where relevant
- not optional
- not "just a view detail"
- foundational schema rule

## Time-Aware Model

Everything temporally connected.

Need to answer:
- what was known as of April 1
- what happened after April 1
- which filing/event/transcript came first
- which source published first
- which macro regime was in effect
- whether later normalized data superseded preferred downstream use

Relevant fields where applicable:
- `published_at`       — when the source released this value/text
- `observed_at`        — when the observation applies (e.g., macro observation_date)
- `effective_at`       — when the datum becomes authoritative from the company's POV
- `ingested_at`        — when Arrow fetched it
- `period_end`
- `fiscal_year`, `fiscal_quarter`
- `calendar_year`, `calendar_quarter`
- `source_priority`
- `superseded_at`      — set when a later value/artifact replaces this one

This is not a latest-state system.
This is a time-aware evidence system.

### Point-in-time rule applies to financials, not just macro

Companies restate. 10-K/A is a first-class event, not an edge case. `financial_facts` must carry `published_at` and `superseded_at`, and PIT queries must resolve "value known as of date D" the same way they do for macro revisions. See `financial_facts` schema below.

## Why Search-First

Financial analyst queries mostly:
- exact number
- exact phrase
- exact date / quarter
- cross-document comparison
- structured joins

So retrieval stack:

1. SQL for structured facts
2. metadata filters
3. Postgres full-text search
4. read section/span
5. read full artifact
6. synthesize with citations

Embeddings only after a real failing query proves need.

## Source Strategy

### FMP now

Use FMP for historical backfill and normal operation:
- filings / filing text
- earnings transcripts
- historical prices
- financial statement endpoints
- some event/calendar coverage

Flow:
`FMP REST -> raw_responses -> artifacts / artifact_chunks / financial_facts / prices / events`

FMP = ingest source.
Not data model.

### SEC direct for fresh filings only

Use SEC direct only when a new filing drops and latency matters.

Applies to:
- 10-Q
- 10-K
- material 8-Ks

Purpose:
- ingest immediately
- parse immediately
- create artifact + sections + provisional events/facts before FMP catches up

Rule:
- historical backlog: FMP-first
- new filing arrival: SEC-first
- after FMP catches up: ingest FMP too
- preserve both sources
- prefer FMP later where normalized structure is better
- keep SEC as first-seen source and provenance anchor

### FMP vs SEC reconciliation is an ongoing job, not a one-time check

When both sources exist for the same filing/period/concept, run a scheduled reconciliation that flags divergences. Trust in FMP is only earned empirically; without this job we're trusting by assertion. See Build Order step 9.5.

### Massive later

Use Massive later for:
- options contracts
- options EOD snapshots
- maybe realtime options later

But:
- schema now
- no ingest now

Flow later:
`Massive -> raw_responses -> options_contracts / options_eod_snapshots`

### Macro data sources later

Need macro data sources for:
- interest rates
- inflation
- unemployment
- GDP / industrial production
- housing / credit / spreads
- other regime-defining indicators

Likely sources later:
- FRED
- BLS
- Treasury
- BEA
- other official sources as needed

Macro should be modeled as its own first-class domain, not stuffed into generic notes.

### Later sources

Need room for:
- news vendors
- video transcripts
- presentations / decks
- research notes / industry explainers
- direct SEC/XBRL expansion

Schema stays vendor-neutral.

## Core Data Model

### Artifacts vs Facts

#### Artifacts
Immutable source objects.

Examples:
- 10-K, 10-Q, 8-K
- transcript
- press release
- news article
- presentation deck
- video transcript
- research explainer
- product primer
- macro release note / report

Rules:
- append-only
- never mutate content
- corrected version supersedes prior artifact (new row, `supersedes` pointer, prior row gets `superseded_at`)
- content-hashed twice:
  - `raw_hash`        — hash of raw bytes as received (exact replay, cross-pull dedup)
  - `canonical_hash`  — hash of canonicalized/normalized text (same filing across format or vendor-wrapper variations dedups to one content identity)
- source provenance attached

#### Facts
Structured data derived from artifacts or vendor payloads.

Examples:
- revenue, gross margin
- guidance statement
- filing event
- price bar
- options snapshot
- macro series observation
- derived signal

Rules:
- provenance required (pointer back to source artifact or raw_response)
- regeneratable from source
- versioned by extraction logic (`extraction_version`)

## Regeneratability Invariants

Stated explicitly so future-us doesn't drift:

- `artifacts` are source truth — never regenerated, only superseded.
- `artifact_chunks` are regeneratable from artifacts — if chunking strategy changes, truncate and re-derive. Cheap under FTS-only.
- `financial_facts` are regeneratable from artifacts + raw_responses — if extraction logic changes, bump `extraction_version` and re-derive; preserve prior row with `superseded_at`.
- If embeddings are ever added: they are regeneratable from chunks. Chunks do not depend on embeddings.

Direction of dependency: `raw_responses → artifacts → chunks → (optional) embeddings`; `raw_responses/artifacts → facts`. Never the reverse.

## Training-Ready By Design

Not training now.
But structure must create future post-training data naturally.

Need to preserve:

### 1. Source truth
- original artifacts
- raw API payloads
- exact source metadata
- hashes
- ingest lineage

### 2. Derived structure
- chunks / sections / speaker turns
- extracted facts
- events
- macro observations
- alignments / diffs later

### 3. Analyst traces
- query
- filters
- retrieved evidence ids
- SQL used
- tool sequence
- answer
- citations
- user correction / approval
- model/version metadata

### 4. Evaluation signals
- accepted / rejected answers
- corrected facts
- revised summaries
- alert quality outcomes

This creates future training datasets for:
- extraction
- retrieval policy
- citation behavior
- analyst reasoning
- alerting quality
- macro-aware reasoning

Rule:
normal system use should emit future training data automatically.

### Privacy / consent boundary on qa_log

`qa_log` is the most valuable training asset and the one most likely to contain private or thesis-sensitive content. Treat as training-data candidate only under explicit consent rule:

- Single-operator today → default-include, user can mark rows `training_opt_out = true`.
- Multi-user later → must be explicit opt-in per row or per user, enforced in the export path, not just by convention.
- Even in single-user mode, tag `sensitivity` on rows touching live positions / non-public deliberation so training-set construction can filter cleanly.

Bake the columns now; enforcement is just a flag read at export time.

## v1 Tables

- `companies`
- `artifacts`
- `artifact_chunks`
- `financial_facts`
- `prices_daily`
- `prices_intraday`            -- schema only
- `options_contracts`          -- schema only for now
- `options_eod_snapshots`      -- schema only for now
- `macro_series`
- `macro_observations`
- `company_events`
- `signals`
- `alerts`
- `watchlists`
- `ingest_runs`
- `raw_responses`
- `qa_log`

## Table Intent

### `raw_responses`
Exact vendor/source payload cache.

Use:
- replay
- dedupe
- audit
- avoid rehitting vendor

Stores:
- vendor/source
- endpoint
- params
- response body (JSONB for JSON; bytea for binary)
- `raw_hash`        — exact payload bytes as received
- `canonical_hash`  — canonicalized payload/text hash where applicable
- fetched_at
- ingest_run_id

### `ingest_runs`
Operational log.

Stores:
- run type
- source/vendor
- ticker scope
- start/end
- status
- counts
- errors
- code/version

### `artifacts`
Canonical documents.

Stores:
- company / ticker if company-specific
- source
- source document id
- artifact type
- title
- date
- `published_at`
- `effective_at`
- fiscal fields if relevant
- calendar fields if relevant
- full text / payload / metadata
- `raw_hash`, `canonical_hash`
- `supersedes` → artifact_id (nullable)
- `superseded_at` (nullable; set when a later artifact replaces this)
- ingest_run_id

Research-corpus rows (industry primers, product explainers, macro primers) carry additional columns:
- `authored_by`
- `created_at`
- `last_reviewed_at`
- `asserted_valid_through`

Rationale: domain context rots. Analyst agent must be able to down-weight stale primers.

### `artifact_chunks`
Derived retrieval units. Regeneratable from `artifacts`.

Deterministic units first:
- filing sections
- transcript speaker turns
- timestamp spans
- slide text blocks
- table blocks
- research-note sections
- macro commentary sections

Stores:
- artifact_id
- chunk_type
- section
- speaker
- offsets / timestamps
- ordinal
- text
- tsvector (GIN-indexed)
- fiscal/calendar metadata where relevant
- filter metadata
- `chunker_version`

### `financial_facts`
Canonical long/skinny financial store.

Stores:
- ticker
- fiscal year / quarter / label
- calendar year / quarter / label
- period_end
- statement
- concept / component_id
- value
- unit
- source artifact or payload
- `extraction_version`
- `published_at`      — when this value was released by the source
- `superseded_at`     — when a later row replaces this (e.g., 10-K/A restates a 10-Q)
- status / confidence if needed

PIT lookup pattern:
```sql
SELECT value FROM financial_facts
WHERE ticker = $1 AND concept = $2 AND period_end = $3
  AND published_at <= $asof
  AND (superseded_at IS NULL OR superseded_at > $asof)
ORDER BY published_at DESC LIMIT 1;
```

Rule:
- fiscal truth always stored
- calendar normalization always stored
- PIT columns always populated

### `prices_daily`
Daily OHLCV history.

Calendar-native.
Fiscal joins via derived views/query logic.

### `prices_intraday`
Reserved for event/reaction workflows.

### `options_contracts`
Static options instrument definitions.
Deferred until Massive active.

### `options_eod_snapshots`
Time-varying options market data.
Deferred until Massive active.

Calendar-native.
Fiscal joins via derived views/query logic.

### `macro_series`
Defines each macro series.

Examples:
- Fed funds rate
- 2Y Treasury, 10Y Treasury
- CPI YoY, core CPI YoY
- unemployment rate
- payrolls
- GDP growth
- industrial production

Stores:
- series_id (PK)
- name
- category
- frequency
- source
- units
- seasonal_adjustment
- transformation notes

### `macro_observations`
Time-series observations for macro data.

Stores:
- series_id
- observation_date   — the period the value describes
- value
- `published_at`     — when the release went out
- `vintage`          — release tag (`advance`, `second`, `third`, `2024-07-comprehensive-revision`, ...)
- source
- ingest_run_id

Composite PK: `(series_id, observation_date, vintage)`.

PIT lookup pattern:
```sql
SELECT value FROM macro_observations
WHERE series_id = $1 AND observation_date = $2
  AND published_at <= $asof
ORDER BY published_at DESC LIMIT 1;
```

Rule:
- macro is calendar-native
- mapped to company fiscal/calendar periods in derived views when needed
- vintage is mandatory; "latest value" is a derived view, not the storage truth

### `company_events`
First-class event anchors.

Examples:
- earnings date
- guidance update
- split, dividend
- material 8-K event
- investor day
- product launch
- executive change
- major filing arrival
- press release
- material news item
- M&A announcement
- regulatory action
- lawsuit / investigation
- major macro release relevance if linked later

Stores:
- event datetime
- event type
- company / ticker
- linked artifact ids where relevant
- source/provenance pointer
- fiscal fields if relevant
- calendar date always

Important:
- news article itself is an artifact
- press release itself is an artifact
- material event derived from those artifacts is a company event
- multiple articles may map to one event
- one article may mention multiple events

Event != signal.

### `signals`
Derived machine/agent interpretations.

Examples:
- guidance tone worsened
- inventory risk rising
- unusual reaction into earnings
- rate-sensitive multiple risk rising

Not source truth.

### `alerts`
Delivered or queued notifications tied to signals / events / watchlists.

### `watchlists`
User-defined company / rule scopes.

### `qa_log`
Analyst interaction log.

Stores:
- query
- filters
- evidence ids
- tool sequence
- SQL / retrieval trace where useful
- answer
- citations
- user correction / acceptance
- model metadata
- `training_opt_out` (bool, default false)
- `sensitivity` (enum: `public` | `internal` | `position_sensitive`)

Part of the future training-data design, governed by consent/sensitivity flags at export time.

## Retrieval Design

### Agent tools

- `list_documents(ticker, doc_type, date_range, fiscal_period, calendar_period)`
- `search_documents(query, filters)`
- `read_document(id)`
- `read_document_section(id, section)`
- `sql_query(...)`
- `get_macro_series(series_id, date_range, asof=None)`   — PIT-aware
- `get_financial_fact(ticker, concept, period, asof=None)` — PIT-aware
- later: `list_events(...)`, `get_price_window(...)`, `get_options_window(...)`

### Retrieval order

1. identify company + fiscal/calendar scope
2. query SQL facts
3. query macro series if relevant
4. query events
5. FTS search over chunks with metadata filters
6. read exact sections/spans
7. read full artifact if needed
8. synthesize with citations

### Deterministic recipes by query type

#### Financial trend
"What changed in gross margin over 4 quarters?"
- SQL first
- commentary second if needed

#### Guidance
"What was NVDA's Q2 FY26 data-center guidance?"
- transcript / prepared remarks / guidance sections
- phrase search
- exact section read
- cite exact text

#### Calendar-normalized comparison
"Compare CY2024 Q2 margins across mega-cap tech"
- calendar-normalized financial views
- supporting commentary second

#### Macro-sensitive comparison
"How did semis behave as rates rose in 2022?"
- macro series (with vintage — use the values available then, not today's revisions)
- calendar-normalized company views (PIT — use values published by then)
- event/news/commentary support
- synthesize

#### Event reaction
"What happened into earnings?"
- company_events
- prices window
- later options window
- related docs search
- synthesize

#### Cross-quarter commentary
"Did the CFO walk that back next quarter?"
- search by speaker + topic across adjacent transcripts
- read both
- compare

## Like-for-Like Comparison Over Time

Need explicit support for same-kind text over time.

Targets:
- risk factors
- MD&A
- prepared remarks
- Q&A
- recurring management themes

Requires:
- stable section extraction
- section-level indexing
- later section alignment / diff logic

Use cases:
- subtle risk-factor change detection
- tone shift in demand/pricing/inventory/China/supply/competition
- guidance framing changes over time

Core analyst feature.

## Research Corpus

Need durable reference knowledge.

Examples:
- how a GPU product works
- industry structure
- supply chain notes
- memory architecture primer
- competitor landscape
- company history / business model explainers
- macro regime explainers

Treat as artifacts too, with types like:
- `research_note`
- `industry_primer`
- `product_primer`
- `macro_primer`

Carry freshness metadata (`authored_by`, `created_at`, `last_reviewed_at`, `asserted_valid_through`) so the analyst agent can down-weight stale domain context.

Reason:
analyst answers need:
- company facts
- event history
- domain context
- macro context

## Agent Split

### Ingestion agent
Owns:
- fetch, cache, normalize, index, extract
- rerun, observability, failure handling

Properties:
- deterministic where possible
- replayable
- auditable
- append-only discipline

### Analyst agent
Owns:
- retrieval, comparison, explanation
- Q&A, alerting, watchlist monitoring

Properties:
- search-first
- citation-first
- tool-using
- debuggable

Do not combine roles.

## Always-On Monitoring

Later system = event-driven.

Flow:
- watch source feeds
- detect new filings / news / transcripts / price moves / macro releases
- ingest artifacts/events
- run deterministic checks
- invoke analyst agent when triggered
- write `signals`
- send `alerts`

Always-on != chatbot always running.
Means monitored feeds + triggered analysis.

## Layer Plan

### Layer 1: Financial Data

Goal: trusted quarterly and annual financial facts in Postgres.

Primary v1 source:
- FMP financial statement endpoints
- FMP as-reported endpoints
- FMP filing-based financial data where useful

Fast path for new filings only:
- SEC direct filing/XBRL ingest on filing drop

Important:
- existing SEC/XBRL pipeline remains core benchmark and asset
- preserve ability to compare FMP-derived data against direct SEC/XBRL outputs
- always store both fiscal truth and calendar normalization
- always store PIT columns (`published_at`, `superseded_at`)

Outputs:
- `financial_facts`
- fiscal summary views
- calendar-normalized summary views
- PIT views (`fact_asof(asof_date)`)

Evaluation:
- gold audit spreadsheet
- formula checks
- cross-statement consistency
- FMP vs SEC/XBRL comparison over time (see Build Order 9.5)

### Layer 2: Qualitative Data

Goal: searchable company text with provenance.

#### 2A. Filings
Historical/default source: FMP filing endpoints / filing text / filing JSON.
Fresh filing fast path: SEC direct ingest for newly dropped 10-Q / 10-K / material 8-K.

Output:
- `artifacts`
- `artifact_chunks`
- filing-derived `company_events`

Preserve:
- filing date
- fiscal period
- calendar date

#### 2B. Transcripts
v1 source: FMP earnings transcript endpoints.

Store:
- full transcript artifact
- speaker-turn chunks
- prepared remarks / Q&A structure where possible
- fiscal period if tied to earnings
- calendar date always

#### 2C. News
Schema now. Pipeline later. Likely non-FMP or mixed vendor.

#### 2D. Video / presentations
Later but planned now.

Store:
- video transcript artifacts
- timestamp spans
- presentation artifacts
- slide text blocks

#### 2E. Research / explainers
Store:
- durable domain/context notes
- industry primers
- product explainers
- macro explainers
- with freshness metadata

### Layer 3: Market + Macro Data

Goal: trusted price, options, and macro history.

#### Prices
v1: `prices_daily`, reserve `prices_intraday`.
Source: FMP.
Calendar-native. Mapped to fiscal periods in derived views when useful.

#### Options
v1 schema only: `options_contracts`, `options_eod_snapshots`.
Planned source: Massive.
Not building ingest until paid.
Calendar-native. Mapped to fiscal periods in derived views when useful.

#### Macro
First-class macro series and observations.

Examples:
- interest rates, unemployment, inflation, payrolls, GDP, industrial production, credit spreads, housing

Calendar-native. Mapped to company calendar/fiscal periods in derived views when useful.
Vintage-preserving.

### Layer 4: Synthesis

Goal: frontier-model analyst over trusted data.

Input:
- SQL facts (PIT-aware)
- macro series (PIT-aware)
- event tables
- FTS-retrieved evidence
- full artifacts where needed
- research corpus where needed
- fiscal views where needed
- calendar-normalized views where needed

Method:
- iterative tool use
- not one-shot retrieval
- answer must cite evidence

Outputs:
- answers
- comparisons
- alerts
- monitoring
- later thesis / estimate support

## Derived Views

Need analyst-friendly views early.

Examples:
- quarterly financial summary by fiscal period
- quarterly financial summary by calendar-normalized period
- PIT financial summary (`fact_asof(asof_date)`)
- valuation summary
- macro overlay views
- macro latest-value view (convenience over vintage PK)
- event timeline
- guidance history
- management commentary timeline
- later: options sentiment summary
- reconciliation divergence view (FMP vs SEC/XBRL)

Store canonical data long. Expose views wide.

## Provenance Rules

Every important row should answer:
- where did this come from
- when was it fetched
- which vendor / source
- which artifact supports it
- which ingest run created it
- which extraction version produced it
- (for revisable data) which vintage / published_at

No provenance, no trust.

## Operational Rules

- REST ingest only
- cache raw API responses always
- artifacts immutable; corrections via supersedes
- artifacts content-hashed twice (raw + canonical)
- chunks derived and regeneratable from artifacts
- facts derived and regeneratable from artifacts/raw_responses; versioned by `extraction_version`
- if embeddings are ever added, they are regeneratable from chunks
- signals not source truth
- alerts delivery objects
- no MCP in core system
- historical ingest FMP-first
- fresh-filing ingest SEC-first only for low-latency cases
- fiscal truth always preserved
- calendar normalization always added where relevant
- macro series first-class and time-aligned
- PIT columns mandatory on revisable domains (`financial_facts`, `macro_observations`)
- normal operation should emit future training/eval traces
- qa_log respects `training_opt_out` and `sensitivity` at export
- add complexity only after concrete failing use case

## Local First, Cloud Later

Start local:
- local Postgres
- local raw cache
- local workers/scripts

Design for easy migration:
- Postgres-compatible from day 1
- env-driven config
- raw artifacts move from filesystem to object storage later
- workers move from scripts to scheduled/queued services later

Later cloud target:
- managed Postgres
- object storage
- scheduled workers / queues
- durable alert delivery

Local first for iteration. Cloud later for durability and reliability.

## Build Order

1. finalize schema doc
2. stand up local Postgres
3. define fiscal-truth + calendar-normalization rules clearly
4. define macro-series model and mapping rules clearly
5. define training-trace requirements clearly
6. implement `raw_responses` + `ingest_runs`
7. implement `artifacts` + `artifact_chunks` (with double-hash)
8. implement FMP ingest for historical filings and transcripts
9. implement `financial_facts` with fiscal, calendar, and PIT fields
9.5. implement FMP ↔ SEC/XBRL reconciliation job + divergence view (validates FMP empirically before trusting)
10. implement `macro_series` + `macro_observations` (vintage-preserving)
11. implement fiscal, calendar-normalized, and PIT derived views
12. implement `prices_daily`
13. implement `company_events`
14. implement analyst retrieval tools (PIT-aware)
15. implement `qa_log` as part of normal analyst flow (with consent flags)
16. add section-over-time comparison support
17. add `signals` + `alerts`
18. add SEC fast-path ingest for newly dropped filings
19. add Massive-backed options ingest later
20. migrate to cloud when durability/reliability justify it

## Decision Rules

- prefer source-neutral schema, source-explicit ingest
- prefer FTS + metadata + SQL over embeddings
- prefer append-only over mutation
- prefer deterministic extraction over hidden heuristics
- prefer evidence-backed answers over fluent answers
- prefer simpler infra now
- use FMP for history
- use SEC direct only for fresh filing latency
- always preserve fiscal truth and add calendar normalization
- always preserve PIT (`published_at`, `superseded_at` / `vintage`) on revisable data
- treat macro as first-class, time-aligned data
- design normal operation to create future post-training data
- respect qa_log consent boundary at export
- add options ingest when Massive is active
- add embeddings only when a concrete query fails under FTS + metadata + SQL
