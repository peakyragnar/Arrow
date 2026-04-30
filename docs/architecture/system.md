# Arrow Plan

## Transition Note

This document is the current architecture direction for Arrow.

It marks the largest architecture change in the repo so far:
- old direction: SEC/XBRL-first extraction pipeline centered on `archive/ai_extract/`
- new direction: FMP-first historical ingest, SEC fast-path for fresh filings, PostgreSQL as system of record, search-first analyst workflow, and point-in-time-aware data model

`archive/ai_extract/` should be treated as the last full design iteration before the FMP pivot.
It remains valuable as archived reference and benchmark context, but it is no longer the active system design.

`archive/deterministic-flow/` is archived as well and should not guide new implementation.

## Goal

Build Arrow into a searchable, replayable, time-aware company-intelligence system.

## Read This First

If you are new to the repo, keep this distinction hard:

- **normal flow** = FMP baseline facts + SEC documents + FMP transcripts + FMP↔XBRL audit (auto-promotes safe corrections, surfaces the rest)
- **standalone audit** = `scripts/reconcile_fmp_vs_xbrl.py` — re-run anytime against already-stored facts

Short version:
- baseline financial source of truth: FMP, with continuous XBRL verification
- SEC role: `8-K` / `10-Q` / `10-K` documents, freshness, filing text, **and** XBRL anchor values for FMP correctness checking
- audit role: integrated into normal flow as a soft gate — auto-promotes XBRL on safe-bucket corruption (direct-tagged, FY≥2022, gap<25%, unambiguous concept) with full provenance via `supersedes_fact_id` + `xbrl-amendment-{is|bs|cf}-v1` extraction_version. Unsafe-bucket divergences don't auto-rewrite — they land in the steward queue (`xbrl_audit_unresolved`) for analyst adjudication.

For the shortest version, read:
- `docs/architecture/normal_vs_audit.md`

Core outcome:
- baseline financial history from FMP, with provenance and PIT semantics
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
- SEC/XBRL = audit rail and amendment rail; auto-promotes only on the unambiguous corruption bucket (direct-tagged anchors, recent FY, moderate gap, primary IS/CF concepts). Unsafe-bucket divergences surface for analyst review rather than rewriting baseline.
- Massive = planned options vendor later; deferred until paid
- macro data is first-class and time-aligned to company data
- raw responses cached and replayable
- artifacts immutable
- facts derived from artifacts
- ingestion agent and analyst agent separate
- point-in-time correctness is universal, not macro-only — applies to financials (restatements) and to any data a vendor may revise

## Two Flows

### 1. Normal product flow

This is the mainline.

Operational default:

```bash
uv run scripts/ingest_company.py TICKER
```

This one command runs the normal company flow:
- seed company from SEC bootstrap
- backfill baseline FMP financial facts
- ingest FMP product and geography revenue segments
- ingest FMP employee counts
- backfill SEC `10-K` / `10-Q` qualitative filings (default 5 fiscal years,
  rounded to complete fiscal years from each company's `fiscal_year_end_md`;
  `index.json` + primary filing doc for 10-K/Q; earnings 8-Ks also retain
  detected earnings-release exhibits)

- FMP historical financial ingest -> `financial_facts`
- FMP revenue segmentation ingest -> dimensioned `financial_facts` rows
- SEC qualitative filing ingest -> `artifacts` + `artifact_sections` +
  `artifact_section_chunks`; earnings EX-99 press releases -> `press_release`
  artifacts + `artifact_text_units` + `artifact_text_chunks`
- FMP transcript ingest -> `artifacts`
- later: news/events/retrieval/synthesis

This is the path to optimize first.

### 2. Audit flow

This is separate.

- FMP vs SEC/XBRL comparison
- amendment detection
- divergence review
- benchmark support
- `data_quality_flags`

Important:
- audit is kept
- audit is useful
- audit is **not** the default ingest path

### Script roles

- `scripts/ingest_company.py` — default company run; normal flow end-to-end
- `scripts/backfill_fmp.py` — FMP-only financial backfill
- `scripts/ingest_segments.py` — FMP product/geography revenue segment refresh only
- `scripts/ingest_employees.py` — employee metric refresh only
- `scripts/fetch_sec_filings.py` — SEC `10-K` / `10-Q` qualitative backfill only (default 5-year window, primary docs only)
- `scripts/reconcile_fmp_vs_xbrl.py` — standalone audit; same logic that runs automatically in `ingest_company.py`. Use to re-audit a ticker on demand.
- `scripts/promote_xbrl_for_corruption.py` — apply XBRL supersession with safety filters; same library function the normal flow uses (`arrow.agents.xbrl_audit.audit_and_promote_xbrl`). Use for backfill / re-promotion outside the normal flow.
- `scripts/triage_xbrl_divergences.py` — classify divergences from past audit runs into definitional / corruption / ambiguous buckets; supports filtering by ticker or bucket.
- `scripts/correct_corrupted_q4_is.py` — manual override for FMP Q-fabrication corruption that auto-promote can't safely fix (e.g., entire Q row corrupt, derive from `annual − sum(other quarters)`).
- `scripts/backfill_q4_period_end.py`, `scripts/backfill_cross_endpoint_period_end.py` — one-shot backfills for the date-stamping classes of bug; idempotent.

## Foundational Schema Rule: Two Clocks Always

Full spec: `docs/reference/periods.md` — canonical field names, derivation algorithms, 52/53-week handling, Q4 rule, YTD→discrete, label formats, invariants. This section states the principle; `periods.md` is authoritative.

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
- daily price + volume history (OHLCV)
- financial statement endpoints (normalized + as-reported)
- metrics, ratios, segmentation
- some event/calendar coverage

Flow:
`FMP REST -> raw_responses -> artifacts / financial_facts / prices_daily / events` (chunks added later when document text is ingested)

FMP = ingest source.
Not data model.

Raw filesystem cache lives under `data/raw/fmp/` with endpoint-mirrored layout (see Raw Cache Layout below).

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

### FMP baseline ingest with XBRL audit gate

Historical `financial_facts` are FMP-first, then XBRL-verified.

Default historical ingest:
- fetch FMP
- normalize into `financial_facts`
- load revenue segmentation as dimensioned `financial_facts`
- enforce Layer 1 load-time statement sanity
  - hard: IS subtotal ties, BS balance identity, CF cash roll-forward
  - soft: BS/CF subtotal-component drift -> `data_quality_flags`
- preserve PIT/vendor revision history
- **run FMP↔XBRL anchor reconciliation** (`arrow.agents.xbrl_audit.audit_and_promote_xbrl`)
  - auto-promote safe-bucket divergences: direct-tagged XBRL value,
    fiscal_year ≥ 2022, gap < 25%, primary IS/CF concept
    (revenue, gross_profit, operating_income, net_income, cfo/cfi/cff)
  - promotion writes a new row at extraction_version
    `xbrl-amendment-{is|bs|cf}-v1` and supersedes the FMP row with
    `supersession_reason='xbrl-disagrees: accn ...'`. The wide view
    preferences `xbrl-amendment-*` over `fmp-*` so the corrected value
    wins downstream automatically.
  - unsafe-bucket divergences (audit-derived Q4, definitional-prone
    concepts like total_equity / ebt_incl_unusual, large gaps,
    pre-2022 years) stay in `ingest_runs.error_details` and surface as
    `xbrl_audit_unresolved` steward findings for analyst review.

The audit step CAN be skipped with `--no-xbrl-audit` for bandwidth-
constrained or no-network re-ingests, but the default is on.

SEC remains active for:
- fresh filing arrival
- raw `8-K` earnings releases
- raw `10-Q` / `10-K` documents
- filing-text extraction
- XBRL anchor values for FMP correctness verification (above)
- amendment detection (separate codepath in `arrow.agents.amendment_detect`)

Manual audit-and-promote tooling (`scripts/reconcile_fmp_vs_xbrl.py`,
`scripts/promote_xbrl_for_corruption.py`,
`scripts/triage_xbrl_divergences.py`) lets operators re-audit on
demand and override safety filters when an analyst has manually
verified a divergence against the actual filing.

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
- `financial_facts` are regeneratable from artifacts + raw_responses — if extraction logic changes, bump `extraction_version` and re-derive; preserve prior row with `superseded_at`.
- `artifact_sections` / `artifact_section_chunks` and `artifact_text_units` /
  `artifact_text_chunks` are regeneratable from artifacts — if extraction or
  chunking strategy changes, truncate and re-derive. Cheap under FTS-only.
- If embeddings are ever added: they are regeneratable from section chunks. Chunks do not depend on embeddings.

Direction of dependency: `raw_responses → artifacts → artifact_sections → artifact_section_chunks → (optional) embeddings`; `raw_responses → artifacts → artifact_text_units → artifact_text_chunks`; `raw_responses/artifacts → facts`. Never the reverse.

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

Status legend:
- **built** — schema applied, model + tests in place
- **deferred** — named in v1 surface, no schema yet (will be added when its data source is real)
- **withdrawn** — was built, then removed; see linked migration

| Table | Status | Notes |
|---|---|---|
| `ingest_runs` | built | migration 002 |
| `raw_responses` | built | migration 003 |
| `artifacts` | built | migration 004; SEC filing identity fields extended in migration 014 |
| `companies` | built | migration 007 |
| `financial_facts` | built | migration 008; segment dimension identity added in migration 016 |
| `artifact_sections` | built | migration 014; canonical extracted filing sections (`10-K`, `10-Q`) |
| `artifact_section_chunks` | built | migration 014; standardized retrieval chunks derived from `artifact_sections` |
| `artifact_text_units` | built | migration 015; generic extracted text units for non-10-K/Q artifacts, starting with earnings press releases; transcript unit type added in migration 020; `company_id` tightened to NOT NULL in migration 021 to match `artifact_sections` |
| `artifact_text_chunks` | built | migration 015; standardized retrieval chunks derived from `artifact_text_units` |
| `artifact_chunks` | withdrawn | migration 005 added it; migration 006 dropped it. Re-add when chunking has real documents to operate on. ADR-0008 captures the prior design. |
| `securities` | built | migration 023 — tradable instruments (common stock, ETFs, indices). `companies.primary_security_id` resolves "ticker NVDA" to a security row. ETFs/indices have NULL `company_id`. Accommodates multi-class shares (GOOG/GOOGL) without future migration. See `docs/architecture/prices_ingest_plan.md`. |
| `prices_daily` | built | migration 023 — daily OHLCV per security. `close` = raw as-traded (FMP `historical-price-eod/non-split-adjusted`); `adj_close` = split + dividend adjusted total-return basis (FMP `historical-price-eod/dividend-adjusted`). |
| `historical_market_cap` | built | migration 023 — daily market cap series from FMP `historical-market-capitalization`. Stored as a fact rather than derived from `price × shares` to capture intra-filing buyback/issuance moves cleanly. |
| `prices_intraday` | deferred | reserved for event-reaction workflows |
| `options_contracts` | deferred | until Massive vendor active |
| `options_eod_snapshots` | deferred | until Massive vendor active |
| `series` | deferred | unified time-series substrate. `domain` discriminator covers macro / industry / commodity. One series = one fully-defined scope (FRED model). |
| `series_observations` | deferred | scalar observations against `series`. Vintage-preserving; same PIT/supersession contract as `financial_facts`. |
| `company_events` | deferred | — |
| `signals` | deferred | — |
| `alerts` | deferred | — |
| `watchlists` | deferred | — |
| `qa_log` | deferred | wired up the moment the analyst flow exists; consent flags must be enforced from the first interaction |
| `coverage_membership` | withdrawn | added in migration 017 with a `tier` column (`core` / `extended`); migration 018 dropped the tier column during the V1.1 simplification; migration 019 dropped the table entirely. The membership concept was an opt-in layer over `companies` that didn't earn its keep — every seeded ticker should be tracked by the steward by default. The steward now reads `companies` directly. See `docs/architecture/steward.md` § V1.2. |
| `data_quality_findings` | built | migration 017 — steward-produced findings with two-state lifecycle (`open` → `closed` with structured `closed_reason`); audit captured in `history` jsonb. Distinct from inline-validation `data_quality_flags`; UNIONed by `v_open_quality_signals` (`db/queries/15_*.sql`) for dashboard. See `docs/architecture/steward.md`. |
| `triage_session` | built | migration 022 — per-operator-session capture of chat-driven triage work (Claude Code / Codex). Records intent, finding_ids, operator_quotes, investigations, actions_taken, outcomes, and a one-sentence captured_pattern. The V1 substrate for the future autonomous data-quality operator agent — these rows ARE the training corpus. See `src/arrow/steward/sessions.py` and `docs/architecture/steward.md` § LLM Trajectory. |

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
- SEC filing identity where relevant: `company_id`, `fiscal_period_key`, `form_family`, `cik`, `accession_number`, `raw_primary_doc_path`
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

### `artifact_sections`
Canonical extracted narrative units for SEC qualitative retrieval.

Stores:
- parent `artifact_id`
- `company_id`
- `fiscal_period_key`
- `form_family`
- `section_key`
- section title / part / item labels
- full extracted section `text`
- extraction offsets
- `extractor_version`
- `confidence`
- `extraction_method`

Rules:
- amendments are additive — original filing sections stay intact
- section composition key is `(company_id, fiscal_period_key, form_family, section_key)`
- fallback `unparsed_body` keeps the retrieval contract uniform when a filing cannot be sectioned cleanly

### `artifact_section_chunks`
Standardized retrieval chunks derived from `artifact_sections`.

Stores:
- parent `section_id`
- `chunk_ordinal`
- faithful `text`
- normalized `search_text`
- `heading_path`
- offsets
- `chunker_version`

Use:
- chunk-level FTS
- passage ranking
- citation-ready model context packets

### `artifact_chunks` *(withdrawn from v1)*
Built in migration 005, dropped in 006 before any chunking happened. Superseded by the section-first `artifact_sections` / `artifact_section_chunks` design that landed in migration 014 for SEC filings. ADR-0008 remains useful background for generated `tsvector` design.

### `financial_facts`
Canonical long/skinny financial store.

Stores:
- ticker
- fiscal year / quarter / label
- calendar year / quarter / label
- period_end
- statement
- concept / component_id
- dimension fields for segment facts:
  `dimension_type`, `dimension_key`, `dimension_label`, `dimension_source`
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
- segment rows use `statement = 'segment'`, `concept = 'revenue'`, and
  non-null dimension fields; non-segment rows keep dimension fields NULL

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

Runtime design: `docs/architecture/analyst_runtime.md` defines the analyst
answer spine (`Intent -> Plan -> Retrieve -> Ground -> Synthesize -> Verify ->
Trace -> Channel`) used by CLI, chat, saved prompts, and future monitoring.

### Agent tools

- `list_documents(ticker, doc_type, date_range, fiscal_period, calendar_period)`
- `search_documents(query, filters)`
- `read_document(id)`
- `read_document_section(id, section)`
- `get_latest_transcripts(ticker, n=4, asof=None)`
- `search_transcript_turns(ticker, query, fiscal_period_key=None, asof=None)`
- `get_transcript_context(ticker, fiscal_period_key, asof=None)`
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

### Steward agent
Owns:
- vigilance over data state: coverage, completeness, quality, freshness, lineage integrity
- finding lifecycle (open → closed with structured reason)
- never mutates source data; surfaces and proposes; operator (or autonomous-promoted check) executes via existing ingest/action paths

Properties:
- mostly deterministic in V1 (SQL checks); LLM-as-judge added in V2 for prose-judgment failure modes
- per-check automation level (human_only → suggest_only → auto_with_review → autonomous); promotion happens by demonstrated correctness on that check, not globally
- audit-first (every state change captured with `actor` + reason)

See `docs/architecture/steward.md` for the full runtime, V1 build slice, and V1→V3 LLM trajectory.

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
- existing SEC/XBRL pipeline remains a benchmark/audit asset
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
- optional cross-source and cross-statement audit passes
- optional FMP vs SEC/XBRL comparison as a separate audit pass (Layer 5 — see `docs/reference/verification.md` § 6; divergences surface in `data_quality_flags`)

### Layer 2: Qualitative Data

Goal: searchable company text with provenance.

#### 2A. Filings
Historical/default source: FMP filing endpoints / filing text / filing JSON.
Fresh filing fast path: SEC direct ingest for newly dropped 10-Q / 10-K / material 8-K.

Output:
- `artifacts`
- `artifact_sections`
- `artifact_section_chunks`
- `press_release` artifacts from 8-K exhibits where present
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

Runtime design: `docs/architecture/analyst_runtime.md`.

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
- action traces for correctness review and performance optimization

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

## Raw Cache Layout

Every ingested vendor response is cached to the filesystem under `data/raw/{vendor}/` in addition to being written to the `raw_responses` table. Filesystem cache is belt-and-suspenders for offline replay, local grepping, and disaster recovery; the `raw_responses` table is the canonical replay index once Postgres is live.

### Rule: endpoint-mirrored, deterministic

For vendors with a stable REST path structure (FMP, SEC, FRED, Massive), the sub-path after `data/raw/{vendor}/` mirrors the vendor's endpoint path. The final segments encode request params deterministically so that given an HTTP request, the cache path is a pure function of (endpoint, params).

### FMP layout

```
data/raw/fmp/{endpoint-path}/{TICKER}/{key}.json
```

Where:
- `{endpoint-path}` is the FMP stable endpoint path with `/` preserved (e.g., `historical-price-eod/full`).
- `{TICKER}` is the symbol the request is scoped to, if any. Unscoped endpoints (e.g., market-wide latest) omit this segment.
- `{key}` encodes the request params deterministically:
  - period-sliced: `annual.json`, `quarter.json`
  - year-sliced: `2024.json`, `2025.json`
  - fiscal-period: `2025-Q2.json`
  - date-range: `{from}_{to}.json`
  - single-shot: ticker filename at the leaf (e.g., `profile/NVDA.json`)

### Examples

```
data/raw/fmp/income-statement/NVDA/annual.json
data/raw/fmp/income-statement/NVDA/quarter.json
data/raw/fmp/balance-sheet-statement/NVDA/quarter.json
data/raw/fmp/cash-flow-statement/NVDA/quarter.json
data/raw/fmp/income-statement-as-reported/NVDA/quarter.json
data/raw/fmp/key-metrics/NVDA/quarter.json
data/raw/fmp/ratios/NVDA/quarter.json
data/raw/fmp/revenue-product-segmentation/NVDA/annual.json
data/raw/fmp/revenue-geographic-segmentation/NVDA/annual.json
data/raw/fmp/historical-price-eod/full/NVDA/2024.json
data/raw/fmp/historical-price-eod/full/NVDA/2025.json
data/raw/fmp/earning-call-transcript/NVDA/2025-Q2.json
data/raw/fmp/earning-call-transcript-dates/NVDA.json
data/raw/fmp/profile/NVDA.json
```

### Why endpoint-mirrored, not category-mirrored

- Deterministic from the HTTP request — no hand-maintained endpoint→category map.
- New FMP endpoints integrate without a taxonomy decision.
- Backfill by endpoint is a simple subtree copy; backfill by ticker is a find/glob.
- Matches `raw_responses` row granularity, so the filesystem and DB share one natural key.

### SEC layout (same principle)

```
data/raw/sec/filings/{CIK}/{ACCESSION}/{filename}
```

Already partially in place (`data/raw/sec/filings/`). Keep accession-rooted directories; one filing = one accession directory, contents are the fetched HTML/XBRL files.

### Notes

- Filesystem cache is a convenience; `raw_responses` is authoritative for replay and provenance.
- Params that cannot be deterministically encoded in a filename (long query strings, complex filters) fall back to a hash of the canonical param set as the `{key}` segment.
- Tickers are UPPERCASE in paths.
- No symlinks, no mutation — write-once, replace by writing a new file and letting the old one age out of the working set.

## Operational Rules

- REST ingest only
- cache raw API responses always (DB `raw_responses` + filesystem under `data/raw/{vendor}/`)
- raw filesystem cache layout mirrors vendor endpoint path (see Raw Cache Layout)
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

**Intended target: Hetzner Cloud.** VM with self-managed Postgres 16 plus Hetzner Storage Box (or Cloudflare R2) for raw cache. Chosen for cost (~$10–35/mo vs $80–150/mo on hyperscaler equivalents) and tooling parity — local Homebrew pg 16 is the same Postgres binary, so migration is `pg_dump | restore` + a `DATABASE_URL` swap. Managed alternatives (Render, Neon) remain viable fallbacks; migration between them is not a trap.

Local first for iteration. Cloud later for durability and reliability.

## Build Order

Status markers (✅ done · 🚧 in progress · ⏳ next · ⬜ not started). When a step lands, append the migration / PR that delivered it.

1. ✅ finalize schema doc
2. ✅ stand up local Postgres
3. ✅ define fiscal-truth + calendar-normalization rules clearly (`docs/reference/periods.md`)
4. ✅ define macro-series model and mapping rules clearly (this doc, § Macro)
5. ✅ define training-trace requirements clearly (this doc, § qa_log + § Training-Ready By Design)
6. ✅ implement `raw_responses` + `ingest_runs` (migrations 002, 003)
7. ✅ implement `artifacts` (migration 004, with double-hash). SEC qualitative filing identity extensions landed in migration 014.
8. ✅ implement FMP ingest for historical filings and transcripts.
   Historical financials, revenue segmentation, and FMP earnings-call
   transcripts are built and backfilled for the active US-filer universe.
   Transcript sub-steps:
   - ✅ FMP earnings transcript endpoint client + raw-response cache
     (mirror `src/arrow/ingest/fmp/income_statement.py` shape;
     deterministic cache path `data/raw/fmp/earning-call-transcript/{TICKER}/FY{YYYY}-Qn.json`).
   - ✅ Normalize transcripts into `artifacts` (artifact_type
     `'transcript'`, with speaker turns in `artifact_text_units` per the
     existing text_units pattern from migration 015; transcript unit type
     added in migration 020).
   - ✅ Wire into `scripts/ingest_company.py` normal flow so every
     subsequent ingest includes transcripts automatically.
   - ✅ Backfill transcripts across the existing active companies
     (one-time operational pass).
   - ✅ Per the working rule "new verticals ship with their checks":
     add transcript-vertical expectations to
     `src/arrow/steward/expectations.py` + add steward checks
     (presence, recency, orphan detection) for the new vertical.
     Calibrate thresholds against live data before coding (per
     `feedback_calibrate_thresholds_first` memory).
   - Estimated effort: ~1 day for ingest + normalize + wire,
     ~half day for steward checks + tests.
9. ✅ implement and populate `financial_facts` schema with fiscal, calendar, PIT, and segment-dimension fields (migrations 008, 016; segment ingest built 2026-04-24).
9.5. ✅ implement FMP ↔ SEC/XBRL audit rail (migrations 010 + 011, built 2026-04-21/22). **Activated in normal flow on 2026-04-27** via `arrow.agents.xbrl_audit.audit_and_promote_xbrl`, called from `scripts/ingest_company.py` after FMP backfill. Auto-promotes safe-bucket corruption with `xbrl-amendment-{is|bs|cf}-v1` extraction_version and full supersession provenance; unsafe-bucket divergences land in the steward queue via `xbrl_audit_unresolved` for analyst adjudication. Companion tooling: `scripts/reconcile_fmp_vs_xbrl.py` (standalone audit), `scripts/promote_xbrl_for_corruption.py` (manual promotion override), `scripts/triage_xbrl_divergences.py` (bucket classifier). Three new structural integrity checks landed alongside: `q4_period_end_consistency`, `cross_endpoint_period_end_consistency`, `quarterly_sum_to_annual_drift`. Amendment-detect remains a separate codepath under `arrow.agents.amendment_detect`.
10. ⬜ implement `series` + `series_observations` (unified macro / industry / commodity substrate, vintage-preserving). Build when first real source lands.
11. 🚧 implement fiscal, calendar-normalized, and PIT derived views. Current metric view stack exists under `db/queries/` (`v_ff_current`, wide period views, TTM/FY/CY/ROIC metrics, and screenable metric views); true PIT as-of view support remains deferred.
12. ⬜ implement `prices_daily`
13. ⬜ implement `company_events`
14. 🚧 implement analyst runtime retrieval tools and deterministic revenue-driver CLI (PIT-aware; see `docs/architecture/analyst_runtime.md`). MVP deterministic revenue-driver CLI exists in `scripts/ask_arrow.py`; it supports annual and quarterly revenue-growth driver questions. Transcript retrieval primitives and the transcript evidence CLI exist in `src/arrow/retrieval/transcripts.py` + `scripts/analyst_transcript_brief.py`; revenue-driver packets include matching transcript evidence. Broader reusable retrieval tools, richer recipes, and full PIT behavior remain in progress.
15. ⬜ implement `qa_log` as part of normal analyst flow (with consent flags)
16. ✅ implement SEC qualitative section + chunk layer (`artifact_sections`, `artifact_section_chunks`) for filing text in migration 014. Generic text-unit chunking for press releases and transcripts uses `artifact_text_units` / `artifact_text_chunks` from migration 015.
17. ⬜ add section-over-time comparison support
18. ⬜ add `signals` + `alerts`
19. 🚧 add SEC fast-path ingest for newly dropped filings (recent submissions + raw filing artifacts; 8-K exhibit/press-release text-unit support landed in migration 015; first-class update orchestration remains next)
20. ⬜ add Massive-backed options ingest later
21. ⬜ migrate to cloud when durability/reliability justify it
22. 🚧 metrics platform + analyst surfaces (see `docs/architecture/metrics_platform.md`, `docs/architecture/dashboard.md`). Shipped: formula spec tweaks, FMP employee-count ingest, segment-aware facts, core metric view stack, dashboard MVP (`scripts/dashboard.py`), screener MVP (`scripts/screen.py`). Remaining: complete presentation views such as `v_metric_changes` / `v_dashboard_panel` if still desired, full-history production backfill validation, and broader analyst-surface integration.
23. 🚧 implement steward (data-trust) layer — `coverage_membership`, `data_quality_findings`, deterministic check registry, dashboard findings/coverage panes, action callables with `actor` field. **Now the load-bearing priority ahead of further analyst feature expansion.** V1 is deterministic (six SQL checks + Python expectations module + dashboard surface); V2 adds LLM suggester + LLM-as-judge checks; V3 promotes per-check autonomy on proven check types. See `docs/architecture/steward.md` for the full runtime, build order, and V1→V3 LLM trajectory.

✅ also: `companies` schema (migration 007) — implicit prerequisite to step 9, was not in the original numbered list but has to land before any fact references a company.

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
