# Analyst Runtime

Status: active design; MVP deterministic revenue-driver slice built

Current shipped slice:

- CLI entrypoint: `scripts/ask_arrow.py`
- Runtime packet / synthesis code: `src/arrow/analysis/company_context.py`
- Covered question shapes: "What drove {TICKER} revenue growth in FY{YEAR}?"
  and "What drove {TICKER} revenue growth in FY{YEAR} Q{N}?"
- Revenue-driver packet includes transcript evidence when available.
- Transcript retrieval primitives: `src/arrow/retrieval/transcripts.py`
- Transcript evidence CLI: `scripts/analyst_transcript_brief.py`

Still pending:

- broader reusable retrieval tool surface beyond transcripts
- multiple recipes and topics
- durable `qa_log`
- full PIT as-of behavior across every source family

This document defines the runtime spine for every analyst-facing surface in
Arrow: CLI questions, chat, saved prompts, dashboard panels, and future
monitoring alerts. The first implementation should be tiny, but it should use
the same seams the full system will need.

## Purpose

Arrow's analyst runtime turns a user question or system event into a grounded
answer with citations and a trace of how the answer was produced.

It is not a separate data source. It is the consumer of the existing substrate:

- `financial_facts` and derived metric views
- `artifacts`, `artifact_sections`, `artifact_section_chunks`
- `artifact_text_units`, `artifact_text_chunks`
- future transcripts, prices, events, estimates, macro series, and news

## Runtime Spine

Every analyst answer follows the same pipeline:

```text
Question | saved prompt | event trigger
  -> Intent
  -> Readiness check
  -> Plan
  -> Retrieval tools
  -> Ground
  -> Evidence packet
  -> Synthesizer
  -> Verifier
  -> Trace writer
  -> Output channel
```

The front of the pipeline varies by surface:

- CLI receives one question string
- chat receives a message plus thread context
- saved prompts fill a template
- monitoring triggers start from a filing, transcript, price move, or other
  event

The middle stays shared. Monitoring is not a different analyst; it is the same
runtime invoked automatically.

Monitoring adds one terminal decision before notification:

```text
... -> Synthesizer -> Verifier -> Score -> Trace writer -> Notify
```

`Score` answers whether a verified answer is novel or important enough to
interrupt the user. Chat and CLI skip `Score` because the user explicitly asked
for the answer.

## Stage Contracts

| Stage | Input | Output | MVP behavior | Later behavior |
|---|---|---|---|---|
| Intent | question string | normalized `Intent` | deterministic parser for explicit ticker + FY | thread context, relative periods, optional LLM parse with validation |
| Readiness | `Intent` | pass/fail + missing prerequisites | validate company, FY metrics, and period-aligned artifacts | source-specific coverage policies and PIT readiness |
| Plan | `Intent`, readiness | named recipe + planned tool calls | one hardcoded recipe for revenue drivers | recipe registry and optional LLM tool-use planner |
| Retrieve | planned tool call | source-native rows/chunks | SQL + FTS only | same tools plus transcripts, events, prices, estimates, macro, news |
| Ground | plan + retrieved results | `EvidencePacket` | normalize facts/chunks and produce gaps | mode-specific packet builders and richer provenance |
| Synthesize | `EvidencePacket` | `Answer` | deterministic template | LLM synthesis constrained to packet evidence |
| Verify | `Answer`, packet | verification status | structural, numeric, citation-shape checks | claim/citation verification and retry for LLM answers |
| Trace | full invocation | JSONL / `qa_log` row | JSONL action trace | Postgres `qa_log`, evals, tuning/export controls |
| Channel | verified answer | rendered output | CLI text | chat, dashboard, saved prompt result, notification |

## Core Objects

### `Intent`

Normalized analyst request.

Fields:

- `ticker`
- `company_id`
- `period` / `fiscal_period_key`
- `calendar_period_label` when relevant
- `topic`
- `mode`
- `asof`
- `source_question`

`asof` is universal, not macro-only. It should exist on `Intent` from day one
even when the MVP always sets it to `None`; future questions need to ask what
was known at a point in time across facts, filings, transcripts, estimates, and
macro data.

MVP:

- deterministic parsing
- explicit ticker required
- explicit fiscal year required
- one mode: `single_company_period`
- one topic: `revenue_growth`

Later:

- relative period resolution
- thread-context resolution
- optional LLM parsing, validated against deterministic company and period
  checks

### `ReadinessCheck`

Fail early when the requested identity or period cannot support the recipe.

Checks:

- company exists
- requested period exists in the relevant metric view
- at least one period-aligned artifact exists
- required source families are present, missing, or only partially present

Readiness distinguishes:

- hard failure: identity or period foundation is broken
- soft gap: expected evidence is missing or thin but the foundation is valid

### `EvidencePacket`

The packet is the contract between retrieval and synthesis.

Shared fields:

- `intent`
- `facts`
- `comparisons`
- `evidence_chunks`
- `gaps`
- `provenance`
- `trace_summary`

Mode-specific packet examples:

- `RevenueDriverPacket`: revenue facts, YoY comparison, segment/geography facts,
  MD&A chunks, earnings-release chunks, Q4 transcript turns
- `TrendPacket`: time series plus commentary timeline
- `EventReactionPacket`: event metadata, price window, related commentary
- `GuidancePacket`: guidance statements, actuals, walk-forward comparison

`provenance` is structured and separate from prose. It includes the fact IDs,
view names, artifact IDs, section IDs, chunk IDs, source document IDs, periods,
and retrieval actions used to build the answer.

Gaps are produced during Ground, not during raw retrieval. A gap means:

```text
Plan requested evidence X
Retrieve returned empty, weak, or period-mismatched results
Ground recorded the missing/weak evidence as a structured gap
```

Every gap should be traceable back to a planned tool call. This keeps gaps
auditable instead of becoming freeform caveats.

### `Answer`

Channel-neutral result.

Fields:

- `intent`
- `summary`
- `details`
- `citations`
- `gaps`
- `verification_status`
- `trace_id`

Output channels render `Answer` to CLI text, chat markdown, dashboard cards, or
alerts. Synthesis should not be channel-specific.

## Retrieval Tools

The Python retrieval primitives should match the architecture tool surface so
they can be called by both scripted recipes and future LLM tool-use loops:

- `get_financial_fact(ticker, concept, period, asof=None)`
- `get_metrics(ticker, period, asof=None)`
- `get_segment_facts(ticker, period, asof=None)`
- `list_documents(ticker, doc_type=None, period=None, asof=None)`
- `search_documents(query, filters, asof=None)`
- `read_document_section(document_id, section_key)`
- `read_chunk(chunk_id)`
- `sql_query(sql, params, asof=None)`
- `get_latest_transcripts(ticker, n=4, asof=None)`
- `search_transcript_turns(ticker, query, fiscal_period_key=None, asof=None)`
- `get_transcript_context(ticker, fiscal_period_key, asof=None)`
- `compare_transcript_mentions(ticker, terms, periods=8, asof=None)`

Rules:

- SQL first for structured facts.
- FTS first for document retrieval.
- Exact section/chunk reads for cited text.
- `asof` is accepted at the boundary from day one, even when MVP passes
  `None`.
- No embeddings until a concrete question fails under SQL + metadata + FTS.

## Topic Registry

Topics map analyst vocabulary to retrieval inputs.

Each topic defines:

- concept names
- metric fields
- segment dimensions, if relevant
- section keys
- artifact types
- FTS keywords
- default gaps

MVP topic:

```text
revenue_growth
  concepts: revenue
  metric fields: revenue_fy
  segments: revenue by product/geography/operating segment
  sections: item_7_mda, part1_item2_mda
  artifacts: 10-K, 10-Q, press_release
  keywords: revenue, growth, customer, commercial, government, demand
```

Keep the registry in Python dataclasses until analysts need to edit it without
code changes.

## Recipes And Tool Use

Known modes use deterministic recipes. `Plan` chooses the recipe and records
the intended tool calls before retrieval starts. A recipe is a named sequence
of tool calls that produces source-native retrieval results; Ground then turns
those results into a packet.

MVP annual recipe:

```text
single_period_driver(intent)
  -> get current FY metrics
  -> get prior FY metrics
  -> get segment revenue facts
  -> get MD&A chunks for the fiscal year
  -> get exact annual or FY-end Q4 earnings-release chunks
  -> search FY-end Q4 transcript turns for revenue-driver commentary
  -> build RevenueDriverPacket
```

MVP quarterly recipe:

```text
quarterly_revenue_driver(intent)
  -> get current quarter metrics
  -> get same-quarter-prior-year metrics
  -> get same-quarter segment revenue facts
  -> get 10-Q MD&A chunks for Q1-Q3 when available
  -> get same-quarter earnings-release chunks
  -> search same-quarter transcript turns for revenue-driver commentary
  -> build RevenueDriverPacket
```

Mode vocabulary:

- `single_company_period`: one ticker, one period, one topic
- `single_company_trend`: one ticker across multiple periods
- `cross_company_period`: multiple tickers in one period
- `event_reaction`: one ticker, one event, price/news/commentary window
- `guidance_history`: guidance statements compared over time
- `cross_quarter_walkback`: adjacent-period management claim or guidance
  comparison

Recipes are deterministic, cheap, and auditable. LLM tool-use planning is a
separate later path for off-pattern questions. Tool-use planning is flexible but
less predictable; it should call the same retrieval primitives, emit an
inspectable plan, and still produce an evidence packet before synthesis.

## Synthesis

The synthesizer turns a packet into an `Answer`.

Backends:

- `DeterministicSynth`: template-based answer from structured packet fields
- `LLMSynth`: model-generated prose constrained to packet evidence

MVP uses `DeterministicSynth` only. This keeps missing evidence visible instead
of letting fluent prose hide weak retrieval.

LLM synthesis comes only after the deterministic revenue-driver packet is useful
across the benchmark questions.

## Verification

Verification always runs. The checks differ by synthesizer.

Deterministic checks:

- required packet slots are present or represented in `gaps`
- every gap corresponds to a planned but empty/weak retrieval action
- cited facts/chunks exist in packet provenance
- cited ticker and period match the intent
- numeric strings in the answer match packet values after formatting
- no stale template text references missing evidence

LLM checks:

- every cited fact/chunk exists in packet provenance
- every substantive claim has a citation
- cited ticker and period match the intent
- numeric claims match packet values where parseable
- unsupported citations cause one retry; repeated failure returns an
  `unverified` answer instead of silently accepting it

MVP deterministic output does not need model claim verification, but it still
needs structural and numeric verification so template bugs and missing-evidence
bugs are visible.

## Action Tracing

Action tracing is part of the runtime, not an observability afterthought.

Every invocation writes a trace containing:

- `trace_id`
- source surface: CLI, chat, saved prompt, monitor
- original question or event
- resolved intent
- readiness results
- recipe name
- ordered retrieval actions
- SQL query labels, parameter hashes, and row counts
- FTS queries, filters, result counts, and selected chunk IDs
- timings per action
- evidence packet summary
- synthesizer backend
- model name/version and token counts when an LLM is used
- verifier status and retry count
- final answer/citation IDs
- gaps
- user correction, acceptance, or rejection when available
- `training_opt_out`
- `sensitivity`

The trace has two purposes:

1. Debug correctness: why did the answer say this?
2. Optimize performance: which tool calls are slow, low-yield, redundant, or
   frequently missing evidence?

MVP tracing can start as JSONL under `outputs/qa_runs/`. Once the trace shape is
stable and LLM answers are enabled, promote it to `qa_log` via migration 017 so
normal analyst flow is persisted in Postgres with consent and sensitivity
flags.

Do not export traces for training or tuning unless `training_opt_out = false`
and sensitivity policy allows it.

## Surfaces

### CLI

First surface. One question in, one answer out. No session state.

### Chat

Chat adds thread context. The runtime remains the same; the intent resolver may
reuse prior ticker, period, and topic when the user asks follow-up questions.

### Saved Prompts

Saved prompts are intent factories. They fill a named template and invoke the
same pipeline.

### Monitoring

Monitoring has three pieces:

1. watchers detect new source events
2. trigger logic decides which saved prompt or recipe to run
3. signal/alert delivery renders the verified answer

The monitoring agent must use the same evidence packet, verifier, and trace
writer as chat. Otherwise it will become a second untestable analyst.

Monitoring also requires `Score`, which is separate from verification:

- `Verify`: is this answer grounded and truthful?
- `Score`: is this verified answer new, important, or urgent enough to notify?

Scoring inputs can include packet diffs, prior answers for the same saved
prompt, filing/event type, metric magnitude, price reaction, watchlist priority,
and user feedback. Scoring is deferred until the chat/CLI runtime can produce
trustworthy grounded answers.

## MVP Slice

Build the first vertical strip:

- script: `scripts/ask_arrow.py`
- shared retrieval: `src/arrow/analysis/company_context.py`
- mode: `single_company_period`
- topic: `revenue_growth`
- parser: deterministic
- recipe: revenue facts + YoY + segment revenue + MD&A where period-aligned + earnings release + transcript evidence
- synthesizer: deterministic
- verifier: structural + numeric checks
- trace: JSONL
- channel: CLI
- benchmark: `docs/benchmarks/ask_arrow_questions.md`

Question shape:

```text
What drove {TICKER} revenue growth in FY{YEAR}?
What drove {TICKER} revenue growth in FY{YEAR} Q{N}?
```

The benchmark pass should run the deterministic output across roughly 20
questions before adding LLM synthesis.

## Non-Goals For MVP

- no web UI
- no multi-turn chat
- no relative period resolution
- no LLM parsing
- no LLM synthesis
- no embeddings
- no monitoring
- no generic SQL agent
- no speaker-role classifier or Q&A boundary logic for transcripts until a
  concrete analyst question requires it
