# AGENTS.md

Arrow is being rebuilt around an FMP-first, PostgreSQL-backed architecture.

This file is the repo map, not the encyclopedia.
Start here. Read only the next doc you need.

## Current Source Of Truth

- Current architecture: `docs/architecture/system.md`
- Fast orientation: `docs/architecture/normal_vs_audit.md`
- Repository flow / folder map: `docs/architecture/repository_flow.md`
- Metrics platform (view stack; shared by dashboard, screener, analyst agent): `docs/architecture/metrics_platform.md`
- Analyst runtime (chat, retrieval recipes, evidence packets, tracing): `docs/architecture/analyst_runtime.md`
- Steward runtime (data trust: findings, coverage, expectations, three-agent split, V1→V3 LLM trajectory): `docs/architecture/steward.md`
- Steward operator runbook (how to use it day-to-day + how to operate so V2 trains well): `docs/reference/steward_operator_runbook.md`
- Driver analysis ingest plan (growth/margin/cash driver substrate): `docs/architecture/driver_analysis_ingest_plan.md`
- Prices ingest plan (daily OHLCV + market cap + valuation views): `docs/architecture/prices_ingest_plan.md`
- Estimates ingest plan (analyst consensus, price targets, surprises, ratings events): `docs/architecture/estimates_ingest_plan.md`
- Dashboard UI surface: `docs/architecture/dashboard.md`
- SEC qualitative layer (filings → sections → chunks, amendments, FTS): `docs/architecture/sec_qualitative_layer.md`
- How to review SEC qualitative extraction quality: `docs/reference/sec_qualitative_review.md`
- Period spec (fiscal ↔ calendar rules): `docs/reference/periods.md`
- Canonical bucket schema (IS/BS/CF normalization contract): `docs/reference/concepts.md`
- FMP ↔ canonical mapping: `docs/reference/fmp_mapping.md`
- Audit reference (optional side rail, not default ingest): `docs/reference/verification.md`
- Metric definitions (formulas + component guards): `docs/reference/formulas.md`
- R&D capitalization reference: `docs/reference/rd_capitalization_reference.md`
- Artifact metadata key conventions: `docs/reference/artifact_metadata.md`
- Architecture decisions (tool + pattern choices): `docs/decisions/`
- Setup runbook: `docs/setup.md`
- Schema (live visual view): `arrow_db_schema.html` — regenerated from the live database by `scripts/gen_schema_viz.py`. Authoritative DDL: `db/schema/*.sql`.
- Older strategy snapshot: `docs/strategy/plan.md`
- Benchmark workbook: `docs/benchmarks/golden_eval.xlsx`

## Read Order

1. `AGENTS.md`
2. `docs/architecture/system.md`
3. `docs/architecture/normal_vs_audit.md`
4. task-specific doc in `docs/` (`docs/architecture/analyst_runtime.md` for analyst/chat/runtime work; `docs/architecture/steward.md` for data-quality / coverage / findings work)
5. live code in `src/` / `db/` / `scripts/`
6. `archive/` only if you need legacy reference

## Live Layout

```text
docs/        current architecture, strategy, references, benchmarks
db/          schema, queries, seeds
src/arrow/   live implementation
scripts/     thin entrypoints / utilities
data/raw/    raw source caches by vendor/source
tests/       test suites
archive/     old systems; reference only
```

## Archive Fence

These are archived, not active implementation guidance:

- `archive/ai_extract/`
- `archive/deterministic-flow/`
- `archive/legacy-dashboard/`
- `archive/legacy-root/fetch.py`
- `archive/legacy-root/calculate.py`

Important:
- `archive/ai_extract/` is the last full design iteration before the FMP pivot
- it remains valuable for benchmark context and legacy extraction logic
- it is not the current system design

## Working Rules

- System of record: PostgreSQL
- `financial_facts` baseline truth: FMP
- SEC active role: raw `8-K` / `10-Q` / `10-K` documents + low-latency fresh filing path
- Audit/reconciliation: runs automatically in normal flow (after FMP backfill). Auto-promotes XBRL values for the safe corruption bucket (direct-tagged, recent FY, unambiguous concept, moderate gap); the rest surface in the steward queue via `xbrl_audit_unresolved` for analyst adjudication. Never silently rewrites without provenance — every supersession carries `supersedes_fact_id` + accession reference.
- Retrieval: search-first, SQL + FTS, not naive RAG
- Preserve fiscal truth and calendar normalization
- Preserve point-in-time semantics where revisions/restatements exist
- Raw responses cached
- Artifacts immutable
- Facts derived and regeneratable
- For analyst/driver work, preserve the split between source evidence, structured observations, derived signals, and AI synthesis. Start with structured facts and deterministic comparisons before asking an LLM to explain them.
- **Schema changes ship with their docs.** A migration that adds, removes, or supersedes a table updates the v1 Tables status table in `docs/architecture/system.md` and any reference-doc mentions in the same commit. ADRs about withdrawn or superseded tables get their status flipped, never their bodies edited. Build Order step markers get updated too. After applying a migration, re-run `uv run scripts/gen_schema_viz.py` to regenerate the live schema view at `arrow_db_schema.html`.
- **New verticals ship with their expectations and steward checks.** Adding a new data vertical (transcripts, news, prices, options, macro, video, research primers, etc.) ships its `expectations.py` entry and at least basic steward checks (presence, freshness, orphan detection) in the same commit. Otherwise the data-trust layer falls behind the data layer. Parallel rule to "schema changes ship with their docs." See `docs/architecture/steward.md` § Working Rules.
- **Capture every triage session structurally.** Chat is the operator's V1 work surface for steward triage and ingest follow-up — not the dashboard (the dashboard is for monitoring and audit). At the end of any meaningful triage activity (working through findings, diagnosing a data-quality issue, applying a fix script), the AI MUST record a `triage_session` row via `scripts/record_triage_session.py` capturing: intent, finding_ids touched, operator_quotes (verbatim), investigations run, actions taken (run_ids, commits), outcomes, and a one-sentence `captured_pattern` if extractable. This is the V1 substrate for the future autonomous data-quality operator agent — these rows ARE the training corpus. Use honest `created_by` labels: `human:michael` only when the operator drove the analysis; `claude:assistant_via_michael` when the AI investigated and the operator approved. Never assert `human:michael` for AI-paraphrased reasoning. See `src/arrow/steward/sessions.py` and `docs/architecture/steward.md` § LLM Trajectory.
- **Check for pending triage at session start.** `ingest_company.py` runs the steward post-ingest and drops a JSON record under `data/pending_triage/` whenever new findings need triage. Any AI session in this repo (Claude Code, Codex, ...) MUST run `uv run scripts/check_pending_triage.py` once at session start. If pending records exist, surface them to the operator concisely ("you have 3 new findings on LITE from yesterday's ingest — want to walk through them?") and proceed with triage when the operator approves. After triage finishes, mark the record resolved with `--resolve <path>` so it doesn't resurface. This is the harness-agnostic post-ingest hook; the operator never has to remember to "run the steward."

## Data Rules

- Keep raw SEC filings under `data/raw/sec/filings/{CIK}/{ACCESSION}/`
- Keep vendor/source caches under `data/raw/{vendor}/`
- Raw filesystem cache layout mirrors the vendor's endpoint path (endpoint-mirrored, not category-mirrored)
- FMP pattern: `data/raw/fmp/{endpoint-path}/{TICKER}/{key}.json` where `{key}` deterministically encodes request params (`annual`, `quarter`, year slice, `YYYY-Qn`, etc.)
- See `docs/architecture/system.md` § Raw Cache Layout for the full rule and examples
- Do not treat generated JSON/CSV artifacts as policy truth
- Use benchmark/reference docs in `docs/reference/` and `docs/benchmarks/`

## Folder Intent

- `docs/architecture/system.md` is the north star
- `docs/strategy/plan.md` is older and superseded for architecture
- `src/arrow/ingest/fmp/` is where live FMP ingest should land
- `src/arrow/ingest/sec/` is where live SEC fast-path ingest should land
- `archive/` is for reading, not for new implementation

## If Unsure

- prefer `docs/architecture/system.md`
- prefer live paths over archived paths
- if a doc in `archive/` conflicts with a doc in `docs/architecture/`, the architecture doc wins
- for "why did we choose X?" questions about tools/patterns, look in `docs/decisions/` — ADRs capture the trade-offs that principles don't settle
