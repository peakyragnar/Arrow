# Repository Flow

A reference map of every folder in the repo, what it's for, and how data moves through the system end-to-end. Pairs with `docs/architecture/system.md` (the architecture doc) and `AGENTS.md` (the repo map / read order).

Use this when you need to answer "where does X live?" or "what depends on what?"

---

## Top-Level Tree

```
Arrow/
├── AGENTS.md           ← repo map, read order, working rules (entry point)
├── CLAUDE.md           ← stub; points to AGENTS.md
├── README.md           ← (optional) public-facing overview
├── .env                ← secrets: FMP_API_KEY, DB creds (gitignored)
├── .gitignore          ← data/, output/, __pycache__/, .env
│
├── docs/               ← knowledge. Not runtime. Not data.
│   ├── architecture/   ← how the system is designed (north star)
│   ├── strategy/       ← plan, postmortems (direction + history)
│   ├── reference/      ← formulas, R&D capitalization rules (stable specs)
│   ├── benchmarks/     ← golden_eval.xlsx (manual truth for validation)
│   └── archive-notes/  ← notes on what was retired and why
│
├── db/                 ← SQL artifacts (not Python)
│   ├── schema/         ← CREATE TABLE, CREATE INDEX, migrations
│   ├── queries/        ← reusable SQL views, reconciliation queries
│   └── seeds/          ← bootstrap data (companies, macro_series defs)
│
├── src/                ← live Python implementation (the library)
│   └── arrow/          ← the `arrow` package — importable everywhere
│       ├── ingest/     ← fetch layer
│       ├── normalize/  ← derive layer
│       ├── models/     ← canonical row shapes
│       ├── db/         ← connection + query helpers
│       ├── reconcile/  ← cross-source divergence jobs
│       ├── retrieval/  ← analyst agent's toolbox
│       ├── research/   ← research corpus pipeline
│       └── agents/     ← orchestration of ingestion + analyst agents
│
├── scripts/            ← thin CLI entrypoints. Wire src/arrow/* together.
│
├── data/               ← runtime data (gitignored)
│   ├── raw/            ← verbatim vendor payloads (immutable)
│   │   ├── fmp/
│   │   ├── sec/filings/
│   │   ├── macro/
│   │   ├── news/
│   │   └── options/
│   ├── staging/        ← intermediate ETL products (ephemeral)
│   ├── cache/          ← derived computed caches (ephemeral)
│   └── exports/        ← human-facing outputs: CSVs, reports, feeds
│
├── tests/              ← test suites
│   ├── unit/           ← pure tests on src/arrow/* modules
│   ├── integration/    ← hit Postgres + mock vendors
│   ├── regression/     ← fixed-input reproducible checks
│   └── fixtures/       ← test data (JSON payloads, seed rows)
│
└── archive/            ← old systems. Reference only. Not active implementation.
    ├── ai_extract/             ← pre-FMP AI extraction pipeline
    ├── deterministic-flow/     ← earlier deterministic pipeline
    ├── legacy-dashboard/       ← retired HTML dashboard
    └── legacy-root/            ← retired root-level scripts (fetch.py, calculate.py)
```

---

## Per-Folder Detail

### `docs/` — knowledge

No runtime, no data, no secrets. Pure Markdown (plus the benchmark XLSX). Changes here should never break code.

| Subfolder | Contents | When to edit |
|---|---|---|
| `docs/architecture/` | `system.md` (north star), `repository_flow.md` (this file) | When the design changes |
| `docs/strategy/` | `plan.md` (older snapshot), `postmortems/` | When direction changes, or after a major failure |
| `docs/reference/` | `formulas.md` (metric definitions), `rd_capitalization_reference.md` | When a canonical formula changes |
| `docs/benchmarks/` | `golden_eval.xlsx` (manual source-of-truth values) | When a benchmark ticker is verified |
| `docs/archive-notes/` | Notes on retired systems | When something gets archived |

**Rule:** if a doc in `archive/` conflicts with one in `docs/architecture/`, the architecture doc wins.

### `db/` — SQL artifacts

This is SQL, not Python. Python code in `src/arrow/db/` connects to and queries the database defined here.

| Subfolder | Contents | Example filename |
|---|---|---|
| `db/schema/` | `CREATE TABLE`, `CREATE INDEX`, migrations | `001_raw_responses.sql`, `002_artifacts.sql` |
| `db/queries/` | Reusable views + reconciliation queries | `view_financials_pit.sql`, `view_fmp_sec_divergence.sql` |
| `db/seeds/` | Bootstrap rows | `companies_seed.sql`, `macro_series_seed.sql` |

Migrations are numbered and append-only. Never edit a past migration; add a new one.

### `src/arrow/` — the library

See the next section for the full subfolder breakdown. Everything importable lives here.

### `scripts/` — thin CLI entrypoints

Runnable commands. Parse argv, instantiate from `arrow.*`, call methods, log. No business logic.

Present:
- `scripts/db_ping.py` — smoke-test the DB connection
- `scripts/apply_schema.py` — apply pending migrations under `db/schema/`
- `scripts/gen_schema_viz.py` — introspect the live DB and regenerate `arrow_db_schema.html` (the visual source of truth for the schema)

Examples (future):
- `scripts/backfill_fmp.py NVDA` — pull every FMP endpoint for a ticker, write raw + load DB
- `scripts/fetch_prices.py --tickers NVDA,MSFT --from 2022-01-01`
- `scripts/reconcile_fmp_sec.py` — run the divergence job
- `scripts/export_training_set.py --out out.jsonl` — dump qa_log with consent filters

**Rule:** a script is the operational seam. If it has logic another piece of code might want to call, that logic belongs in `src/arrow/`, not the script.

### `data/` — runtime data (gitignored)

Nothing here is source. Everything is either fetched from a vendor or derived from something that was.

| Subfolder | Contents | Mutability | Can delete? |
|---|---|---|---|
| `data/raw/` | Verbatim vendor payloads | Write-once, never mutate | No (re-fetch costs money) |
| `data/staging/` | Intermediate ETL products | Rewritten freely | Yes (regen from raw) |
| `data/cache/` | Derived computed caches | Rewritten freely | Yes (regen from DB) |
| `data/exports/` | Human-facing outputs (CSVs, reports) | Rewritten freely | Yes (regen from DB) |

Details of `data/raw/` layout live in `docs/architecture/system.md` § Raw Cache Layout. The rule: endpoint-mirrored, deterministic. `data/raw/fmp/{endpoint-path}/{TICKER}/{key}.json`.

### `tests/` — test suites

| Subfolder | Scope | Speed |
|---|---|---|
| `tests/unit/` | Pure tests on `src/arrow/*` modules (no DB, no network) | Fast |
| `tests/integration/` | Hit local Postgres; mock vendor HTTP | Medium |
| `tests/regression/` | Fixed-input reproducible checks (golden tests, FMP vs SEC divergence snapshots) | Medium |
| `tests/fixtures/` | Saved JSON payloads, seed rows, canonical test data | — |

Tests import from `arrow.*`. Never from `scripts.*`.

### `archive/` — retired systems

Read-only reference. Never extend. Never import from live code.

| Subfolder | What it is | Why kept |
|---|---|---|
| `archive/ai_extract/` | Pre-FMP AI extraction pipeline | Last full iteration; benchmark context; legacy extraction logic |
| `archive/deterministic-flow/` | Earlier deterministic pipeline | Historical reference; contains `nvda.json` gold data |
| `archive/legacy-dashboard/` | Retired HTML dashboard | Superseded by later analyst-facing tools |
| `archive/legacy-root/` | Retired root-level `fetch.py`, `calculate.py` | Old entrypoints |

---

## `src/arrow/` Detail

```
src/arrow/
├── ingest/      ← fetch layer: vendor REST → raw_responses + filesystem cache
│   ├── common/  ← shared ingest primitives (HTTP client, retry, rate-limit,
│   │             raw_responses writer, ingest_runs logger, cache_path helpers)
│   ├── fmp/     ← FMPClient, per-endpoint wrappers, paths.py
│   ├── sec/     ← direct EDGAR fetcher (fresh-filing fast-path only)
│   ├── macro/   ← FRED / BLS / BEA / Treasury clients (later)
│   ├── news/    ← news vendor clients (later)
│   └── options/ ← Massive client (later, when paid)
│
├── normalize/   ← derive layer: raw vendor payload → canonical rows
│   ├── financials/  ← FMP/SEC statements → financial_facts rows
│   │                  (fiscal + calendar + published_at/superseded_at)
│   ├── events/      ← filings, earnings dates, splits → company_events rows
│   └── periods/     ← fiscal ↔ calendar mapping logic (the "two clocks" rule)
│                      lives here because it's shared by every normalizer
│
├── models/      ← canonical data shapes (dataclasses or ORM models):
│                   FinancialFact, Artifact, ArtifactChunk, MacroObservation,
│                   CompanyEvent, etc. What a row looks like in Python
│                   before/after it hits the DB.
│
├── db/          ← connection pool, session mgmt, thin query helpers, migration
│                   glue. Everything that talks to Postgres goes through here.
│
├── reconcile/   ← cross-source divergence jobs. First occupant: FMP vs SEC
│                   financial-facts reconciliation (Build Order 9.5). Later:
│                   macro vintage reconciliation, vendor-vs-vendor checks.
│
├── retrieval/   ← analyst agent's toolbox: search_documents(), list_documents(),
│                   read_document(), sql_query(), get_financial_fact(asof=...),
│                   get_macro_series(asof=...). The API the analyst agent calls.
│
├── research/    ← research corpus pipeline: industry primers, product explainers,
│                   macro primers. Treats them as artifacts with freshness
│                   metadata (authored_by, last_reviewed_at, asserted_valid_through).
│
└── agents/      ← orchestration of the two agents:
                    - ingestion agent (schedules pulls, triggers refetch on filing
                      drops, runs reconcile jobs, writes signals)
                    - analyst agent (tool-using loop over retrieval/*, writes qa_log)
```

---

## End-to-End Data Flow

```
                           (Vendor APIs: FMP, SEC EDGAR, FRED, Massive, News)
                                                │
                                                ▼
                          ┌──────────────────────────────────────────────┐
                          │ src/arrow/ingest/                            │
                          │   common/  fmp/  sec/  macro/  news/  options│
                          └──────────────────────────────────────────────┘
                                                │
                     writes (both)              │
           ┌────────────────────────────────────┴───────────────────────────┐
           ▼                                                                ▼
  data/raw/{vendor}/...                                          raw_responses table
  (filesystem, belt-&-suspenders)                                (Postgres, canonical)
           │                                                                │
           └──────────────┬─────────────────────────────────────────────────┘
                          │ reads raw payloads
                          ▼
                 ┌─────────────────────────────────────────────┐
                 │ src/arrow/normalize/                        │
                 │   financials/  events/  periods/            │
                 └─────────────────────────────────────────────┘
                          │ writes via src/arrow/db/
                          ▼
    ┌─────────────────────────────────────────────────────────────────────┐
    │ Postgres canonical tables                                           │
    │   companies, artifacts, financial_facts,                            │
    │   prices_daily, macro_series, macro_observations, company_events,   │
    │   options_contracts, options_eod_snapshots                          │
    │   (chunks table reintroduced when document text is ingested)        │
    └─────────────────────────────────────────────────────────────────────┘
                          │
        ┌─────────────────┼─────────────────┬─────────────────────┐
        │                 │                 │                     │
        ▼                 ▼                 ▼                     ▼
  ┌───────────┐    ┌───────────────┐  ┌──────────────┐    ┌────────────┐
  │ reconcile │    │ retrieval     │  │ research     │    │ direct SQL │
  │           │    │  (tools)      │  │  corpus      │    │  queries   │
  │ writes    │    │               │  │              │    │            │
  │ divergence│    │ read-only API │  │ ingests      │    │            │
  │ views     │    │ for agents    │  │ primers      │    │            │
  └───────────┘    └───────┬───────┘  └──────────────┘    └────────────┘
                           │
                           ▼
              ┌──────────────────────────────┐
              │ src/arrow/agents/            │
              │   ingestion agent  (cron)    │
              │   analyst agent    (Q&A/alert)│
              └──────────────────────────────┘
                           │
         ┌─────────────────┼────────────────┐
         ▼                 ▼                ▼
  signals + alerts    qa_log writes    data/exports/
   (Postgres)          (Postgres)      (CSVs, reports)
         │
         ▼
    (email / slack / push notifications)
```

### Supporting layers (not in the main flow, but everywhere)

- **`db/` (top-level SQL)** — defines the table shapes the flow writes into. `db/schema/` is the contract; `db/queries/` are reusable views; `db/seeds/` bootstrap rows.
- **`src/arrow/db/` (Python)** — the connection pool + helpers every layer uses to actually talk to Postgres.
- **`src/arrow/models/`** — the Python representation of table rows; used by every layer so they share one vocabulary.
- **`scripts/`** — human/cron entrypoints. Every script imports from `src/arrow/*` and calls into the flow at one of the boxes above.
- **`tests/`** — verify each box in isolation (unit) and combined (integration).
- **`docs/`** — describes all of the above; changes here don't affect runtime.

---

## Dependency Direction (strict)

Nothing lower imports from anything higher. This is what keeps the system testable and refactorable.

```
scripts/     →  src/arrow/*                (scripts use the library)
tests/       →  src/arrow/*                (tests use the library)

agents/      →  retrieval/, ingest/, reconcile/, normalize/, research/
retrieval/   →  db/, models/
reconcile/   →  db/, models/, normalize/
research/    →  db/, models/, ingest/
normalize/   →  db/, models/, ingest/ (only to read raw_responses)
ingest/      →  db/, models/
db/ (python) →  models/
models/      →  (no internal deps — leaf)

db/ (SQL)    ←  defines contracts that Python code honors
docs/        ←  describes; doesn't import
data/        ←  written/read by runtime; not importable
archive/     ←  isolated; never imported
```

### What this buys

- **Models are a leaf.** Changing a row shape is visible everywhere; any breakage surfaces at compile/test time.
- **Ingest doesn't know about analyst tools.** Vendor changes don't ripple into retrieval.
- **Retrieval doesn't know about ingest.** Fetching a new vendor doesn't change how the analyst queries.
- **Agents are the only thing that orchestrates across layers.** The complex wiring lives in one place, not sprinkled.

---

## "Where does this go?" — decision guide

| If you're adding… | It belongs in… | Example |
|---|---|---|
| An HTTP client for a new vendor | `src/arrow/ingest/{vendor}/` | `ingest/massive/client.py` |
| A shared ingest concern (retries, rate-limits) | `src/arrow/ingest/common/` | `ingest/common/http.py` |
| Logic to map a vendor field → canonical row | `src/arrow/normalize/{domain}/` | `normalize/financials/fmp_mapper.py` |
| Fiscal ↔ calendar period math | `src/arrow/normalize/periods/` | `normalize/periods/map.py` |
| A new canonical row shape | `src/arrow/models/` | `models/artifact.py` |
| A new Postgres table | `db/schema/NNN_*.sql` + row in `src/arrow/models/` | `db/schema/005_options_contracts.sql` |
| A SQL view used by many callers | `db/queries/` | `db/queries/view_financials_pit.sql` |
| A divergence / reconciliation job | `src/arrow/reconcile/` | `reconcile/fmp_vs_sec.py` |
| A tool the analyst agent calls | `src/arrow/retrieval/` | `retrieval/tools/search_documents.py` |
| Industry / product / macro primer pipeline | `src/arrow/research/` | `research/primer_loader.py` |
| Orchestration: "when filing drops, do X, Y, Z" | `src/arrow/agents/` | `agents/ingestion/on_filing_drop.py` |
| A command a human or cron runs | `scripts/` | `scripts/backfill_fmp.py` |
| A test for one module, no DB | `tests/unit/` | `tests/unit/test_paths.py` |
| A test that hits Postgres | `tests/integration/` | `tests/integration/test_fmp_load.py` |
| A fixed-input reproducibility check | `tests/regression/` | `tests/regression/test_nvda_is_fy26q4.py` |
| A saved payload or seed for tests | `tests/fixtures/` | `tests/fixtures/fmp/income-statement/NVDA.json` |
| A benchmark value to match | `docs/benchmarks/` | update `golden_eval.xlsx` |
| A formula / metric definition | `docs/reference/` | `docs/reference/formulas.md` |
| An architecture decision | `docs/architecture/system.md` | add section there |
| A retrospective after a failure | `docs/strategy/postmortems/` | `docs/strategy/postmortems/YYYY-MM-DD-title.md` |

**Rule of thumb:** if you're not sure, trace the dependency direction. If the new thing needs to import from layer X, it must live strictly above layer X.

---

## Cross-References

- **Architecture north star:** `docs/architecture/system.md`
- **Repo entry point:** `AGENTS.md`
- **Raw cache filesystem layout:** `docs/architecture/system.md` § Raw Cache Layout
- **Metric definitions:** `docs/reference/formulas.md`
- **Benchmark truth:** `docs/benchmarks/golden_eval.xlsx`
- **Retired systems:** `archive/` (do not extend)
