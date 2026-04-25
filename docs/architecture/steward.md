# Steward Runtime

Status: V1 in progress (migration 017 + deterministic check registry); V2 and V3 planned.

This document defines the **data-trust runtime** for Arrow: the third agent —
distinct from the ingestion agent and the analyst agent — whose only job is
to verify that Arrow's data is right, complete, connected, and fresh.

Mirrors the structure of [`analyst_runtime.md`](analyst_runtime.md). Where
the analyst answers investment questions about companies, the steward
answers operational questions about Arrow itself: *Can this data be trusted?
Is it complete? What broke? What's missing?*

## Purpose

A frontier-model analyst over questionable data is worse than a weak
analyst over verified data — fluent prose hides rot. Data trust is the
prerequisite for any analyst capability being trustworthy. The steward layer
exists so that every claim Arrow makes carries provenance, freshness, and
known coverage.

The steward is also the system's **operator console.** Arrow's owner
operates the system primarily through the dashboard, not through `psql`.
Every finding the steward writes is something the operator (or a future
agent) can act on without writing SQL.

## Three-Agent Split

`docs/architecture/system.md` § Agent Split establishes ingestion vs analyst.
The steward is the third role:

| Agent | Owns | Properties |
|---|---|---|
| Ingestion | Fetch, cache, normalize, index, extract | Deterministic where possible; replayable; auditable; append-only |
| Analyst | Retrieval, comparison, explanation, Q&A | Search-first; citation-first; tool-using; debuggable |
| **Steward** | **Vigilance over data state; surfacing; lifecycle of findings** | **Mostly deterministic; per-check policy; never mutates source data** |

Do not combine roles. The steward is built separately and integrates with
the others through shared substrate (`coverage_membership`,
`data_quality_findings`, `data_quality_flags`) and — at V3+ — shared
retrieval primitives.

## Runtime Spine

Every steward invocation follows the same pipeline:

```text
Trigger (manual sweep | post-ingest hook | cron | ticker scope)
  -> Resolve scope
  -> Filter check registry by scope
  -> For each check: produce FindingDrafts
  -> Compute fingerprint per draft
  -> Reconcile against existing findings:
       - new fingerprint  -> open_finding()
       - existing open    -> bump last_seen_at
       - existing closed-suppressed-and-active -> skip (respect suppression)
       - previously open, not surfaced this run -> resolve_finding(actor=system)
  -> Emit RunSummary
```

Findings flow into the dashboard `/findings` pane. The operator (V1) or
agent (V2+) decides next action through typed action callables — the same
callables that the dashboard route handlers wrap.

## Stage Contracts

| Stage | Input | Output | V1 behavior | V2+ behavior |
|---|---|---|---|---|
| Trigger | scope filter | `Scope` object | manual CLI / explicit ticker | + post-ingest hook + nightly cron |
| Plan | scope | filtered check list | all registered checks matching scope | + per-check automation policy lookup |
| Run | check + scope | `FindingDraft` iterable | deterministic SQL only | + LLM-as-judge checks where prose required |
| Reconcile | drafts + existing findings | DB state changes | open / bump / auto-resolve / respect-suppression | same |
| Suggest | open finding | `agent_suggestion` jsonb | n/a (V1) | RAG over closed findings + LLM proposal |
| Execute | finding + decision | action callable invocation | human clicks lifecycle button | per-check policy: human / suggest / auto |
| Audit | every state change | `history` jsonb append | always | always |

## Core Objects

### `Finding`

A surfaced data-quality concern. Append-only by design (closed findings stay
as historical record); only one *open* finding per fingerprint.

Fields:

- `id`, `fingerprint`, `finding_type`, `severity`
- scope: `company_id`, `ticker`, `vertical`, `fiscal_period_key`
- detection: `source_check`, `evidence` (jsonb), `summary`, `suggested_action`
  (jsonb: `kind`, `params`, `command`, `prose`)
- lifecycle: `status` (`open`|`closed`), `closed_reason`
  (`resolved`|`suppressed`|`dismissed`), `closed_at`, `closed_by`,
  `closed_note`, `suppressed_until`
- audit: `history` jsonb (append-only state changes)
- provenance: `created_at`, `created_by`, `last_seen_at`

Lifecycle is two-state (open → closed). The reason inside `closed`
distinguishes resolved (problem fixed), suppressed (known and not actionable
now), and dismissed (false positive).

### `Check`

```python
class Check(ABC):
    name: str               # 'zero_row_runs'
    severity: str           # default severity for findings it produces
    vertical: str | None    # which vertical it scopes to (None = cross-cutting)

    @abstractmethod
    def run(self, conn, *, scope: Scope) -> Iterable[FindingDraft]:
        """Yield FindingDrafts. Checks never write to DB directly —
        the runner handles fingerprinting, dedup, and persistence."""
```

Checks are pure functions over DB state (V1) plus optional LLM judgment
(V2+). They never write findings themselves; they yield drafts and the
runner persists them. This keeps checks individually testable.

### `ExpectationSet`

Per-tier rules describing what each ticker should have. Lives as a Python
module in V1 (`src/arrow/steward/expectations.py`); promotes to a table when
rules grow past one file or operator-editable exceptions are needed.

```python
@dataclass(frozen=True)
class Expectation:
    vertical: str           # 'financials' | 'segments' | 'employees' | 'sec_qual'
    rule: str               # 'present' | 'min_periods' | 'recency'
    params: dict
```

The `expected_coverage` check (V1.5) iterates `coverage_membership`,
resolves expectations for each ticker's tier, queries actual state, and
yields findings for unmet expectations.

### `Scope`

```python
@dataclass(frozen=True)
class Scope:
    tickers: list[str] | None      # None = universe
    verticals: list[str] | None    # None = all
    check_names: list[str] | None  # None = all
```

### Action callables

Every operator action lives as a typed function in
`src/arrow/steward/actions.py` with an `actor: str` parameter. UI route
handlers are 3-line wrappers. Same call signature, different actor:

- `actor="human:michael"` (V1)
- `actor="human:michael:agent_confirmed"` / `:agent_overridden"` (V2)
- `actor="agent:steward_v1"` (V3, on auto-promoted checks)
- `actor="system:check_runner"` (auto-resolve when state cleared)

Functions:

- `open_finding(...)` — idempotent insert keyed on fingerprint
- `close_finding(id, *, closed_reason, actor, note, suppressed_until)`
- `resolve_finding(...)`, `suppress_finding(...)`, `dismiss_finding(...)` —
  convenience wrappers
- `add_to_coverage(ticker, tier, actor, ...)`, `remove_from_coverage(...)`

Every action appends to `history` jsonb on the affected row:
`{at, actor, action, before, after, note}`.

## LLM Trajectory: V1 → V2 → V3

The agent enters the system progressively. Each version has a distinct LLM
role and a distinct human role.

| Version | LLM role | Human role | What unlocks the next |
|---|---|---|---|
| V1 | None — pure deterministic SQL checks, templated prose | Triage every finding | ~50 closed findings (human decisions = training corpus) |
| V2 | Suggester only — proposes actions, never executes; LLM-as-judge checks for prose-judgment failure modes | Click confirm/override on every action | ~30 consecutive correct suggestions per check type |
| V3 | Autonomous on proven check types; suggester on others | Review activity feed; handle exceptions | ~6 months of operation; specialized fine-tune ready |

### V1 — deterministic foundation

Zero LLM calls. Every check is SQL. Every finding's `suggested_action.prose`
is templated by the check author. The substrate (action callables, audit
trail, structured `suggested_action`, dedup-by-fingerprint) is built so
nothing in V1 changes when V2 plugs in.

V1's job is twofold: (1) build the agent-ready substrate; (2) generate the
training corpus through human triage decisions. Operating V1 *is* the
training generation. The action callables already accept `actor`, the
history captures decisions with reasons — V2's suggester reads from this.

### V2 — LLM as advisor (suggest-only)

The agent enters in three roles, ordered by leverage:

1. **Triage suggester.** For each new open finding, the agent embeds it,
   pulls similar past closed findings (RAG over `data_quality_findings`
   where `status='closed'`), sends evidence + similar precedents to the LLM,
   and writes an `agent_suggestion` jsonb field to the finding.
   Dashboard shows the suggestion as a chip; human clicks confirm or
   override. Override reasons are captured — these are the next-tier
   training signal.

2. **LLM-as-judge checks.** New `LLMCheck` class, same registry. First two
   checks worth building:
   - `segment_taxonomy_drift`: deterministic prefilter on segment-share
     deltas; LLM judges whether a label shift is real reorg or vendor
     relabeling
   - `extraction_quality_regression`: LLM judges whether recently-extracted
     MD&A reads like a coherent section or boilerplate/table-of-contents

3. **Investigator tool.** On-demand "Investigate" button per finding.
   Agent uses retrieval primitives (the same ones the analyst agent uses) to
   pull evidence and produce a one-paragraph investigation summary.

The agent never executes actions in V2. Even at confidence 0.99, the human
clicks confirm. V2's failure mode is rubber-stamping; mitigation is a
weekly random-sample check of agent-confirmed closures.

### V3 — LLM as executor on proven check types

Per-check automation level becomes a real lever (replaces V2's Python
constant default). Progression per check:

```text
human_only       → suggest_only       → auto_with_review       → autonomous
(V1 default)       (V2 default)         (V3 entry per check)     (V3 mature)
```

Promotion requires demonstrated correctness (sample-based) plus an explicit
operator action. Demotion is one click and instantaneous. Drift detection
runs weekly: re-judge a sample of recent agent decisions; auto-demote if
alignment drops below a threshold.

V3 also ships:

- **Activity feed** replaces inbox as primary dashboard view; exceptions
  pane is the new (smaller) inbox
- **Revert window**: every agent-executed action is revertible for 24h
- **`automation_policy` table** (small migration) replacing the V2 Python
  constant

## Autonomy Curve Principle

Autonomy is **per check type, not per system.** Some checks promote to
autonomous quickly (mechanical: zero-row-runs, broken-provenance). Some may
never promote past suggest_only (judgment-heavy: segment-taxonomy-drift,
extraction-quality-regression). That's the right outcome — the system is
honest about what's automatable.

| Check | V1 | V2 | V3 (likely) |
|---|---|---|---|
| `zero_row_runs` | human_only | suggest_only | autonomous |
| `unresolved_flags_aging` | human_only | suggest_only | autonomous |
| `sec_artifact_orphans` | human_only | suggest_only | auto_with_review |
| `unparsed_body_fallback` | human_only | suggest_only | suggest_only |
| `broken_provenance` | human_only | suggest_only | autonomous |
| `section_confidence_drift` | human_only | suggest_only | suggest_only |
| `expected_coverage` | human_only | suggest_only | auto_with_review |
| `segment_taxonomy_drift` (LLM) | n/a | suggest_only | suggest_only |
| `extraction_quality_regression` (LLM) | n/a | suggest_only | suggest_only |

## V1 MVP Slice

Concretely, V1 delivers:

- `db/schema/017_steward_layer.sql` — `coverage_membership` table,
  `data_quality_findings` table, partial unique index for open-fingerprint
  dedup
- `db/queries/15_v_open_quality_signals.sql` — UNION view over open findings
  + open `data_quality_flags`, normalized
- `src/arrow/steward/`:
  - `actions.py` — typed callables with `actor` field
  - `fingerprint.py` — deterministic SHA256 over (check_name | scope_keys |
    rule_params)
  - `registry.py` — `Check` ABC, `register()` decorator, `REGISTRY` list
  - `runner.py` — `run_steward(conn, *, scope, actor)` orchestrator
  - `expectations.py` — Python module: universe defaults per tier +
    per-ticker overrides
  - `coverage.py` — pure SQL queries for the coverage matrix
  - `checks/` — six deterministic checks (see below) + V1.5 `expected_coverage`
- `scripts/run_steward.py` — CLI entrypoint
- Dashboard extensions in `scripts/dashboard.py`:
  - `GET /findings`, `GET /findings/{id}`
  - `POST /findings/{id}/{resolve,suppress,dismiss}`
  - `GET /coverage`, `GET /coverage/{ticker}`
  - `POST /coverage/add`, `POST /coverage/{ticker}/remove`
- New templates: `findings_list`, `finding_detail`, `coverage_matrix`,
  `coverage_ticker`
- Tests: integration tests for actions, runner, each check, dashboard routes

V1 deterministic checks (six):

1. `zero_row_runs` — `ingest_runs` succeeded but wrote 0 rows
2. `unresolved_flags_aging` — `data_quality_flags` open > 14 days
3. `sec_artifact_orphans` — SEC filing artifact with no sections/text units
4. `unparsed_body_fallback` — `artifact_sections.section_key='unparsed_body'`
5. `broken_provenance` — `financial_facts` with NULL or invalid
   `source_raw_response_id`
6. `section_confidence_drift` — rolling-window check on
   `artifact_sections.confidence` per `(form_family, section_key)`

V1.5 (after `coverage_membership` is populated): seventh check
`expected_coverage` consumes `expectations.py`.

## Working Rules

- **Steward is the load-bearing priority.** Ahead of further analyst feature
  expansion (LLM synthesis, transcripts, news, monitoring). Data trust
  before product expansion.
- **Steward never mutates source data.** It surfaces; the operator (or
  agent) decides; actions go through existing ingest scripts and the typed
  action callables. Silent fixes are how you lose the thread of what's true.
- **Every operator action is a typed callable with `actor` field.** UI route
  handlers are 3-line wrappers. Future agent calls the same function with a
  different actor.
- **New verticals ship with their expectations and steward checks.**
  Adding a new data vertical (transcripts, news, prices, options, macro,
  etc.) ships its expectations entry and at least basic steward checks
  (presence, freshness, orphan detection) in the same commit. Parallel to
  "schema changes ship with their docs."
- **Per-check automation, not global.** Automation level is per-check-type;
  promotion happens by demonstrated correctness; demotion is one click.
- **Suppression has structured storage.** Suppression with reason and
  optional expiry lives on the closed finding; the runner respects active
  suppressions when reopening fingerprints.

## Build Order

Status markers (✅ done · 🚧 in progress · ⏳ next · ⬜ not started).

### V1 — deterministic foundation

1. ✅ migration 017: `coverage_membership` + `data_quality_findings` +
   `v_open_quality_signals` (view in `db/queries/15_v_open_quality_signals.sql`).
   `system.md` v1 Tables status flipped to `built`; `arrow_db_schema.html`
   regenerated.
2. ✅ `src/arrow/steward/actions.py` — action callables + `fingerprint.py`.
   Action surface: `open_finding` (idempotent, suppression-respecting),
   `close_finding` + `resolve`/`suppress`/`dismiss` wrappers,
   `add_to_coverage`/`remove_from_coverage`/`set_coverage_tier`. Every
   action takes `actor: str` and appends to `history` jsonb. Tests:
   29 new (10 unit + 19 integration); full suite 250/250.
3. ✅ `src/arrow/steward/registry.py` + `runner.py` + first check
   (`zero_row_runs`). `Check` ABC, `@register` decorator, `Scope`,
   `FindingDraft`. Runner orchestrates execution, persists via
   `open_finding`, auto-resolves cleared findings within run scope,
   captures per-check error without aborting the whole run. First check
   surfaces succeeded ingest_runs that wrote 0 across the recognized
   `OUTPUT_KEYS`. Tests: 11 new integration; full suite 261/261.
4. ✅ `scripts/run_steward.py` CLI. Args: `--ticker` (repeat),
   `--vertical`, `--check`, `--actor`, `--verbose`. Emits JSON summary
   on stdout; per-finding lines to stderr in verbose mode. Exit code 1
   if any check raised. Tests: 7 new (covering scope passthrough,
   verbose, actor capture, exit code, unknown check name).
5. ⬜ Remaining five deterministic checks
6. ⬜ Dashboard `/findings` list + detail + lifecycle POSTs
7. ⬜ Dashboard `/coverage` matrix + per-ticker pane + add/remove ticker
8. ⬜ `expectations.py` module + `expected_coverage` check (V1.5)

### V2 — LLM as advisor

9. ⬜ Triage suggester: RAG over closed findings + dashboard suggestion chip
   + confirm/override flow
10. ⬜ `LLMCheck` infrastructure + first two LLM checks
    (`segment_taxonomy_drift`, `extraction_quality_regression`)
11. ⬜ Investigator tool: on-demand button, agent uses retrieval primitives
12. ⬜ Rubber-stamping detection: weekly sampled review pane

### V3 — autonomy on proven check types

13. ⬜ `automation_policy` table (replaces Python constant default)
14. ⬜ Activity feed dashboard view (replaces inbox as primary)
15. ⬜ Revert button (24h window on agent-executed actions)
16. ⬜ Drift detection runner (weekly sample re-judgment + auto-demote)

### V4+ (Year 2+)

17. ⬜ Specialized fine-tune of an open base model on accumulated
    `(finding, decision, override?, outcome)` corpus

## Non-Goals For V1

- No LLM calls anywhere in V1
- No `coverage_expectations` table — Python module only
- No `coverage_expectation_exceptions` table — suppression with expiry
  covers it
- No separate `finding_audit` table — `history` jsonb on row
- No `automation_policy` table — Python constant default; promotes to table
  in V3
- No three-state lifecycle — open → closed with structured `closed_reason`
- No `/runs` or `/traces` panes — V1.5; JSONL traces still on disk;
  `ingest_runs` still queryable
- No template `_layout.html.j2` refactor — defer until a 4th template needs
  the topbar
- No nightly cron — manual sweep first; cron once cadence proves out
- No post-ingest auto-hook on by default — optional flag; default-on after
  the first 2 weeks of operation if used every time
- No agent — substrate built for one (action callables with `actor`,
  structured `suggested_action`, history capture); agent itself is V2+
- No LLM-as-judge checks — six deterministic checks first; LLM checks
  added in V2 once a deterministic baseline exists

## Cross-References

- Architecture north star: `docs/architecture/system.md`
- Analyst runtime (sibling): `docs/architecture/analyst_runtime.md`
- Dashboard surface: `docs/architecture/dashboard.md`
- Repository folder map: `docs/architecture/repository_flow.md`
- Existing inline-validation flag table: see `db/schema/011_data_quality_flags.sql`
  and `db/schema/012_data_quality_flags_superseded_resolution.sql`. Steward
  layer adds findings as a separate, broader-scope table; the
  `v_open_quality_signals` view UNIONs both for dashboard consumption.
