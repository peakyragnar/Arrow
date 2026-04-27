# Steward Operator Runbook

Status: V1.3 in operation. This is the working manual for using the
steward day-to-day. It complements (does not replace)
[`docs/architecture/steward.md`](../architecture/steward.md), which
covers the runtime design and V1→V3 trajectory.

## Current focus (2026-04-27)

- **Steward V1.3:** complete. Operating mode — ingest companies,
  triage findings, accumulate notes for V2 training corpus.
  Next steward milestone (V2 LLM suggester) needs ~50 closed
  findings; no urgent steward work.
- **Active development:** **analyst transcript evidence retrieval** —
  FMP transcript ingest is shipped and backfilled. The current analyst
  step is making those transcript chunks useful in analyst answers:
  `src/arrow/retrieval/transcripts.py`, `scripts/analyst_transcript_brief.py`,
  and the `scripts/ask_arrow.py` revenue-driver packet.
- **Hard rule still in effect:** no foreign-filer ingestion (TSM,
  ASML, BABA, etc.) — see § "Hard rule: do NOT ingest foreign
  filers yet" below.

There are two audiences for this doc:

1. **You (today)** — operating the steward by hand.
2. **The future agent (months from now)** — being trained on the
   decisions you record while operating.

The disciplines this runbook recommends serve both. Good operating
practice IS good training-data practice. The agent V2 will learn
what to do by imitating what you did.

## What you operate

Three surfaces, all under the same FastAPI dashboard:

| Surface | Route | What it does |
|---|---|---|
| **Findings inbox** | [`/findings`](http://127.0.0.1:8000/findings) | Open data-quality issues to triage |
| **Coverage matrix** | [`/coverage`](http://127.0.0.1:8000/coverage) | What data Arrow has per (ticker × vertical), and per-ticker membership controls |
| **Per-ticker metrics** | [`/t/{TICKER}`](http://127.0.0.1:8000/t/AMD) | Existing analyst lens (unchanged by V1) |

Plus one CLI:

```bash
uv run scripts/run_steward.py [--ticker T] [--check NAME] [--vertical V] [--verbose]
```

Run with no args = universe sweep, all 6 checks. Run with `--ticker
PLTR` after re-ingesting PLTR to see the impact immediately.

## Starting the dashboard

```bash
uv run uvicorn scripts.dashboard:app --reload
# http://127.0.0.1:8000
```

Stays bound to localhost. Reads-only on the metric views; lifecycle
POSTs mutate `data_quality_findings` and `coverage_membership` only.

## The recommended cadence

The single most important thing you can do for V2 is **process
findings on a regular cadence** so the audit trail accumulates with
fresh context. Stale evidence is bad training data.

- **Daily (10–15 min):** open `/findings?status=open`, triage
  anything new since yesterday. Resolve / suppress / dismiss with a
  note.
- **Weekly (30 min):** scan `/coverage` for changes. Add new
  tickers, demote / remove ones you no longer care about. Spot-check
  the closed-findings tail — search for patterns ("did I suppress
  something I shouldn't have?").
- **After every ingest** of any ticker:
  ```bash
  uv run scripts/run_steward.py --ticker TICKER --verbose
  ```
  Then refresh `/findings?ticker=TICKER&status=open` to see what
  changed.

If a week goes by without you opening the inbox, the training data
loses freshness — by the time you triage, the original context
("why did I just re-ingest?") may be gone.

## Triage workflow per finding

Click any row in `/findings` to open the detail page. You'll see:

- **Summary** — one-line description
- **Suggested action** — prose explaining likely cause + a CLI
  command to fix
- **Evidence** — structured jsonb the check captured
- **History** — every state change so far
- **Action buttons** — Resolve / Suppress / Dismiss

For each finding, ask three questions in order:

### 1. Is this real?

If the check fired but the underlying state is fine (false positive),
**Dismiss** with a note explaining why the check was wrong. Example:
"Section extractor flagged unparsed_body, but the filing genuinely
has no Item 7 because it's an annual report with mandatory exemption."

### 2. Is it actionable now?

If real but you can't act today (vendor delay, awaiting next quarter,
known taxonomy change), **Suppress** with reason and optional expiry.
Example: `reason="AVGO renamed segment 'Other' to 'Infrastructure
Software' in Q3 — confirmed in 10-Q"`, `expires=` (leave blank for
permanent).

### 3. Can you fix the underlying problem?

If yes, do the fix (typically: re-run an ingest script per the
suggested command), then **Resolve** with a note describing what you
did. Example: "Ran `ingest_segments.py PLTR`; segments now landing
through Q4."

The next steward sweep will also auto-resolve the finding (because
the fingerprint stops surfacing), but explicit Resolve adds a
training data point that says "I caused this to clear by doing X."

## Writing notes that train V2 well

This is the load-bearing discipline. Every Resolve / Suppress /
Dismiss writes to `data_quality_findings.history` jsonb. V2's
suggester reads similar past findings + their notes to propose
actions on new ones. The quality of its suggestions is bounded by
the quality of your notes.

### The dashboard pre-fills the structure for you

When you click any lifecycle button, the note input is **already
populated** with a 3-line structured template derived from the
finding's `suggested_action.prose`:

```
Action: ran `uv run scripts/backfill_fmp.py PLTR`
Cause: financials below 5y standard (12 of 20 periods)
Expected: finding auto-resolves on next sweep when fingerprint stops surfacing
```

You don't have to author this from scratch. You **read it, edit
the specifics, and click**. If it's accurate as-is, accept it. If
the cause is wrong (you actually re-ran with different args, or
the cause was a vendor issue not a backfill window), edit that
line. If the action you took was different from what the
suggested_action proposed, edit Action.

The Action / Cause / Expected shape is the load-bearing structure.
Keep it. V2 trains on `(finding_evidence, action_kind,
structured_note)` tuples and the labels make it parseable. If you
strip the labels and just write free prose, V2 has to guess at
which clause is the cause vs the expected outcome.

### Good notes look like

- **Resolve:**
  ```
  Action: ran `uv run scripts/backfill_fmp.py AMD --since 2018-01-01`
  Cause: financials below 5y standard (16 of 20 periods); backfill window was set to 2020 originally
  Expected: 28 quarters present, finding auto-resolves on next sweep
  ```
- **Suppress:**
  ```
  Action: suppressed (no expiry)
  Cause: CRWV IPO 2025-03-28; only 4 quarters of public history exist
  Expected: revisit when CRWV has ≥20 quarters (~2030)
  ```
- **Dismiss:**
  ```
  Action: dismissed (false positive)
  Cause: extraction_method_drift fired on 4-row window during holiday backfill — not real regression
  Expected: tune MIN_ROWS threshold if this recurs in next 30 days
  ```

### Bad notes that produce bad training

- `"x"` — the agent learns nothing.
- `"ok"` — same.
- `"fixed"` — fixed *what*, *how*?
- Empty — at least Resolve takes an empty note; don't.
- Free-form prose without the Action/Cause/Expected labels — V2
  has to infer where each piece of meaning is.

### A useful test for your note

Read it back to yourself a month from now without context. Does it
explain the situation well enough that you'd make the same decision
again? If not, it's not enough for V2 either.

## Per-finding-type decision recipes

The 6 V1 checks have distinct typical resolutions. Use these as
starting points; refine as you accumulate experience.

### `zero_row_runs`

Ingest run succeeded but wrote nothing.

- **Resolve** path: re-run the underlying ingest with corrected
  args (broader window, fixed ticker). Note the corrected command.
- **Suppress** path: vendor genuinely has no data for the scope.
  Note the vendor + scope. Use `expires` to retry next quarter
  for periodic data.
- **Dismiss** rare here — usually the check is right.

### `unresolved_flags_aging`

Inline `data_quality_flags` open >14 days.

- **Resolve** by reviewing the underlying flag (use the suggested
  `review_flags.py --show`). Most resolve via approve / override /
  accept.
- **Suppress** path: flag is informational and not worth chasing
  (e.g. <0.1% subtotal drift). Note the rationale.

### `sec_artifact_orphans`

SEC filing with no extracted sections / text units.

- **Resolve** path: re-extract via the suggested command after
  fixing the extractor (if the layout is one we should handle) or
  via re-ingestion (if the artifact was incomplete).
- **Suppress** path: filing genuinely has no extractable structure
  (rare).

### `unparsed_body_fallback`

Extractor fell back to `unparsed_body`.

- **Resolve** path: update the SEC qualitative extractor regex,
  re-extract the affected artifacts. Note the layout pattern.
- **Suppress** path: one-off filing with a layout you don't want to
  invest in handling. Note the layout pattern.

### `expected_coverage`

Coverage member missing data per its tier expectations.

- **Resolve** path: run the suggested re-ingest command. The
  finding auto-clears once the data lands.
- **Suppress** path: ticker truly can't have this data (recent
  IPO, vendor doesn't cover). **Better than suppressing repeatedly:**
  add a `PER_TICKER_OVERRIDES` entry in
  `src/arrow/steward/expectations.py` so the rule itself is right.
  See the CRWV / GEV examples already there.

## Aspirational checks (need a mature corpus before they fire usefully)

A few V1 checks are built and registered but won't produce
actionable signal until the underlying data has accumulated. Don't
let them confuse early triage; revisit when the relevant corpus is
~3+ months old.

### `extraction_method_drift`

Compares the share of `deterministic` SEC extractions in the recent
30 days vs the prior 60 days. **Requires ≥10 sections in EACH
window** (per `(form_family, section_key)`) for the test to fire.
On a young corpus or after a backfill burst, the seasonality of
ingest can superficially look like drift. Treat any early findings
from this check as exploratory until the corpus is steady-state
(≥90 days of organic ingest at a consistent cadence).

When it does fire later:
- **Investigate first** — pull the demoted rows with the suggested
  command. Identify the common pattern (new filer? new template?).
- **Resolve** path: fix the extractor, re-extract.
- **Suppress** path: drift was caused by a one-time burst of
  off-pattern filings (acquisition flurry, etc.) and won't recur.

## Coverage management

`/coverage` shows every ticker in the database — every company you've
ever ingested. **There is no separate membership step.** If a ticker
is in `companies`, the steward enforces the standard against it.
Coverage = the database itself.

### The standard (applied to every tracked ticker)

- **Financials:** 5 years of quarterly data (≥20 distinct periods)
- **Segments:** must be present
- **Employees:** latest count within the last ~14 months
- **SEC qualitative:** ≥20 distinct fiscal periods of 10-K/10-Q

### Legitimate exceptions live in suppression notes, not in code

Some tickers can't meet the standard for real reasons (recent IPO,
spinoff, vendor doesn't cover it, filer doesn't report segments).
In those cases, the steward fires a finding and **you suppress with
a clear reason**. The suppression reason IS the acceptance criteria
— it lives in the audit trail and becomes V2 training data.

### Adding a ticker

There is no "Add to coverage" button in V1.2+. To track a new ticker:

```bash
uv run scripts/ingest_company.py TICKER
```

That seeds the ticker into `companies` (and runs the normal flow:
backfill financials, segments, employees, SEC filings). The next
steward sweep automatically evaluates it.

#### Hard rule: do NOT ingest foreign filers yet

Foreign filers (companies that file SEC **20-F** annual / **6-K**
interim instead of 10-K / 10-Q) are not yet supported across Arrow's
pipeline. Specifically broken or missing for foreign filers:

- **SEC qualitative ingest** doesn't fetch 20-F or 6-K at all (only
  10-K/10-Q). Operator gets `expected_coverage` finding for empty
  sec_qual.
- **Employees ingest** filters `formType != '10-K'` and drops 20-F
  rows; foreign filers get 0 employee facts written even when FMP
  has the data.
- **Financials** load OK, but FMP's TWD/EUR/etc.→USD translation
  produces more soft flags than US filers (e.g. TSMC ingest produced
  21 vs META's 4).
- **`artifacts.artifact_type`** allowed values (`10k`/`10q`/`8k`/
  `press_release`) don't include 20-F/6-K — extending requires a
  schema change.

Examples to avoid until foreign filer support lands:
**TSM** (Taiwan Semiconductor), **ASML**, **TSMC**, **NVO** (Novo
Nordisk), **TM** (Toyota), **SAP**, **BABA**, **TCEHY**, **SHOP**
(Shopify is Canadian — files 40-F), **NTES**, **JD**, etc. ADRs
generally indicate foreign filers; if in doubt, check whether the
ticker appears under `formType: '20-F'` on
[sec.gov/cgi-bin/browse-edgar](https://www.sec.gov/cgi-bin/browse-edgar)
before ingesting.

Foreign filer support is a focused 2–3 day project (extend SEC
fetcher, add 20-F/6-K artifact types, add 20-F section keys to
extractor, fix employees-ingest filter) — it's recorded in the
backlog but not on the immediate path. Doing it piecemeal as bugs
surface produces an incomplete patchwork; doing it as one project
later produces clean, complete support.

If a foreign filer was ingested by accident, delete it cleanly via
the cascade (`financial_facts` → `companies`; CASCADE handles
findings + flags). Re-ingest after foreign-filer support ships.

### Stopping tracking on a ticker

Two paths, depending on intent:

- **Common:** suppress its findings with a structured reason like
  *"PLTR ingested for one-off Q3 lookup; not actively tracking — suppress all `expected_coverage` for this ticker permanently."*
  The data stays. The audit trail records the decision.
- **Rare:** delete the ticker from `companies`. This requires
  deleting all its data first (FK constraints `ON DELETE RESTRICT`
  block otherwise). Use only when you genuinely don't want the data
  at all anymore.

## Operating to teach V2

The whole V1 design is built on one assumption: **every triage
decision you make becomes a labeled training example for V2's
suggester** ([steward.md § LLM
Trajectory](../architecture/steward.md#llm-trajectory-v1-→-v2-→-v3)).
Decisions are captured in `data_quality_findings.history` jsonb
with actor, before/after state, timestamp, and reason. The
suggester later does RAG over these to propose actions on new
findings.

What this means in practice:

- **Quantity matters less than consistency.** 50 high-quality
  triage decisions beat 500 low-quality ones. The agent is going
  to *imitate* you, so a small body of decisive, well-noted choices
  is better than a large body of vague ones.

- **Consistency within finding types matters most.** If you
  resolve `zero_row_runs` with "re-ran ingest" 10 times in a row,
  the agent learns "zero_row_runs → re-ingest" with high confidence.
  If you sometimes resolve, sometimes suppress, sometimes dismiss,
  with no pattern, the agent learns nothing useful.

- **Reasoning in notes is the actual gradient signal.** When V2
  is trained, the model sees `(finding evidence, decision, note)`
  tuples. The note is what teaches it WHY one finding gets
  resolved vs another that gets suppressed despite looking similar.

- **Targeted expectations beat blanket suppressions.** When you
  find yourself suppressing the same finding shape across multiple
  tickers, that's a sign the expectation itself is wrong — fix it
  in `expectations.py` (or open an `INVESTIGATE` finding manually
  and document the pattern). The agent will learn from your
  *targeted* decisions; suppress-spam teaches it nothing.

- **The audit trail is exhaustive.** Every state change writes a
  history entry with timestamp + actor + reason. Don't worry about
  logging things separately — operating the dashboard IS the log.
  Just make sure the notes are honest.

## Useful queries when you want raw data

For when the dashboard isn't enough:

```sql
-- All findings closed in the last 7 days, with resolution reason
SELECT id, ticker, vertical, finding_type, closed_reason, closed_note
FROM data_quality_findings
WHERE status = 'closed' AND closed_at > now() - interval '7 days'
ORDER BY closed_at DESC;

-- How consistent are my decisions per check type?
SELECT finding_type, closed_reason, COUNT(*)
FROM data_quality_findings
WHERE status = 'closed'
GROUP BY finding_type, closed_reason
ORDER BY finding_type, COUNT(*) DESC;

-- Currently-active suppressions (what's filtered out of my inbox)
SELECT id, ticker, vertical, summary, suppressed_until, closed_note
FROM data_quality_findings
WHERE status = 'closed'
  AND closed_reason = 'suppressed'
  AND (suppressed_until IS NULL OR suppressed_until > now());
```

## What's documented elsewhere

- **Architecture & runtime design:**
  [`docs/architecture/steward.md`](../architecture/steward.md) —
  the runtime spine, stage contracts, three-agent split, V1→V3 LLM
  trajectory, autonomy curve, working rules, known limitations.
- **Action callable contracts:**
  [`src/arrow/steward/actions.py`](../../src/arrow/steward/actions.py) —
  every operator action with docstrings explaining idempotency,
  concurrency, and the suppression-respect contract.
- **Per-check reference:**
  [`src/arrow/steward/checks/`](../../src/arrow/steward/checks/) —
  each check's module docstring explains what it detects, why,
  scope behavior, and fingerprint shape.
- **Schema:**
  [`db/schema/017_steward_layer.sql`](../../db/schema/017_steward_layer.sql)
  + [`db/queries/15_v_open_quality_signals.sql`](../../db/queries/15_v_open_quality_signals.sql).
- **Working rules** (parallel to "schema changes ship with their
  docs"):
  [`AGENTS.md`](../../AGENTS.md) § Working Rules.

## When you find a gap in this doc

You'll discover patterns we haven't covered (this is V1, you'll be
the first real operator). When you do:

- Add a section here. Operator runbooks rot fastest when no one
  updates them after first contact with reality.
- If the gap is a missing decision recipe for a check type, add it
  under "Per-finding-type decision recipes" above with a couple of
  example notes.
- If the gap is a workflow you keep doing manually, that's a hint
  it should become a button. File it as a steward V2/V3 candidate
  in [`docs/architecture/steward.md`](../architecture/steward.md)
  Build Order.
