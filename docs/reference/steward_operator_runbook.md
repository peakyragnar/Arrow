# Steward Operator Runbook

Status: V1 in operation. This is the working manual for using the
steward day-to-day. It complements (does not replace)
[`docs/architecture/steward.md`](../architecture/steward.md), which
covers the runtime design and V1→V3 trajectory.

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

### Good notes look like

- **Resolve:** `"Re-ran backfill_fmp.py with --since 2018-01-01;
  AMD financials now have 28 quarters."` — names the action and
  the observed effect.
- **Suppress:** `"FMP genuinely has no pre-2024 employee count
  for CRWV (private until IPO Mar 2025). Permanent suppression."`
  — names the upstream cause and explains why it's not actionable.
- **Dismiss:** `"section_confidence_drift fired on a 4-row window
  during a holiday backfill; not a real regression. Threshold may
  need tuning if this recurs."` — names what actually happened
  and what would change the verdict.

### Bad notes that produce bad training

- `"x"` — the agent learns nothing.
- `"ok"` — same.
- `"fixed"` — fixed *what*, *how*?
- Empty — at least Resolve takes an empty note; don't.

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

`/coverage` is where you decide which tickers the steward enforces
expectations against.

### Adding a ticker

1. The ticker must already exist in `companies` (seeded via
   `uv run scripts/ingest_company.py TICKER`). The dropdown only
   shows seeded but unmembered tickers.
2. Pick a tier:
   - **`core`** — full quality bar. 5y financials, segments,
     employees recency, 5y SEC qual.
   - **`extended`** — lighter bar. 2y financials, SEC qual present.
3. Optional notes (e.g. "Q4 watchlist", "AI infra deep dive").
4. Click **Add to coverage**. Steward will start evaluating it on
   the next sweep.

### Removing a ticker

`/coverage/{TICKER}` → "Remove from coverage_membership". Confirms
via JS prompt. **Does NOT delete data, facts, artifacts, or open
findings** — only the membership claim. Open findings stay open
until you triage them separately. This is intentional: a misclick
shouldn't cascade-destroy.

### Changing tier

`/coverage/{TICKER}` → tier dropdown in the Membership block →
"change". Reapplies expectations on the next sweep; existing
findings against the old tier auto-resolve and new findings
against the new tier open as appropriate.

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
