# Estimates Ingest Plan

Status: active plan; pre-implementation. Endpoint probe validated 2026-04-30.

This document is the v1 plan to add an analyst estimates and ratings layer to
Arrow. It sits next to financials, transcripts, SEC qualitative, and prices on
the same FMP-first + raw-cache + immutable-artifacts substrate.

The plan was scoped through the same lean-default discipline that produced the
prices plan. The full surface area FMP exposes — bulk endpoints, daily
ratings score, historical consensus snapshotting, analyst-firm dimension — is
deferred. What survives is the minimum that earns its keep against four
question classes already arriving in `/ask`.

## Goal

Answer four classes of question in `/ask` that we can't today:

- **Forward expectations.** What does the street expect for NVDA next quarter
  and next FY?
- **Beat / miss history.** How often does META beat? By how much? Did Q3
  surprise?
- **Target gaps.** Is AMD trading below analyst targets? By how much, vs how
  many analysts?
- **Sentiment events.** Has anyone upgraded / downgraded TSLA in the last 90
  days?

## What's In v1

| Component | Why it earns its keep |
|---|---|
| `analyst_estimates` table | Per-period consensus (revenue / EPS / EBITDA / EBIT / net income / SG&A low / avg / high + analyst counts). Substrate for forward valuation and the optional revision-tracking layer later. |
| `price_target_consensus` table | Snapshot of target high / low / median / consensus per security. Substrate for target-gap screens. |
| `earnings_surprises` table | Historical per-quarter actual vs estimate (EPS + revenue). Direct beat / miss series. |
| `analyst_grades` table | Event log of grade changes (upgrade / downgrade / maintain) with grading firm. |
| `analyst_price_targets` table | Event log of individual analyst price-target updates with full news provenance (firm, analyst, source URL, price-when-posted). |
| 4 `/ask` tools (`read_consensus`, `read_target_gap`, `read_surprise_history`, `recent_analyst_actions`) | One per data shape; planner composes for cross-ticker / streak questions. |
| 4 steward checks (estimates freshness, target-consensus freshness, orphan, surprise sanity) | Per the verticals-ship-with-checks rule. |
| Same 18-ticker universe (common stock only) | ETFs / indices have no estimates. SPY / QQQ excluded. |

## What's Deferred or Cut

| Item | Action | Trigger to revisit |
|---|---|---|
| Daily snapshot history of consensus (revision tracking) | **Defer** (lean default) | First time "estimate revisions" or "are analysts moving up?" becomes a routine question. Schema is shaped to allow this without a migration — see `analyst_estimates` notes below. |
| `/stable/historical-ratings` (aggregated daily score) | Cut | Re-add only if the grades event log proves too noisy and we want a smoothed series. Don't want a second source of truth. |
| `/stable/earnings-calendar` | Cut from v1 | Earnings dates are already implicit in `financial_facts.published_at` and `earnings_surprises.announcement_date`. Re-add if forward earnings dates become a primary use. |
| Bulk endpoints (`earnings-surprises-bulk`, etc.) | Cut | Per-ticker calls cost ~108 total for the universe; bulk has no leverage at this scale. |
| FMP `/stable/ratios` forward P / E and forward EV / EBITDA | Cut | We compute valuation in `formulas.md` as canonical. Same single-source-of-truth trap the prices vertical avoided. |
| Analyst-firm dimension table | Defer | Carry firm name as text in `analyst_grades.grading_company` and `analyst_price_targets.analyst_company`. Add the dimension if cross-firm queries become routine. |
| `n_analysts_floor` steward check | Defer | Calibrate after backfill. NVDA quarterly shows `numAnalystsEps=20`, CRWV near-quarter shows `numAnalystsEps=13`; can't pick a floor before observing the universe. |
| `consensus_period_coverage` check (≥1 forward annual + ≥1 forward quarter per ticker) | Defer | Probe (2026-04-30) confirmed all 3 sample tickers including CRWV have both forward annual and forward quarter coverage; can layer this in once we've seen the full 18. |
| Pagination of `analyst_price_targets` beyond ~20 events per ticker | Calibrate during build | Probe used `limit=20&page=0`. NVDA has hundreds of historical events; backfill walks pages until empty. |

## Endpoint Choice (probed 2026-04-30)

The probe hit each endpoint once for NVDA (large, heavy coverage), PLTR
(mid-cap, growth), and CRWV (recent IPO, thin coverage by construction). All
six endpoints returned cleanly across all three tickers.

| Endpoint | Returns | Cadence | Cache path |
|---|---|---|---|
| `/stable/analyst-estimates?symbol=X&period=annual` | Per fiscal year (forward + historical): revenue / EBITDA / EBIT / net income / SG&A / EPS low / high / avg + analyst counts | Replace-by-(ticker, period_kind) | `data/raw/fmp/analyst-estimates/annual/{TICKER}/{fetched_at}.json` |
| `/stable/analyst-estimates?symbol=X&period=quarter` | Per fiscal quarter, same shape | Replace-by-(ticker, period_kind) | `data/raw/fmp/analyst-estimates/quarter/{TICKER}/...` |
| `/stable/price-target-consensus?symbol=X` | Single row: target high / low / median / consensus | Replace-by-ticker | `data/raw/fmp/price-target-consensus/{TICKER}/...` |
| `/stable/earnings?symbol=X` | Per announcement: epsActual / epsEstimated / revenueActual / revenueEstimated / lastUpdated | Append (immutable, dedup on natural key) | `data/raw/fmp/earnings/{TICKER}/...` |
| `/stable/grades?symbol=X` | Full history of rating actions: gradingCompany, previousGrade, newGrade, action | Append (immutable) | `data/raw/fmp/grades/{TICKER}/...` |
| `/stable/price-target-news?symbol=X&page=N&limit=L` | Paginated event log: analystName, analystCompany, priceTarget, adjPriceTarget, priceWhenPosted, news provenance | Append (immutable, paginated walk) | `data/raw/fmp/price-target-news/{TICKER}/page-{N}.json` |

Two probe findings worth noting:

1. **`analyst-estimates` returns both forward AND historical periods.** NVDA
   annual goes 2010 → 2031, quarterly goes 2024-Q1 → 2029-Q1. Past rows are
   frozen historical-consensus snapshots from FMP's archive. We store all
   periods (forward + past) — past rows pair with `earnings_surprises` for
   richer surprise context. If FMP revises a historical row, delete-and-replace
   takes the latest. (The deferred snapshot-history layer would preserve our
   own record of FMP's revisions.)
2. **`grades` returns full history in one call**, no pagination. NVDA returned
   1100 rows back to 2012. `price-target-news` paginates with `page` / `limit`.
   Backfill walks pages until an empty response.

## Schema (5 new tables)

Migration `024_estimates.sql`. All five tables anchor to `securities.id`
(common-stock only — enforced by app-level filter, not a CHECK constraint, to
keep the migration simple; `securities.kind` filter at ingest time).

### `analyst_estimates`

```sql
CREATE TABLE analyst_estimates (
    security_id     bigint NOT NULL REFERENCES securities(id) ON DELETE RESTRICT,
    period_kind     text NOT NULL,          -- 'annual' | 'quarter'
    period_end      date NOT NULL,
    revenue_low     numeric(28,2),
    revenue_avg     numeric(28,2),
    revenue_high    numeric(28,2),
    ebitda_low      numeric(28,2),
    ebitda_avg      numeric(28,2),
    ebitda_high     numeric(28,2),
    ebit_low        numeric(28,2),
    ebit_avg        numeric(28,2),
    ebit_high       numeric(28,2),
    net_income_low  numeric(28,2),
    net_income_avg  numeric(28,2),
    net_income_high numeric(28,2),
    sga_expense_low  numeric(28,2),
    sga_expense_avg  numeric(28,2),
    sga_expense_high numeric(28,2),
    eps_low         numeric(18,6),
    eps_avg         numeric(18,6),
    eps_high        numeric(18,6),
    num_analysts_revenue  integer,
    num_analysts_eps      integer,
    fetched_at      timestamptz NOT NULL,
    source_raw_response_id  bigint NOT NULL REFERENCES raw_responses(id),
    PRIMARY KEY (security_id, period_kind, period_end),
    CONSTRAINT analyst_estimates_period_kind_check
        CHECK (period_kind IN ('annual','quarter'))
);
CREATE INDEX analyst_estimates_period_end_idx ON analyst_estimates (period_end);
```

Replace-by-(security_id, period_kind) on each ingest. **Migration to snapshot
history is a one-line PK change** — add `fetched_at` to the PK and stop the
delete. That is the deferred path.

### `price_target_consensus`

```sql
CREATE TABLE price_target_consensus (
    security_id     bigint PRIMARY KEY REFERENCES securities(id) ON DELETE RESTRICT,
    target_high     numeric(18,6),
    target_low      numeric(18,6),
    target_median   numeric(18,6),
    target_consensus numeric(18,6),
    fetched_at      timestamptz NOT NULL,
    source_raw_response_id  bigint NOT NULL REFERENCES raw_responses(id)
);
```

One row per security; replaced on each ingest. No `n_analysts` field — the
endpoint doesn't expose one. Same migration path to history (add `fetched_at`
to PK) if needed.

### `earnings_surprises`

```sql
CREATE TABLE earnings_surprises (
    security_id     bigint NOT NULL REFERENCES securities(id) ON DELETE RESTRICT,
    announcement_date  date NOT NULL,
    eps_actual      numeric(18,6),
    eps_estimated   numeric(18,6),
    revenue_actual  numeric(28,2),
    revenue_estimated  numeric(28,2),
    last_updated    date,                   -- FMP's lastUpdated, distinct from ingested_at
    source_raw_response_id  bigint NOT NULL REFERENCES raw_responses(id),
    ingested_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (security_id, announcement_date)
);
CREATE INDEX earnings_surprises_announcement_idx ON earnings_surprises (announcement_date);
```

Append on first sight, dedup-replace on subsequent ingests (a row's actuals
populate after announcement; FMP also nudges `last_updated`). Natural key is
(security, announcement_date) — one announcement per fiscal period.

### `analyst_grades`

```sql
CREATE TABLE analyst_grades (
    id              bigserial PRIMARY KEY,
    security_id     bigint NOT NULL REFERENCES securities(id) ON DELETE RESTRICT,
    action_date     date NOT NULL,
    grading_company text NOT NULL,
    previous_grade  text,
    new_grade       text,
    action          text NOT NULL,           -- 'upgrade' | 'downgrade' | 'maintain'
    source_raw_response_id  bigint NOT NULL REFERENCES raw_responses(id),
    ingested_at     timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT analyst_grades_action_check
        CHECK (action IN ('upgrade','downgrade','maintain'))
);
CREATE UNIQUE INDEX analyst_grades_natural_key
    ON analyst_grades (security_id, action_date, grading_company,
                       COALESCE(previous_grade,''), COALESCE(new_grade,''), action);
CREATE INDEX analyst_grades_date_idx ON analyst_grades (security_id, action_date DESC);
```

Append-only event log. Same firm publishing multiple actions on the same day
with different grade transitions counts as multiple rows; the natural-key
index handles re-ingest dedup.

### `analyst_price_targets`

```sql
CREATE TABLE analyst_price_targets (
    id              bigserial PRIMARY KEY,
    security_id     bigint NOT NULL REFERENCES securities(id) ON DELETE RESTRICT,
    published_at    timestamptz NOT NULL,
    analyst_name    text,
    analyst_company text,
    price_target    numeric(18,6),
    adj_price_target numeric(18,6),         -- split-adjusted target
    price_when_posted numeric(18,6),
    news_url        text,
    news_title      text,
    news_publisher  text,
    news_base_url   text,
    source_raw_response_id  bigint NOT NULL REFERENCES raw_responses(id),
    ingested_at     timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX analyst_price_targets_natural_key
    ON analyst_price_targets (security_id, published_at,
                              COALESCE(analyst_company,''),
                              COALESCE(price_target::text,''));
CREATE INDEX analyst_price_targets_security_date_idx
    ON analyst_price_targets (security_id, published_at DESC);
```

`adj_price_target` parallels the prices vertical's `adj_close` — same
split-adjustment treatment. `previous_price_target` is not in the FMP
response; the per-firm time series in this table reproduces deltas where
needed.

## Module Layout

```
src/arrow/ingest/fmp/estimates.py        — 6 endpoint clients + 5 ingest fns
scripts/ingest_estimates.py              — thin entrypoint (mirrors ingest_prices.py)
db/schema/024_estimates.sql              — the migration above
src/arrow/steward/checks/estimates.py    — 4 checks (registered into REGISTRY)
src/arrow/agents/tools/estimates.py      — 4 /ask tools
                                           (path: verify against where read_prices lives
                                            before the build)
```

## Field-Name Mapping (FMP camelCase → DB snake_case)

| FMP field | DB column | Notes |
|---|---|---|
| `revenueLow/Avg/High` | `revenue_low/avg/high` | |
| `ebitdaLow/Avg/High` | `ebitda_low/avg/high` | |
| `ebitLow/Avg/High` | `ebit_low/avg/high` | Operating income, not EBITDA |
| `netIncomeLow/Avg/High` | `net_income_low/avg/high` | |
| `sgaExpenseLow/Avg/High` | `sga_expense_low/avg/high` | |
| `epsLow/Avg/High` | `eps_low/avg/high` | |
| `numAnalystsRevenue` | `num_analysts_revenue` | |
| `numAnalystsEps` | `num_analysts_eps` | |
| `date` (estimates) | `period_end` | Fiscal period end date |
| `targetHigh/Low/Consensus/Median` | `target_high/low/consensus/median` | |
| `epsActual / epsEstimated` | `eps_actual / eps_estimated` | |
| `revenueActual / revenueEstimated` | `revenue_actual / revenue_estimated` | |
| `date` (earnings) | `announcement_date` | |
| `lastUpdated` (earnings) | `last_updated` | |
| `gradingCompany` | `grading_company` | |
| `previousGrade / newGrade` | `previous_grade / new_grade` | |
| `action` | `action` | Values: `upgrade`, `downgrade`, `maintain` |
| `publishedDate` | `published_at` | Timestamp; tz-aware |
| `analystName / analystCompany` | `analyst_name / analyst_company` | |
| `priceTarget / adjPriceTarget` | `price_target / adj_price_target` | |
| `priceWhenPosted` | `price_when_posted` | |
| `newsURL / newsTitle / newsPublisher / newsBaseURL` | `news_url / news_title / news_publisher / news_base_url` | |

## /ask Integration (4 tools)

Each tool follows the existing `read_prices` / `read_valuation` pattern: a
single-purpose primitive the planner can compose.

| Tool | Returns | Citation prefix |
|---|---|---|
| `read_consensus(ticker, period_kind='quarter', n=4)` | Next N forward fiscal periods + the most recent historical period: revenue / EPS / EBITDA / NI low / avg / high + analyst count + fetched_at. Defaults to `period_kind='quarter'`, returns 4 forward + 1 most-recent past. | `E:security_id:period_kind:period_end` |
| `read_target_gap(ticker, as_of=None)` | Target high / low / median / consensus, current close, gap percentage, fetched_at staleness. `as_of` defaults to latest trading day. | `T:security_id:fetched_at` |
| `read_surprise_history(ticker, n=8)` | Last N quarters: actual / estimated EPS, actual / estimated revenue, surprise %, announcement date. | `S:security_id:announcement_date` |
| `recent_analyst_actions(ticker, days=90)` | Combined list of grade events (`G:` rows) and price-target events (`A:` rows) in window, sorted by date descending. | `G:analyst_grades.id` and `A:analyst_price_targets.id` |

Compose-from-primitives cases the planner handles without dedicated tools:

- "Beat streak last 4 quarters across the universe" → loop `read_surprise_history`
- "Cross-ticker target-gap leaderboard" → loop `read_target_gap`
- "Has anyone moved on AMD this week?" → `recent_analyst_actions(AMD, days=7)`
- "What's NVDA's forward P/E?" → `read_consensus(NVDA, 'annual', 1)` × `read_prices` (latest close + shares outstanding)

## Steward

### Expectations

Add `estimates` vertical to `expectations.py`:

```python
estimates = VerticalExpectations(
    name="estimates",
    description="Forward consensus, price targets, surprises, and analyst events",
    coverage=lambda c: ...,  # active common stock has fresh consensus + target + ≥1 surprise in last 6mo
)
```

### Checks (4)

Daily-refresh observation (probed 2026-04-30: NVDA `lastUpdated=2026-04-30`,
PLTR price-target-news has same-day entries) sets the freshness threshold at
**3 days stale**. That handles weekends without false positives and catches a
stuck ingest within one business cycle.

| Check | Logic | Threshold |
|---|---|---|
| `analyst_estimates_freshness` | Every active common stock security has at least one `analyst_estimates` row with `fetched_at` within last 3 days | 3 days |
| `price_target_consensus_freshness` | Same, on `price_target_consensus.fetched_at` | 3 days |
| `analyst_estimates_orphan` | Every `analyst_estimates.security_id` resolves to an active common stock security | Always strict |
| `earnings_surprise_sanity` | When both `eps_actual` and `eps_estimated` are non-null and `eps_estimated != 0`, `abs((actual - estimated) / estimated) < 2.0` | ±200% catches data errors without flagging real beats / misses |

All four register into the existing post-ingest steward sweep (`registry.py`).
No new automation surface.

Deferred checks (per the cuts table): `n_analysts_floor`,
`consensus_period_coverage`, `analyst_action_freshness` — all need a backfill
to calibrate against.

## Backfill

Volume sanity (probe-derived):

| Table | Backfill rows |
|---|---|
| `analyst_estimates` | ~540 (avg ~30 per ticker × 18) |
| `price_target_consensus` | 18 |
| `earnings_surprises` | ~450 |
| `analyst_grades` | ~5–8k (NVDA 1100, PLTR 183, CRWV 80; full universe will skew toward NVDA-class volumes for AVGO / META / MSFT / TSLA) |
| `analyst_price_targets` | ~5–15k depending on history depth and pagination |

Total: ~25k rows, ~108 base FMP calls plus pagination on
`price-target-news` (call until empty page). One afternoon, not a project.

Order:

1. Migration: `024_estimates.sql` adds the 5 tables.
2. Implement `src/arrow/ingest/fmp/estimates.py` — one ingest function per
   endpoint, raw-response-cached, replace-or-append per the per-table
   semantics above.
3. Backfill: `uv run scripts/ingest_estimates.py [TICKER ...]` — defaults to
   all 18 active common-stock securities. Walks `price-target-news`
   pagination until empty.
4. Add `estimates` vertical to `expectations.py`; register the 4 checks into
   `REGISTRY`.
5. Wire the 4 `/ask` tools into the planner.
6. Update `docs/architecture/system.md` v1 Tables status table; regenerate
   `arrow_db_schema.html` via `scripts/gen_schema_viz.py`.
7. Capture a `triage_session` row when the first steward sweep runs against
   the new vertical (per AGENTS.md § Working Rules).

## Open Items

- **Pagination depth on `price-target-news` for high-volume tickers.** Probe
  used `limit=20&page=0`; build will walk pages until empty. Lock the
  per-page limit at 100 (FMP convention) to keep call count modest.
- **Analyst tool path.** Probe didn't trace where `read_prices` actually
  lives in the agent runtime. Verify before placing
  `src/arrow/agents/tools/estimates.py`.
- **Daily ingest scheduling.** Folds into the same deferred Hetzner / cron
  conversation as prices. v1: manual `uv run scripts/ingest_estimates.py`
  triggered after `ingest_company.py`, or as a separate sweep. When the cron
  lands, estimates join the same nightly run.

## When to Revisit

- **Estimate revisions become a routine question** ("are analysts moving up
  or down on NVDA?") → drop the delete-and-replace, add `fetched_at` to the
  PK on `analyst_estimates` and `price_target_consensus`, and you have
  history. No data migration; just stop deleting.
- **Forward valuation** asks become routine → add `v_forward_valuation`
  view (forward P / E, forward EV / EBITDA, PEG) joining `analyst_estimates`
  to `prices_daily` and `historical_market_cap`. Document in `formulas.md`.
- **Cross-firm sentiment** asks (which firms are bullish vs bearish on a
  name?) → add an `analyst_firms` dimension table; reconcile
  `grading_company` and `analyst_company` text into normalized firm IDs.
- **Universe expansion** to multi-class tickers → estimates anchor to
  `security_id`, so multi-class is a query-layer choice, not a migration.
