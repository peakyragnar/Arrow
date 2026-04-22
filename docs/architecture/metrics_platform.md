# Metrics Platform

The layer between stored facts and every analytical surface (dashboard, screener, ad-hoc SQL, future analyst agent). Instantiates the formulas defined in [`../reference/formulas.md`](../reference/formulas.md) as a stack of Postgres views that consumers read with plain SQL.

## Scope

This is not a dashboard doc. The dashboard is one consumer of the platform; the screener is another; the analyst agent will be a third; direct SQL in psql is a fourth. All four read the same views. If a formula moves or a definition changes, one view definition changes and every consumer sees it.

This doc covers:
- what views exist and what they compute
- how the views compose (dependency order)
- partial-history and point-in-time semantics at the view level
- query patterns for consumers

## Scope boundary vs other docs

| Document | Purpose |
|---|---|
| [`../reference/formulas.md`](../reference/formulas.md) | The metric dictionary — formula definitions, grain, filing sources, implementation clarifications. The *what*. |
| [`../reference/concepts.md`](../reference/concepts.md) | Canonical bucket names (revenue, gross_profit, total_assets, …) that formulas reference. |
| `metrics_platform.md` (this doc) | The view stack that computes formulas from stored facts. The *how*. |
| [`./dashboard.md`](./dashboard.md) | One UI surface built on top of the platform. |

## Architectural rule: compute, don't store

Only raw vendor facts live in `financial_facts` — one row per (ticker, concept, period). Metrics are computed on demand by views. This is deliberate:

- **Formula changes ripple instantly.** If ROIC's definition changes, one view updates and every consumer sees the new value. No recompute pass across rows.
- **No staleness.** Views reflect current `financial_facts` state. New ingest rows light up across all metrics at once. Superseded rows drop out automatically via `v_ff_current`.
- **PIT semantics are cheap.** A future `v_ff_asof(asof_date)` view layers in `published_at <= asof_date` filtering; metrics computed on top of it become PIT-correct with no per-metric code change.
- **Scale is fine.** `financial_facts` at current scope is thousands of rows per ticker. Postgres computes every metric for every quarter of every ingested ticker in milliseconds.

We do not materialize metrics into tables. If this ever becomes a performance issue (it won't at the ticker counts Arrow is aimed at), materialized views are a one-line change per metric; the definitions don't move.

## View stack

Ships as `db/queries/*.sql` files. A single apply script (`scripts/apply_views.py`) runs them in dependency order. Views are DROP+CREATE idempotent — re-applying is always safe.

```
financial_facts (long, canonical)
        │
        ▼
┌────────────────────────────────────────────────────────────┐
│ Tier 1 — Base + intermediates                              │
├────────────────────────────────────────────────────────────┤
│ v_ff_current                                                │
│   superseded_at IS NULL. Every view downstream reads this.  │
│                                                              │
│ v_company_period_wide                                       │
│   one row per (ticker, period_end, period_type);            │
│   columns are each canonical bucket (revenue, gross_profit, │
│   total_assets, etc.). Carries fiscal + calendar columns.   │
│                                                              │
│ v_rd_vintage                                                │
│   20-quarter rolling R&D history per (ticker, period_end).  │
│   Partial-history policy: missing-prior quarters = 0.       │
│                                                              │
│ v_rd_derived                                                │
│   R&D Amortization(t), R&D Asset(t), coverage_quarters.     │
│                                                              │
│ v_tax_rate_ttm                                              │
│   tax_expense_ttm / pretax_ttm; 15% fallback if pretax ≤ 0. │
│                                                              │
│ v_ttm_flows                                                 │
│   4-quarter rolling sums for every flow concept             │
│   (revenue, cfo, capex, d&a, sbc, ni, gross_profit, oi,     │
│    interest_expense, cash_paid_for_interest, …).            │
│                                                              │
│ v_stocks_averaged                                           │
│   (current quarter-end + prior quarter-end) / 2 for each    │
│   stock concept (total_assets, equity, debt, AR, AP, inv).  │
│                                                              │
│ v_adjusted_nopat_ttm                                        │
│   (OI + R&D − R&D Amort) × (1 − tax_rate), TTM grain.       │
│                                                              │
│ v_adjusted_ic_q                                             │
│   equity + debt + op-lease − cash − ST-inv + R&D Asset,     │
│   quarter-end.                                              │
└────────────────────────────────────────────────────────────┘
        │
        ▼
┌────────────────────────────────────────────────────────────┐
│ Tier 2 — Metric views (one row per ticker × period)        │
├────────────────────────────────────────────────────────────┤
│ v_metrics_q     — quarter-grain metrics:                    │
│   • 5b  Revenue Growth QoQ Annualized                       │
│   • 12  Cash Conversion Cycle                               │
│   • 15  Net Debt / EBITDA (quarter-end)                     │
│   • 16  Interest Coverage (quarter)                         │
│   • 19  Working Capital Intensity                           │
│   • 20  DSO / DIO / DPO                                     │
│                                                              │
│ v_metrics_ttm   — TTM-grain metrics:                        │
│   • 3   Reinvestment Rate                                   │
│   • 4   Gross Profit TTM (used by others)                   │
│   • 5a  Revenue TTM                                         │
│   • 8   NOPAT Margin                                        │
│   • 9   CFO / NOPAT                                         │
│   • 10  FCF / NOPAT                                         │
│   • 11  Accruals Ratio                                      │
│   • 13  SBC as % Revenue                                    │
│   • 16  Interest Coverage (TTM)                             │
│   • 18  Revenue per Employee                                │
│   • 21  Unlevered FCF                                       │
│                                                              │
│ v_metrics_ttm_yoy   — YoY delta metrics:                    │
│   • 4   Gross Profit TTM Growth                             │
│   • 5a  Revenue Growth YoY                                  │
│   • 6   Incremental Gross Margin                            │
│   • 7   Incremental Operating Margin                        │
│   • 14  Diluted Share Count Growth                          │
│                                                              │
│ v_metrics_roic  — adjusted-capital metrics:                 │
│   • 1   ROIC (adjusted)                                     │
│   • 2   ROIIC                                               │
│   Carries `rd_coverage_quarters` so consumers can filter    │
│   partial-history periods when desired.                     │
└────────────────────────────────────────────────────────────┘
        │
        ▼
┌────────────────────────────────────────────────────────────┐
│ Tier 3 — Presentation                                      │
├────────────────────────────────────────────────────────────┤
│ v_metric_changes                                            │
│   YoY %, QoQ %, and BPS deltas for every metric, via LAG()  │
│   window functions. One row per (ticker, period_end,        │
│   metric_name).                                             │
│                                                              │
│ v_dashboard_panel                                           │
│   wide-row-per-metric for the dashboard UI layout:          │
│   5 CY columns + TTM + 8 rolling fiscal quarters.           │
│   Filtered with WHERE ticker = ? by the dashboard.          │
│   Described in detail in dashboard.md.                      │
└────────────────────────────────────────────────────────────┘
```

Metric 17 (retired per formulas.md). Metric 22 (organic growth) returns NULL — spec says "not fully automatable in v1."

## Partial-history and missing-component semantics

Each metric declares `requires` (inputs) and `on_missing` (NULL-handling) per `formulas.md`. The view layer is the implementation of those rules.

- **Universal rule (formulas.md):** if any required component is NULL for the required period, the metric is NULL. No plugs, no interpolation, no partial values.
- **Documented exception — R&D 20-quarter window (formulas.md):** missing priors are treated as 0 and the metric is still computed, with a `coverage_quarters` field attached so consumers can filter or de-weight early-window values.
- **Point-in-time:** all views read through `v_ff_current` today (superseded_at IS NULL). A future `v_ff_asof(asof_date)` can replace that filter to produce PIT-correct metrics; no metric-level code changes.

## Tax rate and fallback

`v_tax_rate_ttm` computes `tax_expense_ttm / pretax_ttm` and falls back to **15%** when pretax is zero or negative, per `formulas.md` § Tax rate rule. This view is consumed by metric 1 (ROIC), metric 2 (ROIIC), metric 8 (NOPAT Margin), metric 21 (Unlevered FCF). If the fallback rate ever needs to change, it changes in this one view.

## Consumer patterns

### 1. Dashboard (point query)

```sql
SELECT * FROM v_dashboard_panel WHERE ticker = $1;
```

The panel is already shaped for the UI. Render the rows as a table with green/red coloring on the delta columns.

### 2. Screener (cross-company query)

```sql
-- "Which tickers have ROIC rising for the last 4 fiscal quarters?"
WITH ranked AS (
  SELECT ticker, period_end, roic,
         LAG(roic, 1) OVER w AS roic_q1,
         LAG(roic, 2) OVER w AS roic_q2,
         LAG(roic, 3) OVER w AS roic_q3,
         ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY period_end DESC) AS rn
  FROM v_metrics_roic
  WINDOW w AS (PARTITION BY ticker ORDER BY period_end)
)
SELECT ticker FROM ranked
WHERE rn = 1
  AND roic IS NOT NULL AND roic > roic_q1 AND roic_q1 > roic_q2 AND roic_q2 > roic_q3
ORDER BY ticker;
```

Screens live as parameterized SQL files in `db/queries/screens/` and are invoked via `scripts/screen.py`.

### 3. Ad-hoc SQL (analyst in psql)

```sql
-- "Show me NVDA's accruals ratio for the last 8 quarters"
SELECT period_end, accruals_ratio
FROM v_metrics_ttm
WHERE ticker = 'NVDA'
ORDER BY period_end DESC
LIMIT 8;
```

### 4. Future analyst agent

The agent's retrieval tools `get_financial_fact(ticker, concept, period, asof)` and `sql_query(...)` read the same views. `get_metric(ticker, metric_name, period, asof)` becomes a thin wrapper.

## Build sequencing

1. Build Tier 1 views, unit-test against a known ticker.
2. Build Tier 2 views (metrics), spot-check against benchmark values in `docs/benchmarks/golden_eval.xlsx`.
3. Build Tier 3 views (changes, dashboard_panel).
4. Ship dashboard (separate doc). Ship screener.

Each view ships as a `.sql` file under `db/queries/`. Order of creation is captured in `scripts/apply_views.py`.

## What this platform does not do

- **It does not fetch data.** Ingest is the FMP + SEC pipelines documented in `system.md`.
- **It does not decide definitions.** Those are in `formulas.md`.
- **It does not own the UI.** Dashboard and screener are separate surfaces.
- **It does not supersede facts.** Writes to `financial_facts` are ingest-side only; views are read-only.

## Extension protocol

Adding a new metric:

1. Define it in `formulas.md` — name, grain, formula, filing sources, on_missing.
2. If any new canonical bucket is needed, add to `concepts.md` and extend the relevant mapper.
3. Add the metric to `v_metrics_q`, `v_metrics_ttm`, `v_metrics_ttm_yoy`, or `v_metrics_roic` depending on grain.
4. Add the change-row (YoY, QoQ, BPS as appropriate) to `v_metric_changes`.
5. Add the metric row to `v_dashboard_panel` so it shows up in the UI.
6. If it's a screenable metric, drop a new SQL template in `db/queries/screens/`.

No code outside the view stack needs to change for most metric additions.

## Status

- Phase 0 — formula spec tweaks (15% fallback, R&D partial-history rule, metric 18 source): **done** in this commit.
- Phase 1 — mapper audit + employee ingest endpoint + migration 014: **next**.
- Phase 2 — 10-year history backfill for existing tickers: after Phase 1.
- Phase 3 — view stack (this doc's subject): after Phase 2.
- Phase 4 — dashboard (see `dashboard.md`): after Phase 3.
- Phase 5 — screener: after Phase 3 (parallel with Phase 4 possible).
