# Dashboard — UI Surface

One-page analyst dashboard. Per-ticker view of the core metrics, laid out as two time axes side-by-side: five calendar years + TTM on the left, eight rolling fiscal quarters on the right.

This is a thin surface. All math lives in the view stack (see [`metrics_platform.md`](metrics_platform.md)). Render logic is essentially: read `v_dashboard_panel` for the chosen ticker, format numbers, apply color.

## Purpose

- At-a-glance per-ticker analytical snapshot driven by real `financial_facts` data
- Calendar-normalized annual axis for cross-company comparability
- Fiscal-quarterly axis for the filer's own reporting cadence
- Ticker toggle; dropdown driven by the `companies` table (any seeded ticker appears automatically)
- Color-coded deltas (positive green, negative red)

What it is NOT:
- A screener — that's a separate CLI over the same views. See `metrics_platform.md` § Consumer patterns.
- A charting tool — tables only in v1. Charts are a later feature.
- An analyst-agent surface — that's its own build later; the agent will read the same views.

## Layout

Two adjacent blocks, horizontally scrolling if the viewport is narrow:

```
┌────────────────────────────────────────┬──────────────────────────────────────────────────┐
│ CALENDAR ANNUAL (left)                  │ FISCAL QUARTERLY (right)                          │
│ CY2021  CY2022  CY2023  CY2024  CY2025  │ Q-7  Q-6  Q-5  Q-4  Q-3  Q-2  Q-1  Last Q         │
│ TTM                                     │                                                   │
└────────────────────────────────────────┴──────────────────────────────────────────────────┘
```

Rows (one metric per pair of rows when a delta exists):

```
Revenues                            ← absolute values in $M/$B
  YoY / QoQ %                       ← green/red

Gross Margin                         ← %
  BPS change                         ← green/red, in basis points (integer)

Net Income Margin                    ← %
  BPS change                         ← green/red, bps

Operating Margin                     ← %
  BPS change                         ← bps

CFO / NOPAT                          ← ratio
  Δ bps                              ← bps

FCF / NOPAT                          ← ratio
  Δ bps                              ← bps

Adjusted ROIC                        ← %  (tooltip on partial-history periods)
  Δ bps                              ← bps

Reinvestment Rate                    ← %
  Δ bps                              ← bps

Accruals Ratio                       ← %
  Δ bps                              ← bps

CCC (Cash Conversion Cycle)          ← days
  Δ days                             ← integer

DSO / DIO / DPO                      ← days each
  Δ days                             ← integer each

Net Debt / EBITDA                    ← ratio
  Δ                                  ← absolute

SBC as % Revenue                     ← %
  Δ bps                              ← bps

Diluted Shares Growth                ← %
  Δ bps                              ← bps

Revenue per Employee                 ← $M / employee
  YoY %                              ← green/red

…and the remaining metrics defined in formulas.md
```

## Axis semantics

### Calendar annual (left block)

- Columns are **calendar years** (CY2021 … CY2025), populated from `v_metrics_a` joined on `calendar_year`.
- Calendar year is `period_end`-derived per [`../reference/periods.md`](../reference/periods.md). NVDA's fiscal year ending 2025-01-26 maps to CY2025 Q1; NVDA's FY2025 full-year values DO NOT fall under CY2025 — they straddle CY2024 Q1–Q4 and CY2025 Q1. Per the two-clocks rule, calendar-year totals are built by summing calendar-aligned quarters, not by taking a filer's FY.
- **Implication:** the calendar-annual column is a **calendar-quarter rollup**, not a fiscal-year slot. The view computes each calendar year's flow metrics as `sum of 4 calendar quarters` and stock metrics as `calendar Q4 quarter-end`.

### TTM

- One column to the right of the calendar years.
- Rolls the most recent 4 fiscal quarters for each ticker (so "NVDA TTM" is NVDA's latest 4 fiscal quarters, not a calendar window).
- Flow metrics: sum of last 4 quarterly values. Stock metrics: latest quarter-end.
- TTM is the right-most annual-grain column because it's the freshest per filer.

### Fiscal quarterly (right block)

- Columns are the ticker's **most recent 8 fiscal quarters**, ordered from oldest (Q-7) to newest (Last Q).
- Columns use fiscal labels (e.g. `FY26 Q3`) shown as the header, optionally with the calendar-quarter mapping underneath.
- Stock metrics come from each quarter-end; flow metrics come from the discrete quarter value (never YTD).

## Color rules

- **Positive delta**: green (`#22c55e`) text on neutral background.
- **Negative delta**: red (`#ef4444`) text on neutral background.
- **NULL / insufficient data**: gray placeholder (e.g. `—`), hover tooltip explains why (`"missing component: X at period Y"`, `"insufficient 10-K history"`, or `"partial R&D coverage: 12/20 quarters"`).
- **Zero delta**: neutral text, no color.

## Formatting rules

| metric class | format |
|---|---|
| Revenue, NOPAT, cash flows (absolute dollars) | scale to `$M` below $1B, `$B` above. 1 decimal. |
| Margins, ratios expressed as % | 1 decimal percent. |
| Basis-point deltas | signed integer bps. `+40` / `-20`. |
| Growth % deltas | 1 decimal percent signed. `+8.0%` / `-2.3%`. |
| Days (DSO, DIO, DPO, CCC) | integer, trailing `d` optional. |
| Ratios (ND/EBITDA, CFO/NOPAT) | 2 decimals. |
| Share counts | `M` scale, 1 decimal. |

## Hover tooltips

Each metric cell is hover-sensitive. Tooltip content:

- period_end date, fiscal period label, calendar period label (both clocks)
- the raw numerator and denominator values used
- `extraction_version` of the underlying facts
- the XBRL / FMP source accession where the value originated (from `source_raw_response_id`)
- for ROIC/ROIIC/Reinvestment: `R&D coverage: N/20 quarters` when N < 20
- any `data_quality_flags` attached to the fact (flag type, severity, resolution status, note)

## Technical stack

- **Server:** FastAPI (single `scripts/dashboard.py` entrypoint, `uv run uvicorn scripts.dashboard:app --reload`)
- **Templating:** Jinja2, one `templates/dashboard.html.j2` file. No React, no bundler.
- **Styling:** single hand-rolled CSS file, ~100 lines. No Tailwind, no component library.
- **Local-only:** bind to 127.0.0.1, no auth. Cloud deployment (Hetzner per ADR-0001) is a later concern.
- **Read-only:** dashboard issues `SELECT` only. Never writes.

## Routes

| route | handler | returns |
|---|---|---|
| `GET /` | landing | redirect to the first ticker alphabetically, or a "no tickers seeded" page if `companies` is empty |
| `GET /t/<ticker>` | dashboard | rendered HTML for `v_dashboard_panel WHERE ticker = ?` |
| `GET /t/<ticker>/raw` | JSON | same data, JSON — for debugging / future programmatic access |
| `GET /health` | health | DB ping, latest `ingest_runs` timestamp, row counts |

The ticker dropdown on every page queries `SELECT ticker FROM companies ORDER BY ticker;` at render time — any seeded ticker appears automatically.

## What drives the UI data

One SQL call per render:

```sql
SELECT *
FROM v_dashboard_panel
WHERE ticker = $1
ORDER BY metric_row_order;
```

The panel view is responsible for all shape and math. Dashboard code is essentially:

1. Fetch rows.
2. For each row, map to a template cell with formatting + color rule.
3. Render.

If a metric needs to move, change, or be added: one row in `v_dashboard_panel`. No dashboard code change.

## Partial-history display

R&D-dependent metrics (ROIC, ROIIC, Reinvestment Rate) come from `v_metrics_roic` which carries `rd_coverage_quarters`. The dashboard renders these cells with an amber hue plus a tooltip `"R&D coverage: 12/20 quarters — metric derives from partial amortization window per formulas.md"` when coverage < 20. Values below the full-coverage threshold are still shown; analysts are informed, not filtered.

For metric 18 (Revenue per Employee): cells before the ticker's first ingested 10-K render blank with tooltip `"no 10-K employee disclosure available for this period"`.

## Data quality flags in the UI

Facts underlying a cell may have one or more `data_quality_flags` rows (e.g. CF subtotal-component drift from Layer 1 SOFT). The hover tooltip surfaces:

- a small icon if the cell's period has any unresolved flags
- the flag summaries, their severity, and the resolution status (unresolved / accepted / overridden)

Clicking the icon opens the flag detail (same data as `scripts/review_flags.py --show`).

## Future extensions

Deferred for v1 but designed for:

- Side-by-side ticker compare (two panels in one view)
- Charts on any metric cell (click → sparkline)
- PIT toggle (`?asof=YYYY-MM-DD`) that swaps `v_ff_current` for `v_ff_asof(asof_date)` and re-renders
- Screener inline (type a screen expression, run it, results feed into a ticker list)
- Export to CSV (already trivial via `/t/<ticker>/raw`)

None of these require schema changes or view changes; all are UI-layer additions.

## Status

- Phase 0 (formula spec tweaks): done.
- Phase 1 (mapper audit + employee ingest): next.
- Phase 2 (history backfill): after Phase 1.
- Phase 3 (view stack): after Phase 2.
- **Phase 4 (this doc's subject)**: after Phase 3.
- Phase 5 (screener): parallel with Phase 4.
