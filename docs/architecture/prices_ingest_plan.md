# Prices Ingest Plan

Status: active plan; pre-implementation. Spike validated 2026-04-30.

This document is the v1 plan to add a prices and valuation layer to Arrow. It
sits next to the existing financials, transcripts, and SEC qualitative
verticals on the same FMP-first + raw-cache + immutable-artifacts substrate.

The plan was scoped through the Elon Loop — the surface area we considered ran
to ~30 components; what survives is ~15. Cuts are deliberate, not oversights;
each deferred item carries the trigger that would bring it back.

## Goal

Answer price-and-valuation questions in `/ask` without leaving Arrow:

- Did NVDA outperform the market this quarter?
- What was its P/E on 2024-08-15?
- How did the stock react the week of Q3 earnings?
- Is it expensive vs its own history? Vs peers?

## What's In v1

| Component | Why it earns its keep |
|---|---|
| `securities` table (companies + ETFs) | ETFs/indices have no CIK/financials and need a non-companies home. Also accommodates future multi-class shares without a future migration. |
| `prices_daily` table | Price/return substrate for everything else. |
| `historical_market_cap` table | Daily-resolution market cap from FMP. Cleaner than price × shares-outstanding (which step-functions on filing dates and complicates split handling). |
| 2 benchmarks (SPY, QQQ) | SPY = broad-market baseline. QQQ = tech-tilted baseline. Cover the 80% case. |
| 4 valuation views (P/E, EV/EBITDA, P/S, FCF yield) | Cover the dominant valuation questions; remain auditable in `formulas.md`. |
| 2 `/ask` planner tools (`read_prices`, `read_valuations`) | Mirrors the existing `read_transcript` / `compare_transcript_mentions` pattern — primitives the planner composes. |
| 2 steward checks (`prices_freshness`, `prices_gap_detection`) | Per the verticals-ship-with-checks rule. |
| 2018 backfill window | Parity with `DEFAULT_SINCE_DATE` for financials. Pre-2018 prices without financials backstop have limited utility. |

## What's Deferred or Cut

| Item | Action | Trigger to revisit |
|---|---|---|
| `corporate_actions` table | Defer | First split or spin-off causes confusion that adj_close didn't handle. |
| `prices_outlier` steward check | Defer | After backfill — calibrate the threshold against observed σ before adding (per the calibrate-thresholds-first rule). |
| `valuation_at_filing_date_sanity` steward check | Cut | Replace with unit tests on the views; a steward check fails too easily on legitimate TTM-window shifts. |
| `marketcap_price_consistency` recurring check | Cut as recurring; run as one-shot post-backfill | Re-add as recurring if ingest plumbing bugs appear. |
| `price_around_event`, `relative_return`, `compare_valuations` `/ask` tools | Defer | If planner consistently fails to compose these from primitives. |
| XLK benchmark | Defer | First "vs sector ETF" question. |
| P/B, EV/Revenue, dividend yield ratios | Defer | First request. |
| Per-class data in `securities` (GOOG vs GOOGL) | Defer (schema accommodates) | Universe gains a multi-class ticker. |
| Ingest of FMP `ratios` / `key-metrics` / `key-metrics-ttm` | Cut | We compute valuation ourselves so `formulas.md` stays canonical. Ingesting FMP-precomputed numbers introduces a second source of truth — same trap the XBRL audit loop exists to prevent. |
| Intraday bars | Cut | Daily EOD is the resolution we need. News-correlation ("did the stock move on this 8-K?") is well-served by next-day close. |

## Schema (3 new tables)

### `securities`

```sql
CREATE TABLE securities (
    id              bigserial PRIMARY KEY,
    company_id      bigint REFERENCES companies(id) ON DELETE RESTRICT,  -- NULL for ETFs/indices
    ticker          text NOT NULL,
    kind            text NOT NULL,    -- 'common_stock' | 'etf' | 'index'
    status          text NOT NULL DEFAULT 'active',
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT securities_kind_check
        CHECK (kind IN ('common_stock', 'etf', 'index')),
    CONSTRAINT securities_status_check
        CHECK (status IN ('active', 'delisted')),
    CONSTRAINT securities_company_for_stock
        CHECK (
            (kind = 'common_stock' AND company_id IS NOT NULL)
         OR (kind IN ('etf', 'index') AND company_id IS NULL)
        )
);
CREATE UNIQUE INDEX securities_ticker_active_idx
    ON securities (ticker)
    WHERE status = 'active';
```

Plus a column on `companies`:
```sql
ALTER TABLE companies
    ADD COLUMN primary_security_id bigint REFERENCES securities(id);
```

`primary_security_id` resolves "when someone says NVDA, mean this security."
For the current 13 tickers it's a 1:1 mapping; the column earns its keep when
the universe gains a multi-class ticker.

Deferred (do not add in v1): `share_class`, `listing_exchange`, `isin`, `cusip`,
`figi`. Add when first needed.

### `prices_daily`

```sql
CREATE TABLE prices_daily (
    security_id     bigint NOT NULL REFERENCES securities(id) ON DELETE RESTRICT,
    date            date NOT NULL,
    open            numeric(18,6) NOT NULL,
    high            numeric(18,6) NOT NULL,
    low             numeric(18,6) NOT NULL,
    close           numeric(18,6) NOT NULL,    -- raw, as-traded (see Endpoint Choice)
    adj_close       numeric(18,6) NOT NULL,    -- split + dividend adjusted (total-return basis)
    volume          bigint NOT NULL,
    source_raw_response_id  bigint NOT NULL REFERENCES raw_responses(id),
    ingested_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (security_id, date)
);
CREATE INDEX prices_daily_date_idx ON prices_daily (date);
```

### `historical_market_cap`

```sql
CREATE TABLE historical_market_cap (
    security_id     bigint NOT NULL REFERENCES securities(id) ON DELETE RESTRICT,
    date            date NOT NULL,
    market_cap      numeric(28,2) NOT NULL,
    source_raw_response_id  bigint NOT NULL REFERENCES raw_responses(id),
    ingested_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (security_id, date)
);
CREATE INDEX historical_market_cap_date_idx ON historical_market_cap (date);
```

Both tables follow the existing pattern: source-tracked via
`raw_responses.id`, immutable rows, deterministic dedup on the natural key.

## FMP Endpoints

The spike (2026-04-30) probed FMP's stable price endpoints and found three
variants — pick deliberately:

| Endpoint | Returns | Adjustments | Use? |
|---|---|---|---|
| `historical-price-eod/full` | open/high/low/close/volume/vwap | Split-adjusted, NOT dividend-adjusted | **No.** Weird middle state. |
| `historical-price-eod/non-split-adjusted` | adjOpen/adjHigh/adjLow/adjClose/volume | Raw as-traded prices (NVDA $1,210 close on 2024-06-06 pre-split) | **Yes — store as `close`.** |
| `historical-price-eod/dividend-adjusted` | adjOpen/adjHigh/adjLow/adjClose/volume | Split + dividend adjusted (total-return basis) | **Yes — store as `adj_close`.** |

Two calls per ticker per backfill. Both raw responses cached under
`data/raw/fmp/historical-price-eod/{variant}/{TICKER}/...` per the
endpoint-mirrored rule.

For market cap:
- `historical-market-capitalization?symbol={TICKER}` → `{symbol, date, marketCap}`. One call per ticker.

## Universe

13 active companies + 2 benchmarks:

- Companies (existing): AMD, AMZN, AVGO, CRWV, GEV, GOOGL, INTC, META, MSFT, NVDA, PLTR, TSLA, VRT
- Benchmarks (new, stored as `securities` with `kind='etf'`, `company_id=NULL`): SPY, QQQ

Bootstrap: a `seed_securities.py` script (or extension to `seed_companies.py`)
inserts a `securities` row per company (kind=common_stock, company_id linked,
ticker copied), populates `companies.primary_security_id`, and inserts SPY/QQQ
ETF rows.

## Valuation Views

All four ratios live in `db/queries/` as regular SQL views (not materialized,
no refresh discipline needed). Views compose from `prices_daily`,
`historical_market_cap`, and `financial_facts` with point-in-time semantics.

### EV definition (canonical for Arrow)

```
EV = market_cap
   + total_debt              -- long_term_debt + current_portion_lt_debt
   + noncontrolling_interest
   + preferred_equity        -- 0 for current universe; default to 0 if missing
   - cash_and_equivalents
   - short_term_investments  -- operator-validated choice; see below
```

The "subtract short-term investments?" choice is a real fork. For tech
companies holding tens of billions in marketable securities (NVDA, MSFT,
GOOGL, META), excluding STI overstates EV materially. Arrow's choice:
**subtract both `cash_and_equivalents` and `short_term_investments`.** They
are functionally cash for these companies. Operator-validated 2026-04-30.

### EBITDA derivation

```
EBITDA = operating_income + dna_cf
```

`operating_income` from `income_statement`, `dna_cf` from `cash_flow`. We do
not pull a separate "EBITDA" line from FMP — derive from primitives so the
formula is auditable.

### FCF derivation

```
FCF = cfo - |capital_expenditures|
```

`capital_expenditures` is stored as a negative number (cash outflow). The
absolute value is correct for the FCF subtraction.

### Point-in-time methodology (important)

"P/E on date X" can mean two different things:

1. **As-known-on-X (Arrow's choice):** uses TTM through the latest filing
   *published* on or before X. Reproduces what an operator could have seen at
   that moment.
2. **As-recomputed-with-hindsight:** uses TTM through the latest fiscal period
   whose end-date ≤ X, regardless of when the filing was actually published.
   What most public data services show. Wrong for backtests because it leaks
   future information.

Arrow chooses (1). The view filter is:
```sql
WHERE published_at <= as_of_date
  AND (superseded_at IS NULL OR superseded_at > as_of_date)
```

**Consequence — visible divergence from public sources for ~30 days
between fiscal period end and filing date.** Spike (2026-04-30) confirmed:
NVDA P/E on 2024-08-15 computes to 70.76 in Arrow vs 53.08 on
stockanalysis.com — both correct under different methodologies. Once we
move to 2024-08-29 (day after Q2 FY25 filing), Arrow's number drops to
54.42, within 2.5% of stockanalysis.com. **Document this clearly in
`formulas.md` so nobody is surprised.**

### Views shipped (2026-04-30)

The original plan named four parallel ratio views. The cleaner shape that
landed is one components view + one ratios view:

| View | Role |
|---|---|
| `v_quarterly_components_pit` (16) | Pivot quarterly facts wide with `asof_date = MAX(published_at)`. Substrate for the TTM view. |
| `v_quarterly_ttm_pit` (17) | Per (company, period_end): rolling 4-quarter TTM sums + latest BS values. Includes derived `ttm_ebitda` and `ttm_fcf`. |
| `v_valuation_components_daily` (18) | Per (security, trading day): close, adj_close, market_cap, plus latest TTM components KNOWN on that date (LATERAL lookup against (17)). PIT plumbing lives here. |
| `v_valuation_ratios_ttm` (19) | Math layer: P/E, P/S, EV/EBITDA, FCF yield + EV. Carries components for transparency. |

PIT mechanics: every price row joins LATERALly to the most recent
`v_quarterly_ttm_pit` row whose `asof_date <= price.date`. Quarters that
hadn't been filed yet on a given day don't contribute to that day's TTM.

Common stock only — ETFs are excluded from `v_valuation_components_daily`
since they have no underlying financials.

## /ask Integration (shipped 2026-04-30)

Two planner tools, mirroring the existing `read_transcript` /
`compare_transcript_mentions` shape:

- `read_prices(ticker, from_date, to_date)` — Daily bars across the window.
  Returns both `close` (raw as-traded) and `adj_close` (split + dividend
  adjusted total-return). Caps at 400 rows. Works for ETFs (SPY, QQQ) too.
  Summary auto-includes the total return for the window. Each bar carries
  its own citation: `P:security_id:YYYY-MM-DD`. Popup endpoint returns the
  full bar plus market_cap snapshot.

- `read_valuation(ticker, as_of=None)` — P/E, P/S, EV/EBITDA, FCF yield + EV
  + underlying TTM components for one (ticker, date). Defaults to the
  latest available trading day. Common stock only. Cite as
  `M:v_valuation_ratios_ttm:security_id:date` (note: this M-citation keys
  on `security_id`, not `company_id` like the other metric views — the
  popup machinery handles the distinction via `_METRIC_VIEW_ENTITY_COLUMN`).

Compose-from-primitives cases the planner handles without dedicated tools:
- "NVDA stock the week of Q3 earnings" → `read_prices(NVDA, day-3, day+3)`
- "NVDA vs SPY YTD" → two `read_prices` calls, normalize to 100, compute diff
- "Cross-company P/E snapshot" → multiple `read_valuation` calls

## Steward

### Expectations

Add `prices` vertical to `expectations.py`:

```python
prices = VerticalExpectations(
    name="prices",
    description="Daily OHLCV + market cap for active securities",
    coverage=lambda c: ...,  # active securities have prices in last N market days
)
```

### Checks

| Check | Logic |
|---|---|
| `prices_freshness` | Every active security has a `prices_daily` row within last 5 trading days. Trading-day calendar inferred from "any active security has a price that day". |
| `prices_gap_detection` | For each active security, no missing trading days in its series (where "trading day" = any date when at least one other active security has a price). |

Both register into the existing post-ingest steward sweep. No new automation
surface.

Deferred (per the cut table above): `prices_outlier`,
`marketcap_price_consistency` recurring, `valuation_at_filing_date_sanity`.

## Backfill

Volume sanity: 8 years × ~252 trading days × 15 securities ≈ 30k rows total.
FMP returns the full window in one call per ticker. Backfill is one afternoon,
not a project.

Order:
1. Migration: add `securities` + `prices_daily` + `historical_market_cap`
   tables; add `companies.primary_security_id` column.
2. Seed: insert `securities` rows for the 13 companies + SPY + QQQ; backfill
   `companies.primary_security_id`.
3. Backfill 2018-01-01 → today for all 15 securities. Two FMP calls per ticker
   (non-split-adjusted + dividend-adjusted prices) + one for market cap.
4. Add valuation views (`v_pe_ttm`, `v_ev_ebitda_ttm`, `v_ps_ttm`,
   `v_fcf_yield_ttm`, `v_ttm_quarterly` helper).
5. Wire `read_prices` and `read_valuations` planner tools into `/ask`.
6. Register `prices_freshness` + `prices_gap_detection` checks; add `prices`
   vertical to `expectations.py`.
7. One-shot post-backfill: run `marketcap_price_consistency` as an ad-hoc
   sanity check (not a recurring steward check).
8. Update `docs/architecture/system.md` v1 Tables status table; regenerate
   `arrow_db_schema.html`. Add entries to `formulas.md` for the four ratios
   with the EV-cash+STI choice and the point-in-time methodology documented.

## Open Items

- **Daily ingest scheduling.** Folded into the deferred Hetzner/cron
  conversation — for v1, manual `uv run scripts/ingest_prices.py [TICKER ...]`
  triggered after `ingest_company.py`. When the cron lands, prices join the
  same nightly sweep.
- **Total return vs price return wording in `/ask`.** Synthesizer prompts
  should be explicit about which one a returned number represents — adj_close
  drives total return, close drives price return. Document in the planner tool
  docstrings, monitor for confusion in real /ask outputs.

## When to Revisit

- **Multi-class universe**: add a multi-class ticker → populate `share_class`
  on existing securities, add the second-class row.
- **Public-source divergence becomes confusing**: if operators repeatedly
  conflate Arrow's PIT P/E with stockanalysis.com's hindsight P/E,
  add a "hindsight" alternate view as an explicit second metric in `formulas.md`.
- **Sector or peer relative-performance asks become routine**: add XLK and
  other sector ETFs.
- **Backtests start running**: this is when point-in-time strictness pays off,
  and when corporate-actions edge cases (spin-offs, large special dividends)
  start mattering — that's the trigger for the deferred `corporate_actions`
  table.
