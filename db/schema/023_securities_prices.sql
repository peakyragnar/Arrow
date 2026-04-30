-- securities, prices_daily, historical_market_cap: the prices vertical.
--
-- See docs/architecture/prices_ingest_plan.md for the full design.
--
-- Why a separate `securities` table (companies has a primary_security_id):
--   ETFs/indices have no CIK, no financials, no fiscal year. They cannot
--   live in `companies`. Benchmarks (SPY, QQQ) need a non-companies home.
--   The same table accommodates per-class shares (GOOG vs GOOGL) without
--   a future migration when the universe gains a multi-class ticker.
--
-- Why prices and market cap are split tables:
--   FMP exposes them as separate endpoints with different cadences and
--   different raw response shapes. Each table tracks its own raw_response
--   provenance. Market cap is functionally derivable from price × shares,
--   but FMP's daily series captures intra-filing buyback/issuance moves
--   that the (quarterly) shares-outstanding series cannot.
--
-- Why no adj_close-only and no close-only:
--   FMP's stable price endpoints return three variants — split-adjusted,
--   non-split-adjusted (raw as-traded), and dividend-adjusted (split+div
--   total return). We ingest non-split-adjusted as `close` (true historical
--   prices) and dividend-adjusted as `adj_close` (return math). Both are
--   needed: news/context wants raw, performance math wants adjusted.
--
-- Why one row per (security_id, date), no supersession:
--   Prices are not revised. The post-close print stands. If FMP corrects
--   an error, idempotent re-ingest will UPDATE not INSERT — but we won't
--   keep a history of the prior bad value. This is consistent with how
--   FMP exposes the data and matches operator mental model. (Compare to
--   financial_facts where supersession matters because filings restate.)


-- 1. SECURITIES ---------------------------------------------------------

CREATE TABLE securities (
    id              bigserial   PRIMARY KEY,

    -- Link to companies for common stock; NULL for ETFs/indices
    company_id      bigint      REFERENCES companies(id) ON DELETE RESTRICT,

    -- Display / lookup
    ticker          text        NOT NULL,
    kind            text        NOT NULL,
    status          text        NOT NULL DEFAULT 'active',

    -- Bookkeeping
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

-- Active ticker is unique. Delisted rows can collide on ticker (reissues).
CREATE UNIQUE INDEX securities_ticker_active_idx
    ON securities (ticker)
    WHERE status = 'active';

-- Lookup by company (most queries enter via the company)
CREATE INDEX securities_company_idx
    ON securities (company_id)
    WHERE company_id IS NOT NULL;

COMMENT ON TABLE securities IS
  'Tradable instruments: common stock, ETFs, indices. companies.primary_security_id resolves "ticker NVDA" to a security row. ETFs/indices have NULL company_id.';

COMMENT ON COLUMN securities.kind IS
  'common_stock | etf | index. common_stock requires non-NULL company_id; etf/index require NULL.';


-- 2. COMPANIES.PRIMARY_SECURITY_ID -------------------------------------

ALTER TABLE companies
    ADD COLUMN primary_security_id bigint REFERENCES securities(id) ON DELETE SET NULL;

COMMENT ON COLUMN companies.primary_security_id IS
  'Default tradable security for this company. For multi-class issuers (GOOG/GOOGL) the analyst can resolve the other class via securities.company_id. NULL until securities are seeded.';


-- 3. PRICES_DAILY -------------------------------------------------------

CREATE TABLE prices_daily (
    security_id     bigint      NOT NULL REFERENCES securities(id) ON DELETE RESTRICT,
    date            date        NOT NULL,

    -- Raw as-traded prices (from FMP historical-price-eod/non-split-adjusted)
    open            numeric(18,6) NOT NULL,
    high            numeric(18,6) NOT NULL,
    low             numeric(18,6) NOT NULL,
    close           numeric(18,6) NOT NULL,

    -- Split + dividend adjusted close (from FMP historical-price-eod/dividend-adjusted)
    -- Use this for return math; use `close` for "what did the screen show that day".
    adj_close       numeric(18,6) NOT NULL,

    volume          bigint      NOT NULL,

    -- Provenance
    source_raw_response_id  bigint NOT NULL REFERENCES raw_responses(id) ON DELETE RESTRICT,
    ingested_at     timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (security_id, date),
    CONSTRAINT prices_daily_volume_nonneg CHECK (volume >= 0),
    CONSTRAINT prices_daily_close_positive CHECK (close > 0),
    CONSTRAINT prices_daily_adj_close_positive CHECK (adj_close > 0),
    CONSTRAINT prices_daily_high_low_ordered CHECK (high >= low)
);

CREATE INDEX prices_daily_date_idx ON prices_daily (date);

COMMENT ON TABLE prices_daily IS
  'Daily OHLCV per security. close = raw as-traded; adj_close = split + dividend adjusted (total return basis). See docs/architecture/prices_ingest_plan.md.';

COMMENT ON COLUMN prices_daily.close IS
  'Raw as-traded close. For NVDA pre-2024-06-07 this returns four-digit prices (~$1,210). Use for "what did the ticker say that day".';

COMMENT ON COLUMN prices_daily.adj_close IS
  'Split + dividend adjusted close (total-return frame). Use for return calculations: (adj_close[end] / adj_close[start]) - 1 = total return.';


-- 4. HISTORICAL_MARKET_CAP ---------------------------------------------

CREATE TABLE historical_market_cap (
    security_id     bigint      NOT NULL REFERENCES securities(id) ON DELETE RESTRICT,
    date            date        NOT NULL,
    market_cap      numeric(28,2) NOT NULL,

    -- Provenance
    source_raw_response_id  bigint NOT NULL REFERENCES raw_responses(id) ON DELETE RESTRICT,
    ingested_at     timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (security_id, date),
    CONSTRAINT historical_market_cap_nonneg CHECK (market_cap >= 0)
);

CREATE INDEX historical_market_cap_date_idx ON historical_market_cap (date);

COMMENT ON TABLE historical_market_cap IS
  'Daily market cap series per security from FMP historical-market-capitalization. Stored as a fact (not derived from price × shares) to capture intra-filing buyback/issuance moves cleanly.';
