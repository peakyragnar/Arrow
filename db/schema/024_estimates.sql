-- analyst_estimates, price_target_consensus, earnings_surprises,
-- analyst_grades, analyst_price_targets: the estimates vertical.
--
-- See docs/architecture/estimates_ingest_plan.md for the full design.
--
-- Why five tables (not one polymorphic):
--   The five FMP endpoints have disjoint shapes. A polymorphic table
--   would be mostly NULL columns. Keeping them physically separate keeps
--   each table's natural key tight and lets the steward checks scope
--   cleanly per data shape.
--
-- Why anchor to securities.id (not companies.id):
--   Estimates are about a tradable instrument. Common stock today, but
--   if/when we add multi-class names (GOOG vs GOOGL), each class can
--   carry its own estimates without a migration. Same pattern as the
--   prices vertical (migration 023).
--
-- Why no historical snapshot of consensus in v1 (deferred):
--   FMP refreshes consensus daily; tracking analyst REVISIONS as their
--   own factor is valuable but not v1. Schema is shaped to allow
--   snapshot history without a migration: add `fetched_at` to the PK
--   on `analyst_estimates` and `price_target_consensus` and stop the
--   delete-and-replace. See estimates_ingest_plan.md § "When to Revisit".


-- 1. ANALYST_ESTIMATES ---------------------------------------------------
--
-- Forward + historical analyst consensus per fiscal period. FMP's
-- /stable/analyst-estimates endpoint returns BOTH forward periods (next
-- few quarters / years) AND frozen historical-consensus snapshots from
-- its archive. We store all rows; past rows pair with `earnings_surprises`
-- for richer surprise context. If FMP revises a row, delete-and-replace
-- takes the latest.

CREATE TABLE analyst_estimates (
    security_id     bigint      NOT NULL REFERENCES securities(id) ON DELETE RESTRICT,
    period_kind     text        NOT NULL,
    period_end      date        NOT NULL,

    -- Revenue, EBITDA, EBIT (operating income), net income, SG&A: low / avg / high
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

    -- Provenance
    fetched_at      timestamptz NOT NULL,
    source_raw_response_id  bigint NOT NULL REFERENCES raw_responses(id) ON DELETE RESTRICT,

    PRIMARY KEY (security_id, period_kind, period_end),
    CONSTRAINT analyst_estimates_period_kind_check
        CHECK (period_kind IN ('annual', 'quarter'))
);

CREATE INDEX analyst_estimates_period_end_idx
    ON analyst_estimates (period_end);

COMMENT ON TABLE analyst_estimates IS
  'Forward + historical analyst consensus per (security, period_kind, period_end). Replace-by-(security, period_kind) on each ingest. Migration to snapshot history is a one-line PK change. See docs/architecture/estimates_ingest_plan.md.';

COMMENT ON COLUMN analyst_estimates.period_kind IS
  'annual | quarter. The endpoint serves both, parameterized via period= query string.';

COMMENT ON COLUMN analyst_estimates.ebit_avg IS
  'Operating income consensus, distinct from EBITDA. EBITDA = EBIT + D&A.';


-- 2. PRICE_TARGET_CONSENSUS ----------------------------------------------
--
-- Snapshot of high / low / median / consensus price target per security.
-- One row per security; replaced on each ingest. FMP's
-- /stable/price-target-consensus does not expose an analyst count.

CREATE TABLE price_target_consensus (
    security_id      bigint      PRIMARY KEY REFERENCES securities(id) ON DELETE RESTRICT,
    target_high      numeric(18,6),
    target_low       numeric(18,6),
    target_median    numeric(18,6),
    target_consensus numeric(18,6),

    fetched_at       timestamptz NOT NULL,
    source_raw_response_id  bigint NOT NULL REFERENCES raw_responses(id) ON DELETE RESTRICT
);

COMMENT ON TABLE price_target_consensus IS
  'Latest analyst consensus price target per security. One row per security; replaced on each ingest. No n_analysts (endpoint does not expose).';


-- 3. EARNINGS_SURPRISES --------------------------------------------------
--
-- Per-quarter historical actuals vs estimates (EPS + revenue). Append on
-- first sight; UPSERT on subsequent ingests because (a) actuals populate
-- after announcement, and (b) FMP nudges `last_updated` on existing rows.

CREATE TABLE earnings_surprises (
    security_id     bigint      NOT NULL REFERENCES securities(id) ON DELETE RESTRICT,
    announcement_date  date     NOT NULL,

    eps_actual         numeric(18,6),
    eps_estimated      numeric(18,6),
    revenue_actual     numeric(28,2),
    revenue_estimated  numeric(28,2),

    -- FMP's lastUpdated, distinct from our ingested_at
    last_updated       date,

    -- Provenance
    source_raw_response_id  bigint NOT NULL REFERENCES raw_responses(id) ON DELETE RESTRICT,
    ingested_at        timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (security_id, announcement_date)
);

CREATE INDEX earnings_surprises_announcement_idx
    ON earnings_surprises (announcement_date);

COMMENT ON TABLE earnings_surprises IS
  'Historical EPS + revenue actual vs estimate per announcement. Natural key (security, announcement_date). UPSERT on re-ingest because actuals populate after announcement and FMP can nudge lastUpdated.';


-- 4. ANALYST_GRADES ------------------------------------------------------
--
-- Event log of rating actions (upgrade / downgrade / maintain). FMP
-- returns full history in one call (no pagination). Append-only with
-- natural-key dedup.

CREATE TABLE analyst_grades (
    id              bigserial   PRIMARY KEY,
    security_id     bigint      NOT NULL REFERENCES securities(id) ON DELETE RESTRICT,
    action_date     date        NOT NULL,

    grading_company text        NOT NULL,
    previous_grade  text,
    new_grade       text,
    action          text        NOT NULL,

    source_raw_response_id  bigint NOT NULL REFERENCES raw_responses(id) ON DELETE RESTRICT,
    ingested_at     timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT analyst_grades_action_check
        CHECK (action IN ('upgrade', 'downgrade', 'maintain'))
);

CREATE UNIQUE INDEX analyst_grades_natural_key
    ON analyst_grades (
        security_id, action_date, grading_company,
        COALESCE(previous_grade, ''), COALESCE(new_grade, ''), action
    );
CREATE INDEX analyst_grades_security_date_idx
    ON analyst_grades (security_id, action_date DESC);

COMMENT ON TABLE analyst_grades IS
  'Event log of analyst rating actions from FMP /stable/grades. Append-only; re-ingest dedups on (security, date, firm, prev_grade, new_grade, action).';


-- 5. ANALYST_PRICE_TARGETS -----------------------------------------------
--
-- Event log of individual analyst price-target updates from FMP
-- /stable/price-target-news. Carries full news provenance (analyst name,
-- firm, source URL, price-when-posted) plus split-adjusted target. No
-- previousPriceTarget field in the response — deltas are inferred from
-- the per-firm time series within this table.

CREATE TABLE analyst_price_targets (
    id              bigserial   PRIMARY KEY,
    security_id     bigint      NOT NULL REFERENCES securities(id) ON DELETE RESTRICT,
    published_at    timestamptz NOT NULL,

    analyst_name    text,
    analyst_company text,
    price_target    numeric(18,6),
    adj_price_target numeric(18,6),
    price_when_posted numeric(18,6),

    news_url        text,
    news_title      text,
    news_publisher  text,
    news_base_url   text,

    source_raw_response_id  bigint NOT NULL REFERENCES raw_responses(id) ON DELETE RESTRICT,
    ingested_at     timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX analyst_price_targets_natural_key
    ON analyst_price_targets (
        security_id, published_at,
        COALESCE(analyst_company, ''),
        COALESCE(price_target::text, '')
    );
CREATE INDEX analyst_price_targets_security_date_idx
    ON analyst_price_targets (security_id, published_at DESC);

COMMENT ON TABLE analyst_price_targets IS
  'Event log of individual analyst price-target updates from FMP /stable/price-target-news. adj_price_target parallels prices_daily.adj_close (split-adjusted). previousPriceTarget is not exposed — infer deltas from the per-firm time series.';
