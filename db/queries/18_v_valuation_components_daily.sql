-- v_valuation_components_daily: per (security, trading day), every input
-- needed to compute valuation ratios.
--
-- One row per row in prices_daily for common-stock securities (ETFs are
-- excluded — they have no underlying financials). Joins:
--   - prices_daily for close/adj_close (own row)
--   - historical_market_cap for market_cap on the same date
--   - v_quarterly_ttm_pit (LATERAL) for TTM components KNOWN as of the
--     trading day — i.e. the latest quarterly publication on or before
--     the price date.
--
-- The LATERAL is the PIT plumbing: pulls the latest pre-publication
-- quarterly row, so for any trading day before a fiscal quarter has
-- been filed, that quarter does NOT yet contribute to TTM. This produces
-- "as known then" valuation, not "as known now" hindsight.
--
-- See docs/architecture/prices_ingest_plan.md § Point-in-time methodology
-- for why this matters and what divergence to expect from public sources.

CREATE OR REPLACE VIEW v_valuation_components_daily AS
SELECT
    pd.security_id,
    s.ticker,
    s.company_id,
    pd.date,

    pd.close,
    pd.adj_close,
    hmc.market_cap,

    ttm.fiscal_period_label_at_asof,
    ttm.asof_date           AS components_known_since,
    ttm.quarters_in_window,

    -- TTM flows
    ttm.ttm_net_income,
    ttm.ttm_revenue,
    ttm.ttm_operating_income,
    ttm.ttm_dna,
    ttm.ttm_cfo,
    ttm.ttm_capex,
    ttm.ttm_ebitda,
    ttm.ttm_fcf,

    -- BS components (PIT, used in EV)
    ttm.cash_and_equivalents,
    ttm.short_term_investments,
    ttm.long_term_debt,
    ttm.current_portion_lt_debt,
    ttm.noncontrolling_interest

FROM prices_daily pd
JOIN securities s
  ON s.id = pd.security_id
 AND s.kind = 'common_stock'
LEFT JOIN historical_market_cap hmc
  ON hmc.security_id = pd.security_id
 AND hmc.date = pd.date
LEFT JOIN LATERAL (
    SELECT *
    FROM v_quarterly_ttm_pit q
    WHERE q.company_id = s.company_id
      AND q.asof_date <= pd.date
    ORDER BY q.asof_date DESC, q.period_end DESC
    LIMIT 1
) ttm ON true;

COMMENT ON VIEW v_valuation_components_daily IS
    'Per (security, trading day): close, adj_close, market_cap, plus TTM components KNOWN as of the trading day (LATERAL lookup into v_quarterly_ttm_pit). ETFs/indices excluded — only common_stock has financial backing. PIT-correct on the publication-date axis; restatement-aware backtest is a v2 concern.';
