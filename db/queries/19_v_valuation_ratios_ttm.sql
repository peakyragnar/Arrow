-- v_valuation_ratios_ttm: P/E, P/S, EV/EBITDA, FCF yield + EV components.
--
-- One row per row in v_valuation_components_daily. Math layer only.
--
-- EV definition (canonical for Arrow, formulas.md § Enterprise Value):
--   EV = market_cap
--      + total_debt              (long_term_debt + current_portion_lt_debt)
--      + noncontrolling_interest
--      - cash_and_equivalents
--      - short_term_investments
--
-- Why subtract short-term investments: tech-heavy filers (NVDA, MSFT,
-- GOOGL, META) hold tens of billions in marketable securities that are
-- functionally cash. Excluding them overstates EV materially.
-- Operator-validated 2026-04-30.
--
-- Ratio NULL-out rules:
--   - P/E NULL when ttm_net_income <= 0 (negative earnings → meaningless P/E)
--   - All ratios NULL when quarters_in_window < 4 (partial TTM history)
--   - All ratios NULL when market_cap IS NULL (price day pre-listing on
--     market cap series)

CREATE OR REPLACE VIEW v_valuation_ratios_ttm AS
SELECT
    security_id,
    ticker,
    company_id,
    date,

    close,
    adj_close,
    market_cap,
    fiscal_period_label_at_asof,
    components_known_since,
    quarters_in_window,

    -- EV
    market_cap
      + COALESCE(long_term_debt, 0)
      + COALESCE(current_portion_lt_debt, 0)
      + COALESCE(noncontrolling_interest, 0)
      - COALESCE(cash_and_equivalents, 0)
      - COALESCE(short_term_investments, 0)
        AS ev,

    -- P/E TTM
    CASE
        WHEN quarters_in_window < 4 THEN NULL
        WHEN market_cap IS NULL OR ttm_net_income IS NULL THEN NULL
        WHEN ttm_net_income <= 0 THEN NULL
        ELSE market_cap / ttm_net_income
    END AS pe_ttm,

    -- P/S TTM
    CASE
        WHEN quarters_in_window < 4 THEN NULL
        WHEN market_cap IS NULL OR ttm_revenue IS NULL OR ttm_revenue = 0 THEN NULL
        ELSE market_cap / ttm_revenue
    END AS ps_ttm,

    -- EV/EBITDA TTM
    CASE
        WHEN quarters_in_window < 4 THEN NULL
        WHEN market_cap IS NULL OR ttm_ebitda IS NULL OR ttm_ebitda <= 0 THEN NULL
        ELSE (market_cap
              + COALESCE(long_term_debt, 0)
              + COALESCE(current_portion_lt_debt, 0)
              + COALESCE(noncontrolling_interest, 0)
              - COALESCE(cash_and_equivalents, 0)
              - COALESCE(short_term_investments, 0)) / ttm_ebitda
    END AS ev_ebitda_ttm,

    -- FCF yield TTM (returned as a fraction; consumers can format as %)
    CASE
        WHEN quarters_in_window < 4 THEN NULL
        WHEN market_cap IS NULL OR market_cap = 0 OR ttm_fcf IS NULL THEN NULL
        ELSE ttm_fcf / market_cap
    END AS fcf_yield_ttm,

    -- Components carried through for evidence/transparency
    ttm_net_income,
    ttm_revenue,
    ttm_operating_income,
    ttm_dna,
    ttm_ebitda,
    ttm_cfo,
    ttm_capex,
    ttm_fcf,
    cash_and_equivalents,
    short_term_investments,
    long_term_debt,
    current_portion_lt_debt,
    noncontrolling_interest

FROM v_valuation_components_daily;

COMMENT ON VIEW v_valuation_ratios_ttm IS
    'P/E, P/S, EV/EBITDA, FCF yield per (security, trading day) with PIT TTM. NULL when quarters_in_window<4 or denominators non-positive. Carries underlying components for evidence/transparency.';
