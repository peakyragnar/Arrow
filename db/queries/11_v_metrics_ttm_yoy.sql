-- v_metrics_ttm_yoy: YoY delta metrics (one row per (company_id, period_end)).
--
-- Covers formulas.md:
--    4  Gross Profit TTM Growth
--   5a  Revenue Growth YoY (TTM)
--    6  Incremental Gross Margin = ΔGross Profit TTM / ΔRevenue TTM
--    7  Incremental Operating Margin = ΔOp Income TTM / ΔRevenue TTM
--   14  Diluted Share Count Growth (YoY same-quarter)
--
-- Uses LAG(..., 4) for YoY against the same-quarter-last-year.

CREATE OR REPLACE VIEW v_metrics_ttm_yoy AS
WITH t AS (
    SELECT
        t.ticker,
        t.company_id,
        t.period_end,
        t.revenue_ttm,
        t.gross_profit_ttm,
        t.operating_income_ttm,
        LAG(t.revenue_ttm, 4) OVER w            AS revenue_ttm_prior_year,
        LAG(t.gross_profit_ttm, 4) OVER w       AS gross_profit_ttm_prior_year,
        LAG(t.operating_income_ttm, 4) OVER w   AS operating_income_ttm_prior_year,
        w.shares_diluted_weighted_avg,
        LAG(w.shares_diluted_weighted_avg, 4) OVER pw AS shares_diluted_prior_year
    FROM v_ttm_flows t
    LEFT JOIN v_company_period_wide w
        ON w.company_id = t.company_id AND w.period_end = t.period_end
           AND w.period_type = 'quarter'
    WINDOW w AS (PARTITION BY t.company_id ORDER BY t.period_end),
           pw AS (PARTITION BY t.company_id ORDER BY t.period_end)
)
SELECT
    ticker,
    company_id,
    period_end,

    -- ===== 5a Revenue Growth YoY (TTM) =====
    CASE
        WHEN revenue_ttm_prior_year IS NULL OR revenue_ttm_prior_year <= 0 THEN NULL
        WHEN revenue_ttm IS NULL THEN NULL
        ELSE (revenue_ttm - revenue_ttm_prior_year) / revenue_ttm_prior_year
    END AS revenue_yoy_ttm,

    -- ===== 4 Gross Profit TTM Growth =====
    CASE
        WHEN gross_profit_ttm_prior_year IS NULL OR gross_profit_ttm_prior_year <= 0 THEN NULL
        WHEN gross_profit_ttm IS NULL THEN NULL
        ELSE (gross_profit_ttm - gross_profit_ttm_prior_year) / gross_profit_ttm_prior_year
    END AS gross_profit_yoy_ttm,

    -- ===== 6 Incremental Gross Margin = ΔGP TTM / ΔRevenue TTM =====
    CASE
        WHEN revenue_ttm IS NULL OR revenue_ttm_prior_year IS NULL THEN NULL
        WHEN gross_profit_ttm IS NULL OR gross_profit_ttm_prior_year IS NULL THEN NULL
        WHEN (revenue_ttm - revenue_ttm_prior_year) = 0 THEN NULL
        ELSE (gross_profit_ttm - gross_profit_ttm_prior_year)
           / (revenue_ttm - revenue_ttm_prior_year)
    END AS incremental_gross_margin,

    -- ===== 7 Incremental Operating Margin = ΔOI TTM / ΔRevenue TTM =====
    CASE
        WHEN revenue_ttm IS NULL OR revenue_ttm_prior_year IS NULL THEN NULL
        WHEN operating_income_ttm IS NULL OR operating_income_ttm_prior_year IS NULL THEN NULL
        WHEN (revenue_ttm - revenue_ttm_prior_year) = 0 THEN NULL
        ELSE (operating_income_ttm - operating_income_ttm_prior_year)
           / (revenue_ttm - revenue_ttm_prior_year)
    END AS incremental_operating_margin,

    -- ===== 14 Diluted Share Count Growth (YoY same-quarter) =====
    CASE
        WHEN shares_diluted_prior_year IS NULL OR shares_diluted_prior_year = 0 THEN NULL
        WHEN shares_diluted_weighted_avg IS NULL THEN NULL
        ELSE (shares_diluted_weighted_avg - shares_diluted_prior_year) / shares_diluted_prior_year
    END AS diluted_share_count_growth,

    -- debug / lineage
    revenue_ttm,
    revenue_ttm_prior_year,
    gross_profit_ttm,
    gross_profit_ttm_prior_year,
    operating_income_ttm,
    operating_income_ttm_prior_year,
    shares_diluted_weighted_avg,
    shares_diluted_prior_year
FROM t;

COMMENT ON VIEW v_metrics_ttm_yoy IS
    'YoY TTM growth and incremental-margin metrics. Uses LAG(..., 4) for same-quarter-last-year.';
