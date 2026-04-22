-- v_metrics_q: quarter-grain metrics (one row per (ticker, period_end, period_type='quarter')).
--
-- Covers formulas.md metrics:
--   5b  Revenue Growth QoQ Annualized
--   12  Cash Conversion Cycle (CCC)
--   15  Net Debt, Net Debt / EBITDA
--   16  Interest Coverage (quarter view)
--   19  Working Capital Intensity
--   20  DSO / DIO / DPO
--   plus margin levels used throughout (gross/op/net margins), so the
--   dashboard can show per-quarter values without having to re-derive.

CREATE OR REPLACE VIEW v_metrics_q AS
WITH q AS (
    SELECT
        w.ticker,
        w.company_id,
        w.period_end,
        w.period_type,
        w.fiscal_year,
        w.fiscal_quarter,
        w.fiscal_period_label,
        w.calendar_year,
        w.calendar_quarter,
        w.calendar_period_label,
        w.revenue,
        w.cogs,
        w.gross_profit,
        w.operating_income,
        w.net_income,
        w.interest_expense,
        w.accounts_receivable,
        w.inventory,
        w.accounts_payable,
        w.current_portion_lt_debt,
        w.long_term_debt,
        w.current_portion_leases_operating,
        w.long_term_leases_operating,
        w.cash_and_equivalents,
        w.short_term_investments,
        t.revenue_ttm,
        t.cogs_ttm,
        t.ebitda_ttm,
        LAG(w.revenue) OVER (PARTITION BY w.company_id ORDER BY w.period_end) AS revenue_prior_q
    FROM v_company_period_wide w
    LEFT JOIN v_ttm_flows t ON t.company_id = w.company_id AND t.period_end = w.period_end
    WHERE w.period_type = 'quarter'
)
SELECT
    ticker,
    company_id,
    period_end,
    period_type,
    fiscal_year,
    fiscal_quarter,
    fiscal_period_label,
    calendar_year,
    calendar_quarter,
    calendar_period_label,

    -- ===== Levels (as-reported) =====
    revenue,
    gross_profit,
    operating_income,
    net_income,

    -- ===== Margins (quarter) =====
    CASE WHEN revenue IS NULL OR revenue = 0 THEN NULL ELSE gross_profit / revenue END AS gross_margin,
    CASE WHEN revenue IS NULL OR revenue = 0 THEN NULL ELSE operating_income / revenue END AS operating_margin,
    CASE WHEN revenue IS NULL OR revenue = 0 THEN NULL ELSE net_income / revenue END AS net_margin,

    -- ===== 5b Revenue Growth QoQ Annualized =====
    -- ((R_t / R_{t-1}) ^ 4) − 1. Suppress when prior ≤ 0.
    CASE
        WHEN revenue_prior_q IS NULL OR revenue_prior_q <= 0 THEN NULL
        WHEN revenue IS NULL THEN NULL
        ELSE POWER(revenue / revenue_prior_q, 4.0) - 1
    END AS revenue_qoq_annualized,

    -- ===== 20 DSO / DIO / DPO (quarter-end BS / TTM IS) =====
    CASE
        WHEN revenue_ttm IS NULL OR revenue_ttm = 0 OR accounts_receivable IS NULL THEN NULL
        ELSE accounts_receivable / revenue_ttm * 365
    END AS dso,
    CASE
        WHEN cogs_ttm IS NULL OR cogs_ttm = 0 OR inventory IS NULL THEN NULL
        ELSE inventory / cogs_ttm * 365
    END AS dio,
    CASE
        WHEN cogs_ttm IS NULL OR cogs_ttm = 0 OR accounts_payable IS NULL THEN NULL
        ELSE accounts_payable / cogs_ttm * 365
    END AS dpo,

    -- ===== 12 Cash Conversion Cycle = DSO + DIO − DPO =====
    CASE
        WHEN revenue_ttm IS NULL OR revenue_ttm = 0
          OR cogs_ttm IS NULL OR cogs_ttm = 0
          OR accounts_receivable IS NULL OR inventory IS NULL OR accounts_payable IS NULL
        THEN NULL
        ELSE (accounts_receivable / revenue_ttm * 365)
           + (inventory / cogs_ttm * 365)
           - (accounts_payable / cogs_ttm * 365)
    END AS ccc,

    -- ===== 19 Working Capital Intensity = NWC / Revenue TTM =====
    CASE
        WHEN revenue_ttm IS NULL OR revenue_ttm = 0 THEN NULL
        WHEN accounts_receivable IS NULL OR inventory IS NULL OR accounts_payable IS NULL THEN NULL
        ELSE (accounts_receivable + inventory - accounts_payable) / revenue_ttm
    END AS working_capital_intensity,

    -- ===== 15 Net Debt (quarter-end) =====
    -- ST debt + LT debt + op-lease curr + op-lease non-curr − cash − ST investments
    (
        COALESCE(current_portion_lt_debt, 0)
      + COALESCE(long_term_debt, 0)
      + COALESCE(current_portion_leases_operating, 0)
      + COALESCE(long_term_leases_operating, 0)
      - COALESCE(cash_and_equivalents, 0)
      - COALESCE(short_term_investments, 0)
    ) AS net_debt,

    -- ===== 15 Net Debt / EBITDA TTM =====
    CASE
        WHEN ebitda_ttm IS NULL OR ebitda_ttm = 0 THEN NULL
        ELSE (
            COALESCE(current_portion_lt_debt, 0)
          + COALESCE(long_term_debt, 0)
          + COALESCE(current_portion_leases_operating, 0)
          + COALESCE(long_term_leases_operating, 0)
          - COALESCE(cash_and_equivalents, 0)
          - COALESCE(short_term_investments, 0)
        ) / ebitda_ttm
    END AS net_debt_to_ebitda,

    -- ===== 16 Interest Coverage (quarter) =====
    -- Operating Income / Interest Expense. FMP's interest_expense is
    -- typically reported positive; operating_income can be positive or
    -- negative. Suppress when interest expense is zero or NULL.
    CASE
        WHEN interest_expense IS NULL OR interest_expense = 0 THEN NULL
        WHEN operating_income IS NULL THEN NULL
        ELSE operating_income / interest_expense
    END AS interest_coverage_q
FROM q;

COMMENT ON VIEW v_metrics_q IS
    'Quarter-grain metrics. Margins, QoQ growth, CCC components, WC intensity, net debt, interest coverage.';
