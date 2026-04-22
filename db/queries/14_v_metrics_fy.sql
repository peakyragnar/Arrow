-- v_metrics_fy: fiscal-year metrics read straight from the filer's own
-- FY-annual `financial_facts` rows. Each row corresponds 1:1 to a 10-K
-- filing — that's the audit property we want for the dashboard.
--
-- Difference vs v_metrics_cy: v_metrics_cy aggregates 4 calendar quarters
-- and takes calendar Q4 period-ends for stocks. v_metrics_fy reads the
-- filer's reported FY totals directly (period_type = 'annual'). Stocks
-- on annual rows are snapshot at the fiscal year-end.
--
-- Consumed by scripts/dashboard.py for the FY annual columns.
-- v_metrics_cy remains for screener queries that want calendar
-- normalization across filers.

CREATE OR REPLACE VIEW v_metrics_fy AS
SELECT
    ticker,
    company_id,
    fiscal_year,
    fiscal_period_label,
    period_end AS fy_end,

    -- Levels
    revenue                   AS revenue_fy,
    cogs                      AS cogs_fy,
    gross_profit              AS gross_profit_fy,
    operating_income          AS operating_income_fy,
    net_income                AS net_income_fy,
    rd                        AS rd_fy,
    sbc                       AS sbc_fy,
    cfo                       AS cfo_fy,
    capital_expenditures      AS capital_expenditures_fy,
    dna_cf                    AS dna_fy,
    interest_expense          AS interest_expense_fy,
    cash_paid_for_interest    AS cash_paid_for_interest_fy,
    acquisitions              AS acquisitions_fy,
    tax                       AS tax_fy,
    ebt_incl_unusual          AS ebt_incl_unusual_fy,

    -- Margins
    CASE WHEN revenue IS NULL OR revenue = 0 THEN NULL
         ELSE gross_profit / revenue END     AS gross_margin_fy,
    CASE WHEN revenue IS NULL OR revenue = 0 THEN NULL
         ELSE operating_income / revenue END AS operating_margin_fy,
    CASE WHEN revenue IS NULL OR revenue = 0 THEN NULL
         ELSE net_income / revenue END       AS net_margin_fy,
    CASE WHEN revenue IS NULL OR revenue = 0 THEN NULL
         ELSE sbc / revenue END              AS sbc_pct_revenue_fy,

    -- Stocks at FY-end (same period_end as the annual row)
    total_assets              AS total_assets_fy_end,
    total_equity              AS total_equity_fy_end,
    cash_and_equivalents      AS cash_fy_end,
    short_term_investments    AS short_term_investments_fy_end,
    current_portion_lt_debt   AS current_portion_lt_debt_fy_end,
    long_term_debt            AS long_term_debt_fy_end,
    current_portion_leases_operating AS current_portion_leases_operating_fy_end,
    long_term_leases_operating       AS long_term_leases_operating_fy_end,
    accounts_receivable       AS accounts_receivable_fy_end,
    inventory                 AS inventory_fy_end,
    accounts_payable          AS accounts_payable_fy_end,

    -- Net debt at FY-end
    (
        COALESCE(current_portion_lt_debt, 0)
      + COALESCE(long_term_debt, 0)
      + COALESCE(current_portion_leases_operating, 0)
      + COALESCE(long_term_leases_operating, 0)
      - COALESCE(cash_and_equivalents, 0)
      - COALESCE(short_term_investments, 0)
    ) AS net_debt_fy_end,

    total_employees           AS total_employees_fy
FROM v_company_period_wide
WHERE period_type = 'annual';

COMMENT ON VIEW v_metrics_fy IS
    'Fiscal-year metrics sourced from period_type=annual rows. Each row ties to one 10-K. Used by the dashboard''s FY annual columns.';
