-- v_metrics_ttm: TTM-grain metrics (one row per (ticker, period_end)
-- where period_end is a quarter end; metrics apply to the trailing 4
-- quarters ending there).
--
-- Covers formulas.md:
--    3  Reinvestment Rate
--    4  Gross Profit TTM (level) — growth is in v_metrics_ttm_yoy
--   5a  Revenue TTM (level)       — growth is in v_metrics_ttm_yoy
--    8  NOPAT Margin
--    9  CFO / NOPAT
--   10  FCF / NOPAT  (FCF = CFO − CapEx, both TTM)
--   11  Accruals Ratio
--   13  SBC as % of Revenue
--   16  Interest Coverage (TTM view)
--   18  Revenue per Employee
--   21  Unlevered FCF
--
-- Metric 3's "Delta R&D Asset" joins rd_asset_q vs its prior-year value
-- (4-quarter lag). Delta Operating Working Capital uses quarter-end
-- stocks vs prior-year stocks.

CREATE OR REPLACE VIEW v_metrics_ttm AS
WITH emp AS (
    -- For each quarterly (company, period_end), carry forward the most
    -- recent employee count where employee_period_end <= period_end.
    SELECT
        q.company_id,
        q.period_end,
        (
            SELECT e.value
            FROM v_ff_current e
            WHERE e.company_id = q.company_id
              AND e.concept = 'total_employees'
              AND e.period_end <= q.period_end
            ORDER BY e.period_end DESC
            LIMIT 1
        ) AS employee_count
    FROM v_company_period_wide q
    WHERE q.period_type = 'quarter'
),
rd_asset_delta AS (
    SELECT
        r.company_id,
        r.period_end,
        r.rd_asset_q,
        LAG(r.rd_asset_q, 4) OVER (PARTITION BY r.company_id ORDER BY r.period_end) AS rd_asset_prior_year
    FROM v_rd_derived r
),
wc_delta AS (
    SELECT
        w.company_id,
        w.period_end,
        w.accounts_receivable,
        w.inventory,
        w.accounts_payable,
        LAG(w.accounts_receivable, 4) OVER pw AS ar_prior_year,
        LAG(w.inventory,           4) OVER pw AS inv_prior_year,
        LAG(w.accounts_payable,    4) OVER pw AS ap_prior_year
    FROM v_company_period_wide w
    WHERE w.period_type = 'quarter'
    WINDOW pw AS (PARTITION BY w.company_id ORDER BY w.period_end)
)
SELECT
    t.ticker,
    t.company_id,
    t.period_end,

    -- ===== Levels =====
    t.revenue_ttm,
    t.gross_profit_ttm,
    t.operating_income_ttm,
    t.net_income_ttm,
    t.cfo_ttm,
    t.capital_expenditures_ttm,
    t.dna_ttm,
    t.sbc_ttm,
    n.adjusted_oi_ttm,
    n.adjusted_nopat_ttm,
    n.tax_rate_ttm,

    -- ===== 8 NOPAT Margin =====
    CASE
        WHEN t.revenue_ttm IS NULL OR t.revenue_ttm = 0 THEN NULL
        WHEN n.adjusted_nopat_ttm IS NULL THEN NULL
        ELSE n.adjusted_nopat_ttm / t.revenue_ttm
    END AS nopat_margin,

    -- ===== 9 CFO / NOPAT =====
    CASE
        WHEN n.adjusted_nopat_ttm IS NULL OR n.adjusted_nopat_ttm = 0 THEN NULL
        WHEN t.cfo_ttm IS NULL THEN NULL
        ELSE t.cfo_ttm / n.adjusted_nopat_ttm
    END AS cfo_to_nopat,

    -- ===== 10 FCF / NOPAT (FCF = CFO − CapEx). CapEx cash-impact sign is
    -- negative, so FCF = cfo + capital_expenditures (NOT cfo − cx per
    -- formulas.md § 5.4 note).
    CASE
        WHEN n.adjusted_nopat_ttm IS NULL OR n.adjusted_nopat_ttm = 0 THEN NULL
        WHEN t.cfo_ttm IS NULL OR t.capital_expenditures_ttm IS NULL THEN NULL
        ELSE (t.cfo_ttm + t.capital_expenditures_ttm) / n.adjusted_nopat_ttm
    END AS fcf_to_nopat,

    -- ===== 11 Accruals Ratio = (NI_TTM − CFO_TTM) / avg total assets =====
    CASE
        WHEN sa.avg_total_assets IS NULL OR sa.avg_total_assets = 0 THEN NULL
        WHEN t.net_income_ttm IS NULL OR t.cfo_ttm IS NULL THEN NULL
        ELSE (t.net_income_ttm - t.cfo_ttm) / sa.avg_total_assets
    END AS accruals_ratio,

    -- ===== 13 SBC as % Revenue (TTM) =====
    CASE
        WHEN t.revenue_ttm IS NULL OR t.revenue_ttm = 0 THEN NULL
        WHEN t.sbc_ttm IS NULL THEN NULL
        ELSE t.sbc_ttm / t.revenue_ttm
    END AS sbc_pct_revenue,

    -- ===== 16 Interest Coverage (TTM) = OI TTM / Interest Expense TTM =====
    CASE
        WHEN t.interest_expense_ttm IS NULL OR t.interest_expense_ttm = 0 THEN NULL
        WHEN t.operating_income_ttm IS NULL THEN NULL
        ELSE t.operating_income_ttm / t.interest_expense_ttm
    END AS interest_coverage_ttm,

    -- ===== 18 Revenue per Employee =====
    -- Employees are annual-grain only (10-K disclosure); join against
    -- the carried-forward count.
    CASE
        WHEN e.employee_count IS NULL OR e.employee_count = 0 THEN NULL
        WHEN t.revenue_ttm IS NULL THEN NULL
        ELSE t.revenue_ttm / e.employee_count
    END AS revenue_per_employee,
    e.employee_count,

    -- ===== 21 Unlevered FCF = CFO + cash_paid_for_interest × (1 − tax_rate) − CapEx
    -- CapEx is cash-impact sign (negative); subtract its absolute or add
    -- back its signed value. Per formulas.md § 5.4: use cfo + capital_expenditures.
    -- Cash paid for interest is a positive USD magnitude.
    -- If cash_paid_for_interest_ttm is missing (FMP returned all NULLs for the
    -- window), approximate with interest_expense_ttm per formulas.md § 21 note.
    CASE
        WHEN t.cfo_ttm IS NULL OR t.capital_expenditures_ttm IS NULL THEN NULL
        WHEN n.tax_rate_ttm IS NULL THEN NULL
        ELSE t.cfo_ttm
           + COALESCE(t.cash_paid_for_interest_ttm, t.interest_expense_ttm, 0) * (1 - n.tax_rate_ttm)
           + t.capital_expenditures_ttm  -- capex is negative signed; + adds the outflow
    END AS unlevered_fcf_ttm,

    -- ===== 3 Reinvestment Rate =====
    -- Reinvestment = CapEx + ΔOpWC + Acquisitions − D&A + ΔR&D_Asset
    --  (CapEx is negative cash-impact; formulas use absolute CapEx as
    --   outflow magnitude. Keep as positive by flipping the sign below.)
    -- ΔOpWC = ΔAR + ΔInventory − ΔAP  (stock deltas between quarter-ends,
    --   YoY 4-quarter lag)
    -- ΔR&D_Asset = rd_asset_q − rd_asset_q_4_ago
    CASE
        WHEN n.adjusted_nopat_ttm IS NULL OR n.adjusted_nopat_ttm = 0 THEN NULL
        WHEN t.capital_expenditures_ttm IS NULL OR t.dna_ttm IS NULL THEN NULL
        WHEN wc.accounts_receivable IS NULL OR wc.ar_prior_year IS NULL THEN NULL
        WHEN wc.inventory IS NULL OR wc.inv_prior_year IS NULL THEN NULL
        WHEN wc.accounts_payable IS NULL OR wc.ap_prior_year IS NULL THEN NULL
        ELSE (
              (-t.capital_expenditures_ttm)
            + ((wc.accounts_receivable - wc.ar_prior_year)
             + (wc.inventory - wc.inv_prior_year)
             - (wc.accounts_payable - wc.ap_prior_year))
            + COALESCE(-t.acquisitions_ttm, 0)
            - t.dna_ttm
            + (COALESCE(rd.rd_asset_q, 0) - COALESCE(rd.rd_asset_prior_year, 0))
        ) / n.adjusted_nopat_ttm
    END AS reinvestment_rate,

    -- ===== Partial-history signal =====
    n.rd_coverage_quarters,
    t.quarters_in_window
FROM v_ttm_flows t
LEFT JOIN v_adjusted_nopat_ttm n
    ON n.company_id = t.company_id AND n.period_end = t.period_end
LEFT JOIN v_stocks_averaged sa
    ON sa.company_id = t.company_id AND sa.period_end = t.period_end
LEFT JOIN emp e
    ON e.company_id = t.company_id AND e.period_end = t.period_end
LEFT JOIN rd_asset_delta rd
    ON rd.company_id = t.company_id AND rd.period_end = t.period_end
LEFT JOIN wc_delta wc
    ON wc.company_id = t.company_id AND wc.period_end = t.period_end;

COMMENT ON VIEW v_metrics_ttm IS
    'TTM-grain metrics: margins, CFO/FCF ratios, accruals, SBC %, interest coverage, revenue/employee, Unlevered FCF, Reinvestment Rate.';
