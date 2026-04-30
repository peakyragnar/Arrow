-- v_quarterly_ttm_pit: rolling 4-quarter TTM sums + latest BS values, with
-- asof_date carried through.
--
-- One row per (company_id, period_end). Window: last 4 quarters at or
-- before this row's period_end. Income statement and cash flow concepts
-- are SUMmed; balance sheet concepts pass through unchanged (BS is a
-- point-in-time stock, not a flow).
--
-- Daily consumers (v_valuation_components_daily) find the most recent row
-- here with `asof_date <= price_date` to get TTM-as-known-then.
--
-- quarters_in_window < 4 means partial TTM history (recent IPO,
-- spinoff). Downstream views NULL-out ratios when the window is partial,
-- to avoid misleading "P/E based on 1 quarter annualized" numbers.

CREATE OR REPLACE VIEW v_quarterly_ttm_pit AS
SELECT
    company_id,
    period_end,
    fiscal_period_label                                          AS fiscal_period_label_at_asof,
    asof_date,

    -- TTM flows (NULL-safe SUM via COALESCE)
    SUM(net_income)            OVER w  AS ttm_net_income,
    SUM(revenue)               OVER w  AS ttm_revenue,
    SUM(operating_income)      OVER w  AS ttm_operating_income,
    SUM(dna_cf)                OVER w  AS ttm_dna,
    SUM(cfo)                   OVER w  AS ttm_cfo,
    SUM(capital_expenditures)  OVER w  AS ttm_capex,

    -- TTM EBITDA = Operating Income TTM + D&A TTM (formulas.md § 15 EBITDA derivation rule)
    SUM(operating_income)      OVER w
      + SUM(dna_cf)            OVER w  AS ttm_ebitda,

    -- TTM FCF = CFO TTM - |CapEx TTM|
    SUM(cfo)                   OVER w
      - ABS(SUM(capital_expenditures) OVER w) AS ttm_fcf,

    -- Balance sheet snapshots (latest at this row's period_end — pass-through)
    cash_and_equivalents,
    short_term_investments,
    long_term_debt,
    current_portion_lt_debt,
    noncontrolling_interest,

    COUNT(*) OVER w AS quarters_in_window
FROM v_quarterly_components_pit
WINDOW w AS (
    PARTITION BY company_id
    ORDER BY period_end
    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
);

COMMENT ON VIEW v_quarterly_ttm_pit IS
    'Per (company_id, period_end): rolling 4-quarter TTM sums for IS/CF concepts + latest BS values. asof_date is the publication date; downstream daily consumers use it for PIT lookup. quarters_in_window < 4 means partial TTM (consumers should NULL ratios).';
