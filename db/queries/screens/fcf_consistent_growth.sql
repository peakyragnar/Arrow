-- Screen: tickers with positive FCF TTM YoY growth in each of the last
-- 4 TTM windows.
--
-- "FCF TTM" here = CFO TTM − CapEx TTM (with CapEx in cash-impact sign,
-- so CFO + CapEx). YoY growth = FCF(t) vs FCF(t − 4 quarters).
-- "Consistent" = last 4 quarterly observations all show positive YoY.

WITH fcf AS (
    SELECT
        ticker,
        company_id,
        period_end,
        (cfo_ttm + capital_expenditures_ttm) AS fcf_ttm
    FROM v_metrics_ttm
    WHERE cfo_ttm IS NOT NULL AND capital_expenditures_ttm IS NOT NULL
),
with_yoy AS (
    SELECT
        ticker,
        period_end,
        fcf_ttm,
        LAG(fcf_ttm, 4) OVER w AS fcf_ttm_prior_year,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY period_end DESC) AS rn
    FROM fcf
    WINDOW w AS (PARTITION BY company_id ORDER BY period_end)
),
recent4 AS (
    SELECT ticker, period_end, fcf_ttm, fcf_ttm_prior_year, rn
    FROM with_yoy
    WHERE rn <= 4
)
SELECT
    ticker,
    MIN(period_end) AS oldest_quarter_in_window,
    MAX(period_end) AS latest_quarter_end,
    ROUND(MAX(fcf_ttm) / 1e9, 2) AS latest_fcf_ttm_bn
FROM recent4
GROUP BY ticker
HAVING
    BOOL_AND(fcf_ttm_prior_year IS NOT NULL)
    AND BOOL_AND(fcf_ttm > fcf_ttm_prior_year)
ORDER BY latest_fcf_ttm_bn DESC;
