-- Screen: earnings-quality concern — positive NI TTM but CFO TTM lagging.
--
-- A classic accrual-quality red flag: net income TTM > 0 while CFO TTM
-- is less than 50% of NI TTM, meaning earnings are being driven by
-- accruals rather than converting into cash. Tied directly to the
-- Accruals Ratio in formulas.md § 11.
--
-- Only the most recent TTM window per ticker is considered.

WITH latest AS (
    SELECT
        ticker,
        period_end,
        net_income_ttm,
        cfo_ttm,
        accruals_ratio,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY period_end DESC) AS rn
    FROM v_metrics_ttm
    WHERE net_income_ttm IS NOT NULL AND cfo_ttm IS NOT NULL
)
SELECT
    ticker,
    period_end                                AS latest_quarter_end,
    ROUND(net_income_ttm / 1e9, 2)            AS ni_ttm_bn,
    ROUND(cfo_ttm / 1e9, 2)                   AS cfo_ttm_bn,
    ROUND(cfo_ttm / NULLIF(net_income_ttm,0), 2) AS cfo_to_ni,
    ROUND(accruals_ratio * 100, 1)            AS accruals_ratio_pct
FROM latest
WHERE rn = 1
  AND net_income_ttm > 0
  AND cfo_ttm < 0.5 * net_income_ttm
ORDER BY cfo_ttm / NULLIF(net_income_ttm, 0) ASC;  -- worst first
