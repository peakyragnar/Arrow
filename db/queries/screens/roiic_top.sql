-- Screen: top ROIIC at the latest quarter per ticker.
--
-- ROIIC (formulas.md § 2) measures how efficiently a business turns
-- incremental invested capital into incremental NOPAT, YoY. Top values
-- indicate strong marginal capital productivity.

WITH latest AS (
    SELECT
        ticker,
        period_end,
        roic,
        roiic,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY period_end DESC) AS rn
    FROM v_metrics_roic
    WHERE roiic IS NOT NULL
)
SELECT
    ticker,
    period_end AS latest_quarter_end,
    ROUND(roiic * 100, 1) AS roiic_pct,
    ROUND(roic  * 100, 1) AS roic_pct
FROM latest
WHERE rn = 1
ORDER BY roiic DESC;
