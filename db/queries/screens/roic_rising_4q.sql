-- Screen: tickers whose Adjusted ROIC has risen strictly for the last 4
-- fiscal quarters.
--
-- Source: v_metrics_roic. ROIC is TTM numerator / average quarter-end IC.
-- "Last 4" refers to each ticker's most recent 4 fiscal quarters in our
-- database — fiscal cadence per filer, not calendar-aligned across
-- companies.

WITH ranked AS (
    SELECT
        ticker,
        period_end,
        roic,
        LAG(roic, 1) OVER w AS roic_q1,
        LAG(roic, 2) OVER w AS roic_q2,
        LAG(roic, 3) OVER w AS roic_q3,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY period_end DESC) AS rn
    FROM v_metrics_roic
    WINDOW w AS (PARTITION BY ticker ORDER BY period_end)
)
SELECT
    ticker,
    period_end AS latest_quarter_end,
    ROUND(roic * 100, 2)    AS roic_pct,
    ROUND(roic_q1 * 100, 2) AS roic_qm1_pct,
    ROUND(roic_q2 * 100, 2) AS roic_qm2_pct,
    ROUND(roic_q3 * 100, 2) AS roic_qm3_pct
FROM ranked
WHERE rn = 1
  AND roic    IS NOT NULL
  AND roic_q1 IS NOT NULL AND roic > roic_q1
  AND roic_q2 IS NOT NULL AND roic_q1 > roic_q2
  AND roic_q3 IS NOT NULL AND roic_q2 > roic_q3
ORDER BY roic DESC;
