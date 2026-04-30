"""prices_gap_detection: missing trading days inside a security's price series.

A "trading day" is inferred from the data: if any other active security has a
price for date D, then D was a trading day. So holidays (when no security
trades) do not count as gaps. This avoids hardcoding the NYSE calendar; the
universe self-defines its trading days.

Anchor: gaps are only counted from a security's MIN(date) onwards. A new
listing or recent IPO doesn't count as missing the years before it existed.

Severity: warning. A real gap usually means an ingest run failed silently
(another zero_row_runs cousin) or the universe is in an awkward partial state
mid-backfill.

Fingerprint: per (security_id) — one finding per affected security, listing
gap dates in evidence. Re-runs auto-resolve once the gap is filled.
"""

from __future__ import annotations

from typing import Iterable

import psycopg

from arrow.steward.fingerprint import fingerprint
from arrow.steward.registry import Check, FindingDraft, Scope, register


@register
class PricesGapDetection(Check):
    name = "prices_gap_detection"
    severity = "warning"
    vertical = "prices"

    def run(self, conn: psycopg.Connection, *, scope: Scope) -> Iterable[FindingDraft]:
        sql = """
        WITH active AS (
            SELECT id, ticker, company_id, kind
            FROM securities
            WHERE status = 'active'
        ),
        trading_days AS (
            SELECT DISTINCT pd.date
            FROM prices_daily pd
            JOIN active s ON s.id = pd.security_id
        ),
        per_security_window AS (
            SELECT s.id AS security_id, s.ticker, s.company_id,
                   MIN(pd.date) AS first_date,
                   MAX(pd.date) AS last_date
            FROM active s
            JOIN prices_daily pd ON pd.security_id = s.id
            GROUP BY s.id, s.ticker, s.company_id
        ),
        expected AS (
            SELECT psw.security_id, psw.ticker, psw.company_id, td.date
            FROM per_security_window psw
            JOIN trading_days td
              ON td.date >= psw.first_date AND td.date <= psw.last_date
        ),
        missing AS (
            SELECT e.security_id, e.ticker, e.company_id, e.date
            FROM expected e
            LEFT JOIN prices_daily pd
              ON pd.security_id = e.security_id AND pd.date = e.date
            WHERE pd.security_id IS NULL
        )
        SELECT security_id, ticker, company_id,
               COUNT(*)         AS gap_count,
               MIN(date)        AS first_gap,
               MAX(date)        AS last_gap,
               array_agg(date ORDER BY date) FILTER (WHERE rn <= 20) AS sample_gaps
        FROM (
            SELECT m.*,
                   row_number() OVER (PARTITION BY security_id ORDER BY date) AS rn
            FROM missing m
        ) ranked
        GROUP BY security_id, ticker, company_id
        ORDER BY gap_count DESC, ticker;
        """

        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

        for security_id, ticker, company_id, gap_count, first_gap, last_gap, sample_gaps in rows:
            if scope.tickers is not None and ticker.upper() not in {t.upper() for t in scope.tickers}:
                continue

            fp = fingerprint(
                self.name,
                scope={"security_id": security_id},
                rule_params={},
            )
            yield FindingDraft(
                fingerprint=fp,
                finding_type=self.name,
                severity=self.severity,
                company_id=company_id,
                ticker=ticker,
                vertical=self.vertical,
                fiscal_period_key=None,
                evidence={
                    "security_id": security_id,
                    "gap_count": int(gap_count),
                    "first_gap": first_gap.isoformat() if first_gap else None,
                    "last_gap": last_gap.isoformat() if last_gap else None,
                    "sample_gaps": [d.isoformat() for d in (sample_gaps or [])],
                },
                summary=(
                    f"{ticker}: {gap_count} missing trading day(s) in prices "
                    f"between {first_gap} and {last_gap} (universe had prices on those dates)."
                ),
                suggested_action={
                    "kind": "reingest_prices",
                    "params": {"ticker": ticker, "security_id": security_id},
                    "command": f"uv run scripts/ingest_prices.py {ticker}",
                    "prose": (
                        f"Other active securities have prices on these dates but {ticker} "
                        f"does not. Most likely cause: a prior ingest run failed mid-stream "
                        f"or returned a partial window. Re-run the ingest for this ticker. "
                        f"If the gap is real (delisting, trading halt, IPO date alignment), "
                        f"suppress with a clear reason."
                    ),
                },
            )
