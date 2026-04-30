"""earnings_surprise_sanity: implausible EPS surprise magnitudes.

When both `eps_actual` and `eps_estimated` are non-null and `eps_estimated`
is non-zero, a surprise of |actual − estimated| / |estimated| > 200% is
almost always a data error (units mismatch, sign flip, period
misalignment) rather than a real beat / miss. This check fires per
(security, announcement_date) so a re-ingest with the corrected value
auto-resolves.

Threshold: 2.0 (i.e. ±200%). Real EPS beats / misses for the universe
sit comfortably under 100%; 200% catches the obvious junk.

We deliberately skip rows where:
  - either side is NULL  (upcoming announcement or pre-IPO history)
  - eps_estimated is exactly 0  (division by zero; no surprise to compute)

Severity: warning. The data may still be usable for the right side of
the comparison; this is an alert to investigate, not a hard block.
"""

from __future__ import annotations

from typing import Iterable

import psycopg

from arrow.steward.fingerprint import fingerprint
from arrow.steward.registry import Check, FindingDraft, Scope, register


# 2.0 = ±200%. Calibrate against the observed distribution if we
# start seeing legitimate surprises in this range.
SURPRISE_THRESHOLD = 2.0


@register
class EarningsSurpriseSanity(Check):
    name = "earnings_surprise_sanity"
    severity = "warning"
    vertical = "estimates"

    def run(self, conn: psycopg.Connection, *, scope: Scope) -> Iterable[FindingDraft]:
        sql = """
        SELECT es.security_id, s.ticker, s.company_id,
               es.announcement_date,
               es.eps_actual, es.eps_estimated,
               ABS((es.eps_actual - es.eps_estimated) / es.eps_estimated) AS surprise_ratio
        FROM earnings_surprises es
        JOIN securities s ON s.id = es.security_id
        WHERE es.eps_actual IS NOT NULL
          AND es.eps_estimated IS NOT NULL
          AND es.eps_estimated <> 0
          AND ABS((es.eps_actual - es.eps_estimated) / es.eps_estimated) > %s
        ORDER BY surprise_ratio DESC;
        """
        with conn.cursor() as cur:
            cur.execute(sql, (SURPRISE_THRESHOLD,))
            rows = cur.fetchall()

        for security_id, ticker, company_id, announcement_date, eps_actual, eps_est, ratio in rows:
            if scope.tickers is not None and ticker.upper() not in {t.upper() for t in scope.tickers}:
                continue

            fp = fingerprint(
                self.name,
                scope={
                    "security_id": security_id,
                    "announcement_date": announcement_date.isoformat(),
                },
                rule_params={"threshold": SURPRISE_THRESHOLD},
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
                    "announcement_date": announcement_date.isoformat(),
                    "eps_actual": str(eps_actual),
                    "eps_estimated": str(eps_est),
                    "surprise_ratio": float(ratio),
                    "threshold": SURPRISE_THRESHOLD,
                },
                summary=(
                    f"{ticker} {announcement_date}: EPS actual {eps_actual} vs "
                    f"estimated {eps_est} = {float(ratio) * 100:.0f}% surprise. "
                    f"Above ±{int(SURPRISE_THRESHOLD * 100)}% threshold — likely "
                    f"a units / sign / period mismatch."
                ),
                suggested_action={
                    "kind": "investigate_earnings_surprise",
                    "params": {
                        "ticker": ticker,
                        "announcement_date": announcement_date.isoformat(),
                    },
                    "command": (
                        f"uv run scripts/ingest_estimates.py {ticker}  "
                        f"# re-fetch; FMP may have corrected"
                    ),
                    "prose": (
                        f"Surprises this large are almost always a data "
                        f"problem. Re-run the ingest to pick up any FMP "
                        f"corrections. If the value persists, compare "
                        f"epsActual against the company's filed press release "
                        f"for the period — the discrepancy usually surfaces a "
                        f"share-count split or a non-GAAP / GAAP mismatch."
                    ),
                },
            )
