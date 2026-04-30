"""price_target_consensus_freshness: every active common-stock security has a
fresh price_target_consensus snapshot.

Why a dedicated check (not just expected_coverage recency on the estimates
vertical): the estimates vertical's `latest` rolls up `analyst_estimates`,
not `price_target_consensus`. The two endpoints fetch in the same ingest
run but in separate transactions — partial failures could let one stay
fresh while the other goes stale. This check fires per-security on
`price_target_consensus.fetched_at`.

Threshold: 3 days. FMP refreshes consensus daily (probe 2026-04-30); 3
days handles weekends without false positives.

Severity: warning. A stale or missing snapshot does not corrupt anything;
it just means /ask answers about price targets will be older than they
should be.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterable

import psycopg

from arrow.steward.fingerprint import fingerprint
from arrow.steward.registry import Check, FindingDraft, Scope, register


MAX_AGE_DAYS = 3


@register
class PriceTargetConsensusFreshness(Check):
    name = "price_target_consensus_freshness"
    severity = "warning"
    vertical = "estimates"

    def run(self, conn: psycopg.Connection, *, scope: Scope) -> Iterable[FindingDraft]:
        cutoff = datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)
        sql = """
        SELECT s.id AS security_id, s.ticker, s.company_id,
               ptc.fetched_at
        FROM securities s
        LEFT JOIN price_target_consensus ptc ON ptc.security_id = s.id
        WHERE s.status = 'active' AND s.kind = 'common_stock'
          AND (ptc.fetched_at IS NULL OR ptc.fetched_at < %s)
        ORDER BY s.ticker;
        """
        with conn.cursor() as cur:
            cur.execute(sql, (cutoff,))
            rows = cur.fetchall()

        now = datetime.now(timezone.utc)
        for security_id, ticker, company_id, fetched_at in rows:
            if scope.tickers is not None and ticker.upper() not in {t.upper() for t in scope.tickers}:
                continue

            fp = fingerprint(
                self.name,
                scope={"security_id": security_id},
                rule_params={"max_age_days": MAX_AGE_DAYS},
            )
            if fetched_at is None:
                summary = (
                    f"{ticker}: no price_target_consensus row. "
                    f"Run ingest_estimates.py to populate."
                )
            else:
                age_days = (now - fetched_at).total_seconds() / 86400.0
                summary = (
                    f"{ticker}: price_target_consensus is {age_days:.1f}d old "
                    f"(> {MAX_AGE_DAYS}d threshold)."
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
                    "fetched_at": fetched_at.isoformat() if fetched_at else None,
                    "max_age_days": MAX_AGE_DAYS,
                },
                summary=summary,
                suggested_action={
                    "kind": "reingest_estimates",
                    "params": {"ticker": ticker},
                    "command": f"uv run scripts/ingest_estimates.py {ticker}",
                    "prose": (
                        f"FMP refreshes price-target consensus daily. A stale "
                        f"snapshot for {ticker} usually means the scheduled "
                        f"ingest hasn't run. Re-run the suggested command. If "
                        f"FMP genuinely has no consensus for this ticker (rare; "
                        f"recent IPO with thin coverage), suppress with reason."
                    ),
                },
            )
