"""analyst_estimates_orphan: every analyst_estimates row anchors to an
active common-stock security.

The FK on `analyst_estimates.security_id REFERENCES securities(id)`
prevents truly dangling rows, but a security row can be marked
status='delisted' or kind != 'common_stock' after the estimates were
loaded. This check surfaces those mismatches so the operator can either
delete the orphan rows or, if the security is legitimately changing
state, suppress with a reason.

Severity: warning. Orphaned estimates aren't actively wrong, but they
will surface in downstream queries and confuse the operator.
"""

from __future__ import annotations

from typing import Iterable

import psycopg

from arrow.steward.fingerprint import fingerprint
from arrow.steward.registry import Check, FindingDraft, Scope, register


@register
class AnalystEstimatesOrphan(Check):
    name = "analyst_estimates_orphan"
    severity = "warning"
    vertical = "estimates"

    def run(self, conn: psycopg.Connection, *, scope: Scope) -> Iterable[FindingDraft]:
        sql = """
        SELECT s.id AS security_id, s.ticker, s.kind, s.status, s.company_id,
               COUNT(ae.*) AS row_count
        FROM securities s
        JOIN analyst_estimates ae ON ae.security_id = s.id
        WHERE s.status != 'active' OR s.kind != 'common_stock'
        GROUP BY s.id, s.ticker, s.kind, s.status, s.company_id
        ORDER BY s.ticker;
        """
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

        for security_id, ticker, kind, status, company_id, row_count in rows:
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
                    "kind": kind,
                    "status": status,
                    "orphan_row_count": int(row_count),
                },
                summary=(
                    f"{ticker}: {row_count} analyst_estimates rows anchor to "
                    f"a security with kind={kind!r} status={status!r}. "
                    f"Estimates require active common stock."
                ),
                suggested_action={
                    "kind": "review_orphan_estimates",
                    "params": {"ticker": ticker, "security_id": security_id},
                    "command": (
                        f"# inspect rows: SELECT * FROM analyst_estimates "
                        f"WHERE security_id = {security_id};"
                    ),
                    "prose": (
                        f"{ticker} has analyst_estimates rows but the security "
                        f"is no longer active common stock. Either delete the "
                        f"orphan rows (if the security was legitimately "
                        f"delisted) or correct the security's status / kind. "
                        f"If this is intentional (e.g. a temporary kind change "
                        f"during a class restructuring), suppress with reason."
                    ),
                },
            )
