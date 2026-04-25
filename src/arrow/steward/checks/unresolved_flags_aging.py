"""unresolved_flags_aging: surface inline-validation flags that have sat
unresolved for too long.

`data_quality_flags` is the inline ingest-time soft-validation table
(BS subtotal drift, Q-sum vs FY ties, etc.). It's been there a while
and accumulates rows the operator never gets back to. The steward
makes those visible by finding-ifying any flag still unresolved past
``DEFAULT_THRESHOLD_DAYS``.

One finding per aged flag (fingerprint includes the flag id), so
each rotting flag has exactly one finding in the inbox. Severity
inherits from the flag.

Vertical: ``"financials"`` — flags are always financial-fact-scoped.

Scope: ``scope.tickers`` filters by the flag's ``company_id`` →
``companies.ticker``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

import psycopg

from arrow.steward.fingerprint import fingerprint
from arrow.steward.registry import Check, FindingDraft, Scope, register

DEFAULT_THRESHOLD_DAYS = 14


@register
class UnresolvedFlagsAging(Check):
    name = "unresolved_flags_aging"
    severity = "warning"  # default; per-finding inherits from the flag
    vertical = "financials"

    def run(self, conn: psycopg.Connection, *, scope: Scope) -> Iterable[FindingDraft]:
        sql = [
            "SELECT f.id, f.flag_type, f.severity, f.reason, f.flagged_at,",
            "       f.statement, f.concept, f.fiscal_year, f.fiscal_quarter,",
            "       c.id, c.ticker",
            "FROM data_quality_flags f",
            "JOIN companies c ON c.id = f.company_id",
            "WHERE f.resolved_at IS NULL",
            f"  AND f.flagged_at < now() - interval '{DEFAULT_THRESHOLD_DAYS} days'",
        ]
        params: list = []
        if scope.tickers is not None:
            sql.append("  AND c.ticker = ANY(%s)")
            params.append([t.upper() for t in scope.tickers])
        sql.append("ORDER BY f.flagged_at ASC;")

        with conn.cursor() as cur:
            cur.execute("\n".join(sql), params)
            rows = cur.fetchall()

        for (
            flag_id, flag_type, sev, reason, flagged_at,
            statement, concept, fiscal_year, fiscal_quarter,
            company_id, ticker,
        ) in rows:
            yield self._build_draft(
                flag_id=flag_id,
                flag_type=flag_type,
                severity=sev,
                reason=reason,
                flagged_at=flagged_at,
                statement=statement,
                concept=concept,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                company_id=company_id,
                ticker=ticker,
            )

    def _build_draft(self, *, flag_id, flag_type, severity, reason, flagged_at,
                     statement, concept, fiscal_year, fiscal_quarter,
                     company_id, ticker) -> FindingDraft:
        fp = fingerprint(
            self.name,
            scope={"flag_id": flag_id},
            rule_params={"threshold_days": DEFAULT_THRESHOLD_DAYS},
        )
        period = (
            f"FY{fiscal_year}-Q{fiscal_quarter}" if fiscal_year and fiscal_quarter
            else f"FY{fiscal_year}" if fiscal_year
            else None
        )
        age_days = (
            (datetime.now(timezone.utc) - flagged_at).days
            if flagged_at else None
        )

        summary = (
            f"Flag #{flag_id} ({flag_type}, {severity}) on {ticker} "
            f"{statement}/{concept} {period or 'unscoped'} has been unresolved "
            f"for {age_days} days."
        )
        suggested = {
            "kind": "review_flag",
            "params": {"flag_id": flag_id},
            "command": f"uv run scripts/review_flags.py --show {flag_id}",
            "prose": (
                f"Inline validation flagged this {flag_type} {age_days} days ago "
                f"with reason: {reason!r}. Review via review_flags.py and either "
                f"approve the suggested correction, override with a different "
                f"value, or accept-as-is. Resolving the underlying flag will also "
                f"clear this steward finding on the next sweep."
            ),
        }
        return FindingDraft(
            fingerprint=fp,
            finding_type=self.name,
            severity=severity,
            company_id=company_id,
            ticker=ticker,
            vertical=self.vertical,
            fiscal_period_key=period,
            evidence={
                "flag_id": flag_id,
                "flag_type": flag_type,
                "flag_severity": severity,
                "flagged_at": flagged_at.isoformat() if flagged_at else None,
                "age_days": age_days,
                "statement": statement,
                "concept": concept,
                "fiscal_year": fiscal_year,
                "fiscal_quarter": fiscal_quarter,
                "reason": reason,
            },
            summary=summary,
            suggested_action=suggested,
        )
