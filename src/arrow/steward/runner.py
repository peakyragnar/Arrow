"""Steward runner: orchestrate check execution and reconcile findings.

The runner is the only place that writes to ``data_quality_findings``.
Checks yield ``FindingDraft`` instances; the runner stamps
``source_check`` with the check's name, persists via
``open_finding`` (idempotent + suppression-respecting), and after
each check completes auto-resolves any open findings that the check
should have produced this run but didn't.

Auto-resolve rule
-----------------
For each check that ran with scope ``s``, the runner queries open
findings WHERE ``source_check = check.name`` AND scope-intersects(s).
Any such finding whose fingerprint did NOT surface this run is
resolved via ``resolve_finding(actor='system:check_runner',
note='cleared by check')``.

Scope intersection (V1):
- If ``scope.tickers`` is set: only resolve findings whose ``ticker``
  is in the set. Cross-cutting findings (ticker IS NULL) are not
  in scope of a ticker-scoped run and are NOT auto-resolved here —
  they'll be picked up by a universe sweep.
- If ``scope.tickers`` is None (universe sweep): resolve any open
  finding of this check.

This keeps "I just re-ingested PLTR" runs from accidentally
auto-resolving cross-cutting infrastructure findings, while still
letting the universe sweep do full reconciliation.
"""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field
from typing import Any

import psycopg

from arrow.steward.actions import (
    StewardActionError,
    open_finding,
    resolve_finding,
)
from arrow.steward.registry import (
    Check,
    FindingDraft,
    Scope,
    select_checks,
)


@dataclass
class CheckResult:
    name: str
    findings_new: int = 0
    findings_unchanged: int = 0
    findings_suppressed: int = 0
    findings_resolved: int = 0
    duration_ms: float = 0.0
    error: str | None = None


@dataclass
class RunSummary:
    scope: Scope
    actor: str
    checks_run: list[str] = field(default_factory=list)
    results: list[CheckResult] = field(default_factory=list)
    findings_new: int = 0
    findings_unchanged: int = 0
    findings_suppressed: int = 0
    findings_resolved: int = 0
    duration_ms: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "scope": {
                "tickers": self.scope.tickers,
                "verticals": self.scope.verticals,
                "check_names": self.scope.check_names,
            },
            "actor": self.actor,
            "checks_run": self.checks_run,
            "totals": {
                "new": self.findings_new,
                "unchanged": self.findings_unchanged,
                "suppressed": self.findings_suppressed,
                "resolved": self.findings_resolved,
            },
            "duration_ms": round(self.duration_ms, 1),
            "per_check": [
                {
                    "name": r.name,
                    "new": r.findings_new,
                    "unchanged": r.findings_unchanged,
                    "suppressed": r.findings_suppressed,
                    "resolved": r.findings_resolved,
                    "duration_ms": round(r.duration_ms, 1),
                    "error": r.error,
                }
                for r in self.results
            ],
        }


def run_steward(
    conn: psycopg.Connection,
    *,
    scope: Scope,
    actor: str = "system:check_runner",
) -> RunSummary:
    """Run all checks selected by ``scope`` against ``conn`` and return
    a summary. Each check runs inside its own savepoint-style scope so
    one failing check does not abort the run.
    """
    overall_start = time.perf_counter()
    summary = RunSummary(scope=scope, actor=actor)

    checks = select_checks(scope)
    for check in checks:
        result = _run_one_check(conn, check=check, scope=scope, actor=actor)
        summary.results.append(result)
        if result.error is None:
            summary.checks_run.append(check.name)
        summary.findings_new += result.findings_new
        summary.findings_unchanged += result.findings_unchanged
        summary.findings_suppressed += result.findings_suppressed
        summary.findings_resolved += result.findings_resolved

    summary.duration_ms = (time.perf_counter() - overall_start) * 1000.0
    return summary


def _run_one_check(
    conn: psycopg.Connection,
    *,
    check: Check,
    scope: Scope,
    actor: str,
) -> CheckResult:
    result = CheckResult(name=check.name)
    start = time.perf_counter()

    try:
        produced_fingerprints: set[str] = set()
        for draft in check.run(conn, scope=scope):
            ref = open_finding(
                conn,
                fingerprint=draft.fingerprint,
                finding_type=draft.finding_type,
                severity=draft.severity,
                company_id=draft.company_id,
                ticker=draft.ticker,
                vertical=draft.vertical,
                fiscal_period_key=draft.fiscal_period_key,
                source_check=check.name,
                evidence=draft.evidence,
                summary=draft.summary,
                suggested_action=draft.suggested_action,
                actor=actor,
            )
            produced_fingerprints.add(draft.fingerprint)
            if ref.outcome == "created":
                result.findings_new += 1
            elif ref.outcome == "re_observed":
                result.findings_unchanged += 1
            elif ref.outcome == "suppressed":
                result.findings_suppressed += 1

        result.findings_resolved = _auto_resolve_cleared(
            conn,
            check_name=check.name,
            scope=scope,
            produced_fingerprints=produced_fingerprints,
            actor=actor,
        )
    except Exception as e:
        result.error = f"{type(e).__name__}: {e}"

    result.duration_ms = (time.perf_counter() - start) * 1000.0
    return result


def _auto_resolve_cleared(
    conn: psycopg.Connection,
    *,
    check_name: str,
    scope: Scope,
    produced_fingerprints: set[str],
    actor: str,
) -> int:
    """Resolve open findings of ``check_name`` that didn't surface this run.

    Scope intersection: if scope.tickers is set, only consider findings
    with ticker in that set. Universe runs consider all open findings of
    the check.
    """
    sql = ["SELECT id, fingerprint FROM data_quality_findings",
           "WHERE status = 'open' AND source_check = %s"]
    params: list[Any] = [check_name]

    if scope.tickers is not None:
        sql.append("AND ticker = ANY(%s)")
        params.append([t.upper() for t in scope.tickers])

    with conn.cursor() as cur:
        cur.execute(" ".join(sql), params)
        rows = cur.fetchall()

    resolved = 0
    for row_id, fp in rows:
        if fp in produced_fingerprints:
            continue
        try:
            resolve_finding(
                conn, row_id, actor=actor,
                note=f"cleared by {check_name} (no longer surfacing)",
            )
            resolved += 1
        except StewardActionError:
            # Race: someone else closed it between our SELECT and resolve.
            # Acceptable; carry on.
            pass
    return resolved
