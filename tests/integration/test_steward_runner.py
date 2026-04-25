"""Integration tests for the steward runner + first concrete check.

Covers:
  - registry registration (zero_row_runs is present)
  - end-to-end: seed a zero-row succeeded run → run steward → finding appears
  - idempotency: re-run, no duplicate, finding's last_seen_at bumped
  - auto-resolve: simulate the run getting fixed → re-run → finding closes
  - suppression: suppress the finding → re-run → no new open finding
  - scope: ticker-scoped run only resolves findings within that ticker
  - non-zero-row run: not flagged
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from arrow.db.connection import get_conn
from arrow.db.migrations import apply as apply_migrations
from arrow.steward.actions import suppress_finding
from arrow.steward.registry import REGISTRY, Scope
from arrow.steward.runner import run_steward

# Importing the checks package self-registers each check.
import arrow.steward.checks  # noqa: F401


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _reset(conn: psycopg.Connection) -> None:
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE;")
        cur.execute("CREATE SCHEMA public;")
    apply_migrations(conn)


def _insert_run(
    conn: psycopg.Connection,
    *,
    vendor: str = "fmp",
    run_kind: str = "manual",
    status: str = "succeeded",
    ticker_scope: list[str] | None = None,
    counts: dict | None = None,
    finished_minutes_ago: int = 5,
) -> int:
    finished_at = datetime.now(timezone.utc) - timedelta(minutes=finished_minutes_ago)
    started_at = finished_at - timedelta(minutes=1)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingest_runs (
                vendor, run_kind, status, ticker_scope,
                started_at, finished_at, counts
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
            RETURNING id;
            """,
            (
                vendor, run_kind, status, ticker_scope,
                started_at, finished_at,
                _json(counts or {}),
            ),
        )
        return cur.fetchone()[0]


def _json(d: dict) -> str:
    import json
    return json.dumps(d)


def _open_count(conn: psycopg.Connection, *, source_check: str | None = None) -> int:
    with conn.cursor() as cur:
        if source_check:
            cur.execute(
                "SELECT COUNT(*) FROM data_quality_findings "
                "WHERE status = 'open' AND source_check = %s;",
                (source_check,),
            )
        else:
            cur.execute("SELECT COUNT(*) FROM data_quality_findings WHERE status = 'open';")
        return cur.fetchone()[0]


# ---------------------------------------------------------------------------
# Registry sanity
# ---------------------------------------------------------------------------


def test_zero_row_runs_check_registered() -> None:
    names = {c.name for c in REGISTRY}
    assert "zero_row_runs" in names


# ---------------------------------------------------------------------------
# End-to-end: surface a zero-row run
# ---------------------------------------------------------------------------


def test_zero_row_succeeded_run_yields_finding() -> None:
    with get_conn() as conn:
        _reset(conn)
        run_id = _insert_run(
            conn,
            counts={"rows_processed": 0, "raw_responses": 0, "facts_written": 0},
            ticker_scope=["TEST"],
        )

        summary = run_steward(
            conn, scope=Scope.universe(), actor="system:check_runner",
        )
        zero_row = next(r for r in summary.results if r.name == "zero_row_runs")
        assert zero_row.findings_new >= 1
        assert _open_count(conn, source_check="zero_row_runs") >= 1

        with conn.cursor() as cur:
            cur.execute(
                "SELECT ticker, evidence->>'ingest_run_id', summary "
                "FROM data_quality_findings "
                "WHERE source_check = 'zero_row_runs' AND status = 'open' "
                "ORDER BY id DESC LIMIT 1;"
            )
            ticker, evidence_run_id, summary_text = cur.fetchone()
            assert ticker == "TEST"
            assert int(evidence_run_id) == run_id
            assert "succeeded" in summary_text


def test_run_with_meaningful_output_not_flagged() -> None:
    with get_conn() as conn:
        _reset(conn)
        _insert_run(
            conn,
            counts={"rows_processed": 100, "facts_written": 80},
            ticker_scope=["TEST"],
        )

        summary = run_steward(conn, scope=Scope.universe())
        zero_row = next(r for r in summary.results if r.name == "zero_row_runs")
        assert zero_row.findings_new == 0
        assert _open_count(conn, source_check="zero_row_runs") == 0


def test_failed_run_not_flagged() -> None:
    """The check is for SUCCEEDED runs that wrote nothing — failures
    are a different concern (caught by status='failed' indexing)."""
    with get_conn() as conn:
        _reset(conn)
        _insert_run(conn, status="failed", counts={"rows_processed": 0})
        summary = run_steward(conn, scope=Scope.universe())
        zero_row = next(r for r in summary.results if r.name == "zero_row_runs")
        assert zero_row.findings_new == 0


def test_old_run_outside_window_not_flagged() -> None:
    with get_conn() as conn:
        _reset(conn)
        _insert_run(
            conn,
            counts={"rows_processed": 0},
            finished_minutes_ago=60 * 24 * 30,  # 30 days ago
        )
        summary = run_steward(conn, scope=Scope.universe())
        zero_row = next(r for r in summary.results if r.name == "zero_row_runs")
        assert zero_row.findings_new == 0


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


def test_rerun_does_not_duplicate_finding() -> None:
    with get_conn() as conn:
        _reset(conn)
        _insert_run(conn, counts={"rows_processed": 0}, ticker_scope=["TEST"])

        run_steward(conn, scope=Scope.universe())
        before = _open_count(conn, source_check="zero_row_runs")

        summary = run_steward(conn, scope=Scope.universe())
        zero_row = next(r for r in summary.results if r.name == "zero_row_runs")
        # Second run sees same fingerprint, bumps last_seen, doesn't insert.
        assert zero_row.findings_new == 0
        assert zero_row.findings_unchanged >= 1
        after = _open_count(conn, source_check="zero_row_runs")
        assert before == after


# ---------------------------------------------------------------------------
# Auto-resolve
# ---------------------------------------------------------------------------


def test_finding_auto_resolves_when_run_no_longer_zero_row() -> None:
    """Simulate fixing the ingest: bump counts so the next sweep doesn't
    surface this fingerprint, and verify the finding auto-closes."""
    with get_conn() as conn:
        _reset(conn)
        run_id = _insert_run(
            conn, counts={"rows_processed": 0}, ticker_scope=["TEST"],
        )
        run_steward(conn, scope=Scope.universe())
        assert _open_count(conn, source_check="zero_row_runs") == 1

        # Operator re-runs ingest; counts are updated to non-zero.
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE ingest_runs SET counts = '{\"rows_processed\": 50, "
                "\"facts_written\": 50}'::jsonb WHERE id = %s;",
                (run_id,),
            )

        summary = run_steward(conn, scope=Scope.universe())
        zero_row = next(r for r in summary.results if r.name == "zero_row_runs")
        assert zero_row.findings_resolved == 1
        assert _open_count(conn, source_check="zero_row_runs") == 0

        # The closed finding's history shows the auto-resolve actor.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT closed_by, closed_reason, history "
                "FROM data_quality_findings "
                "WHERE source_check = 'zero_row_runs' "
                "ORDER BY id DESC LIMIT 1;"
            )
            closed_by, closed_reason, history = cur.fetchone()
        assert closed_by == "system:check_runner"
        assert closed_reason == "resolved"
        assert any(e["action"] == "closed:resolved" for e in history)


# ---------------------------------------------------------------------------
# Suppression
# ---------------------------------------------------------------------------


def test_suppressed_finding_does_not_reopen_on_next_run() -> None:
    with get_conn() as conn:
        _reset(conn)
        _insert_run(conn, counts={"rows_processed": 0}, ticker_scope=["TEST"])

        run_steward(conn, scope=Scope.universe())
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM data_quality_findings "
                "WHERE source_check = 'zero_row_runs' AND status = 'open';"
            )
            fid = cur.fetchone()[0]
        suppress_finding(
            conn, fid, actor="human:michael",
            reason="vendor confirmed no data this period",
            expires=datetime.now(timezone.utc) + timedelta(days=30),
        )
        assert _open_count(conn, source_check="zero_row_runs") == 0

        # Next sweep: the same fingerprint is still surfaced by the check
        # but open_finding respects the active suppression.
        summary = run_steward(conn, scope=Scope.universe())
        zero_row = next(r for r in summary.results if r.name == "zero_row_runs")
        assert zero_row.findings_suppressed == 1
        assert zero_row.findings_new == 0
        assert _open_count(conn, source_check="zero_row_runs") == 0


# ---------------------------------------------------------------------------
# Scope behavior
# ---------------------------------------------------------------------------


def test_ticker_scoped_run_only_resolves_in_scope_findings() -> None:
    """A scope=ticker:PLTR run that finds nothing should NOT auto-resolve
    open findings whose ticker is MSFT — those are out of scope."""
    with get_conn() as conn:
        _reset(conn)
        _insert_run(conn, counts={"rows_processed": 0}, ticker_scope=["PLTR"])
        _insert_run(conn, counts={"rows_processed": 0}, ticker_scope=["MSFT"])

        run_steward(conn, scope=Scope.universe())
        # Two open findings, one per ticker.
        assert _open_count(conn, source_check="zero_row_runs") == 2

        # Now "fix" PLTR's run.
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE ingest_runs SET counts = '{\"facts_written\": 5}'::jsonb "
                "WHERE 'PLTR' = ANY(ticker_scope);"
            )

        # Run scoped only to PLTR. MSFT's finding must NOT be touched.
        summary = run_steward(conn, scope=Scope.for_tickers("PLTR"))
        zero_row = next(r for r in summary.results if r.name == "zero_row_runs")
        assert zero_row.findings_resolved == 1
        assert _open_count(conn, source_check="zero_row_runs") == 1

        with conn.cursor() as cur:
            cur.execute(
                "SELECT ticker FROM data_quality_findings "
                "WHERE source_check = 'zero_row_runs' AND status = 'open';"
            )
            still_open = [r[0] for r in cur.fetchall()]
        assert still_open == ["MSFT"]


# ---------------------------------------------------------------------------
# Summary shape
# ---------------------------------------------------------------------------


def test_run_summary_to_dict_shape() -> None:
    with get_conn() as conn:
        _reset(conn)
        _insert_run(conn, counts={"rows_processed": 0}, ticker_scope=["TEST"])

        summary = run_steward(conn, scope=Scope.universe())
        d = summary.to_dict()
        assert d["scope"] == {"tickers": None, "verticals": None, "check_names": None}
        assert d["actor"] == "system:check_runner"
        assert "zero_row_runs" in d["checks_run"]
        assert d["totals"]["new"] >= 1
        assert d["duration_ms"] >= 0
        assert any(r["name"] == "zero_row_runs" for r in d["per_check"])


def test_check_failure_does_not_abort_run() -> None:
    """If one check raises, others still run; the failing check's result
    captures the error string."""
    with get_conn() as conn:
        _reset(conn)
        _insert_run(conn, counts={"rows_processed": 0}, ticker_scope=["TEST"])

        # Inject a check that raises, alongside the registered ones.
        from arrow.steward.registry import REGISTRY, Check, FindingDraft, Scope as _Scope

        class _Boom(Check):
            name = "_boom"
            severity = "warning"
            vertical = None

            def run(self, conn, *, scope):
                raise RuntimeError("intentional")

        boom = _Boom()
        REGISTRY.append(boom)
        try:
            summary = run_steward(conn, scope=Scope.universe())
        finally:
            REGISTRY.remove(boom)

        boom_result = next(r for r in summary.results if r.name == "_boom")
        assert boom_result.error is not None
        assert "intentional" in boom_result.error
        # Other checks still produced their findings.
        zero_row = next(r for r in summary.results if r.name == "zero_row_runs")
        assert zero_row.findings_new >= 1
