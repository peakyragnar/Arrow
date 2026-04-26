"""Integration tests for steward action callables.

Covers:
  - open_finding idempotency (re-observe vs new insert)
  - lifecycle transitions (open → closed by reason)
  - history capture on every state change
  - suppression respect (open_finding skips when active suppression)
  - coverage_membership add/remove/set_tier idempotency

Warning: these tests DROP and recreate the public schema in the
configured DATABASE_URL.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from arrow.db.connection import get_conn
from arrow.db.migrations import apply as apply_migrations
from arrow.steward.actions import (
    StewardActionError,
    add_to_coverage,
    close_finding,
    dismiss_finding,
    open_finding,
    remove_from_coverage,
    resolve_finding,
    suppress_finding,
)
from arrow.steward.fingerprint import fingerprint


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _reset(conn: psycopg.Connection) -> None:
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE;")
        cur.execute("CREATE SCHEMA public;")
    apply_migrations(conn)


def _seed_company(conn: psycopg.Connection, *, ticker: str = "TEST", cik: int = 9999999) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO companies (cik, ticker, name, fiscal_year_end_md) "
            "VALUES (%s, %s, %s, %s) RETURNING id;",
            (cik, ticker, f"{ticker} Inc.", "12-31"),
        )
        return cur.fetchone()[0]


def _row(conn: psycopg.Connection, finding_id: int) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status, closed_reason, closed_by, closed_note, "
            "       suppressed_until, history, last_seen_at "
            "FROM data_quality_findings WHERE id = %s;",
            (finding_id,),
        )
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, cur.fetchone()))


# ---------------------------------------------------------------------------
# open_finding
# ---------------------------------------------------------------------------


def test_open_finding_inserts_with_initial_history() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn)
        fp = fingerprint("zero_row_runs", {"ticker": "TEST"}, {})

        ref = open_finding(
            conn,
            fingerprint=fp,
            finding_type="zero_row_runs",
            severity="warning",
            company_id=cid,
            ticker="TEST",
            vertical=None,
            fiscal_period_key=None,
            source_check="arrow.steward.checks.zero_row_runs",
            evidence={"ingest_run_id": 42},
            summary="Zero-row succeeded ingest run for TEST",
            suggested_action={
                "kind": "trigger_reingest",
                "params": {"ticker": "TEST"},
                "command": "uv run scripts/ingest_company.py TEST",
                "prose": "Re-run ingest.",
            },
            actor="system:check_runner",
        )

        assert ref.status == "open"
        assert ref.fingerprint == fp
        row = _row(conn, ref.id)
        assert row["status"] == "open"
        assert row["closed_reason"] is None
        assert len(row["history"]) == 1
        entry = row["history"][0]
        assert entry["actor"] == "system:check_runner"
        assert entry["action"] == "opened"


def test_open_finding_is_idempotent_bumps_last_seen_and_appends_history() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn)
        fp = fingerprint("zero_row_runs", {"ticker": "TEST"}, {})
        kwargs = dict(
            fingerprint=fp,
            finding_type="zero_row_runs",
            severity="warning",
            company_id=cid,
            ticker="TEST",
            vertical=None,
            fiscal_period_key=None,
            source_check="arrow.steward.checks.zero_row_runs",
            evidence={},
            summary="Zero-row succeeded ingest run for TEST",
            suggested_action=None,
            actor="system:check_runner",
        )
        first = open_finding(conn, **kwargs)
        # Wait a tick so last_seen_at can advance.
        with conn.cursor() as cur:
            cur.execute("SELECT pg_sleep(0.05);")
        second = open_finding(conn, **kwargs)

        assert first.id == second.id  # same row, not a duplicate
        row = _row(conn, second.id)
        assert len(row["history"]) == 2
        assert row["history"][1]["action"] == "re_observed"


def test_open_finding_skipped_when_active_suppression() -> None:
    """If a closed-suppressed row with future suppressed_until exists for
    the fingerprint, open_finding returns that row instead of creating a
    new open one. This is what makes 'suppress with reason' actually
    stick across nightly sweeps."""
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn)
        fp = fingerprint("seg_taxonomy", {"ticker": "TEST"}, {})
        first = open_finding(
            conn,
            fingerprint=fp,
            finding_type="seg_taxonomy",
            severity="investigate",
            company_id=cid,
            ticker="TEST",
            vertical="segments",
            fiscal_period_key=None,
            source_check="arrow.steward.checks.seg_taxonomy",
            evidence={},
            summary="Segment 'Other' jumped 4% → 28%",
            suggested_action=None,
            actor="system:check_runner",
        )
        suppress_finding(
            conn,
            first.id,
            actor="human:michael",
            reason="confirmed reorg post-acquisition",
            expires=datetime.now(timezone.utc) + timedelta(days=180),
        )

        # Steward sweeps again — same fingerprint surfaces.
        again = open_finding(
            conn,
            fingerprint=fp,
            finding_type="seg_taxonomy",
            severity="investigate",
            company_id=cid,
            ticker="TEST",
            vertical="segments",
            fiscal_period_key=None,
            source_check="arrow.steward.checks.seg_taxonomy",
            evidence={},
            summary="Segment 'Other' jumped 4% → 28%",
            suggested_action=None,
            actor="system:check_runner",
        )
        assert again.id == first.id
        assert again.status == "closed"
        assert again.closed_reason == "suppressed"


def test_open_finding_creates_new_open_when_suppression_expired() -> None:
    """A suppression with suppressed_until in the past should NOT block
    re-opening the same fingerprint."""
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn)
        fp = fingerprint("seg_taxonomy_expired", {"ticker": "TEST"}, {})
        first = open_finding(
            conn,
            fingerprint=fp,
            finding_type="seg_taxonomy",
            severity="warning",
            company_id=cid,
            ticker="TEST",
            vertical="segments",
            fiscal_period_key=None,
            source_check="arrow.steward.checks.seg_taxonomy",
            evidence={},
            summary="Segment drift",
            suggested_action=None,
            actor="system:check_runner",
        )
        suppress_finding(
            conn,
            first.id,
            actor="human:michael",
            reason="suppressed for 1 day only",
            expires=datetime.now(timezone.utc) - timedelta(days=1),
        )

        again = open_finding(
            conn,
            fingerprint=fp,
            finding_type="seg_taxonomy",
            severity="warning",
            company_id=cid,
            ticker="TEST",
            vertical="segments",
            fiscal_period_key=None,
            source_check="arrow.steward.checks.seg_taxonomy",
            evidence={},
            summary="Segment drift",
            suggested_action=None,
            actor="system:check_runner",
        )
        assert again.id != first.id
        assert again.status == "open"


def test_open_finding_invalid_severity_rejected() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn)
        with pytest.raises(StewardActionError):
            open_finding(
                conn,
                fingerprint="x",
                finding_type="x",
                severity="critical",  # not in enum
                company_id=cid,
                ticker="TEST",
                vertical=None,
                fiscal_period_key=None,
                source_check="x",
                evidence={},
                summary="x",
                suggested_action=None,
                actor="x",
            )


# ---------------------------------------------------------------------------
# close_finding / resolve / suppress / dismiss
# ---------------------------------------------------------------------------


def test_resolve_finding_sets_state_and_history() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn)
        fp = fingerprint("zero_row_runs", {"ticker": "TEST"}, {})
        ref = open_finding(
            conn,
            fingerprint=fp,
            finding_type="zero_row_runs",
            severity="warning",
            company_id=cid,
            ticker="TEST",
            vertical=None,
            fiscal_period_key=None,
            source_check="x",
            evidence={},
            summary="x",
            suggested_action=None,
            actor="system:check_runner",
        )

        closed = resolve_finding(conn, ref.id, actor="human:michael", note="ran reingest")
        assert closed.status == "closed"
        assert closed.closed_reason == "resolved"

        row = _row(conn, ref.id)
        assert row["closed_by"] == "human:michael"
        assert row["closed_note"] == "ran reingest"
        assert row["suppressed_until"] is None
        assert any(e["action"] == "closed:resolved" for e in row["history"])


def test_dismiss_finding_works() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn)
        fp = fingerprint("x", {"ticker": "TEST"}, {})
        ref = open_finding(
            conn,
            fingerprint=fp,
            finding_type="x",
            severity="warning",
            company_id=cid,
            ticker="TEST",
            vertical=None,
            fiscal_period_key=None,
            source_check="x",
            evidence={},
            summary="x",
            suggested_action=None,
            actor="system:check_runner",
        )
        dismissed = dismiss_finding(conn, ref.id, actor="human:michael", note="false positive")
        assert dismissed.closed_reason == "dismissed"


def test_suppress_finding_requires_reason() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn)
        fp = fingerprint("x", {"ticker": "TEST"}, {})
        ref = open_finding(
            conn,
            fingerprint=fp,
            finding_type="x",
            severity="warning",
            company_id=cid,
            ticker="TEST",
            vertical=None,
            fiscal_period_key=None,
            source_check="x",
            evidence={},
            summary="x",
            suggested_action=None,
            actor="system:check_runner",
        )
        with pytest.raises(StewardActionError):
            suppress_finding(conn, ref.id, actor="human:michael", reason="")


def test_close_already_closed_finding_raises() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn)
        fp = fingerprint("x", {"ticker": "TEST"}, {})
        ref = open_finding(
            conn,
            fingerprint=fp,
            finding_type="x",
            severity="warning",
            company_id=cid,
            ticker="TEST",
            vertical=None,
            fiscal_period_key=None,
            source_check="x",
            evidence={},
            summary="x",
            suggested_action=None,
            actor="system:check_runner",
        )
        resolve_finding(conn, ref.id, actor="human:michael")
        with pytest.raises(StewardActionError):
            resolve_finding(conn, ref.id, actor="human:michael")


def test_close_invalid_reason_rejected() -> None:
    with get_conn() as conn:
        _reset(conn)
        with pytest.raises(StewardActionError):
            close_finding(conn, 9999, closed_reason="bogus", actor="human:michael")


def test_close_nonexistent_finding_raises() -> None:
    with get_conn() as conn:
        _reset(conn)
        with pytest.raises(StewardActionError):
            close_finding(conn, 9999, closed_reason="resolved", actor="human:michael")


# ---------------------------------------------------------------------------
# Coverage membership
# ---------------------------------------------------------------------------


def test_add_to_coverage_inserts_then_idempotent() -> None:
    with get_conn() as conn:
        _reset(conn)
        _seed_company(conn, ticker="TEST")

        first = add_to_coverage(conn, ticker="TEST", actor="human:michael")
        assert first.tier == "core"
        # Idempotent: same call returns same row.
        again = add_to_coverage(conn, ticker="TEST", actor="human:michael")
        assert again.id == first.id


def test_add_to_coverage_unseeded_ticker_raises() -> None:
    """Ticker must exist in companies first — coverage_membership is a
    membership claim, not a seeding mechanism."""
    with get_conn() as conn:
        _reset(conn)
        with pytest.raises(StewardActionError):
            add_to_coverage(conn, ticker="UNSEEDED", actor="human:michael")


def test_add_to_coverage_normalizes_ticker_case() -> None:
    with get_conn() as conn:
        _reset(conn)
        _seed_company(conn, ticker="TEST")
        ref = add_to_coverage(conn, ticker="test", actor="human:michael")
        assert ref.ticker == "TEST"


def test_remove_from_coverage_returns_true_then_false() -> None:
    with get_conn() as conn:
        _reset(conn)
        _seed_company(conn, ticker="TEST")
        add_to_coverage(conn, ticker="TEST", actor="human:michael")
        assert remove_from_coverage(conn, ticker="TEST", actor="human:michael") is True
        assert remove_from_coverage(conn, ticker="TEST", actor="human:michael") is False


# ---------------------------------------------------------------------------
# v_open_quality_signals view sanity
# ---------------------------------------------------------------------------


def test_open_finding_concurrent_inserts_no_crash() -> None:
    """Two+ concurrent open_finding calls for the SAME fingerprint must
    not crash with UniqueViolation against the partial unique index.
    Exactly one row should end up open; the other callers should report
    outcome='re_observed'.

    This test would have failed against the previous three-step
    implementation (suppression check + existing-open check + INSERT)
    because callers race between the existing-open check and the INSERT
    and the second INSERT hits the unique constraint. The atomic
    INSERT...ON CONFLICT DO UPDATE fixes it.
    """
    import threading

    with get_conn() as setup_conn:
        _reset(setup_conn)
        cid = _seed_company(setup_conn, ticker="TEST")

    fp = fingerprint("zero_row_runs", {"ticker": "TEST"}, {})
    kwargs = dict(
        fingerprint=fp,
        finding_type="zero_row_runs",
        severity="warning",
        company_id=cid,
        ticker="TEST",
        vertical=None,
        fiscal_period_key=None,
        source_check="test",
        evidence={},
        summary="concurrent insert test",
        suggested_action=None,
        actor="system:check_runner",
    )

    results: list = []
    errors: list = []
    barrier = threading.Barrier(8)

    def caller():
        try:
            with get_conn() as conn:
                # Wait at the barrier so all threads execute the
                # critical section as concurrently as possible.
                barrier.wait()
                results.append(open_finding(conn, **kwargs))
        except Exception as e:  # noqa: BLE001 — we want to surface anything
            errors.append(e)

    threads = [threading.Thread(target=caller) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == [], (
        f"open_finding crashed under concurrency: "
        f"{[type(e).__name__ + ': ' + str(e) for e in errors]}"
    )
    assert len(results) == 8

    # End state: exactly one open row for this fingerprint.
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM data_quality_findings "
            "WHERE fingerprint = %s AND status = 'open';",
            (fp,),
        )
        assert cur.fetchone()[0] == 1

    # Exactly one caller saw outcome='created'; the rest saw 're_observed'.
    n_created = sum(1 for r in results if r.outcome == "created")
    n_re_observed = sum(1 for r in results if r.outcome == "re_observed")
    assert n_created == 1, f"expected exactly one created, got {n_created}"
    assert n_re_observed == 7, f"expected 7 re_observed, got {n_re_observed}"


def test_view_surfaces_open_finding_only() -> None:
    """Open findings appear in v_open_quality_signals; closed ones do not."""
    with get_conn() as conn:
        _reset(conn)
        # apply_views isn't in conftest's per-test reset; reapply manually.
        from scripts.apply_views import main as apply_views_main
        apply_views_main()

        cid = _seed_company(conn, ticker="TEST")
        fp = fingerprint("zero_row_runs", {"ticker": "TEST"}, {})
        ref = open_finding(
            conn,
            fingerprint=fp,
            finding_type="zero_row_runs",
            severity="warning",
            company_id=cid,
            ticker="TEST",
            vertical=None,
            fiscal_period_key=None,
            source_check="x",
            evidence={},
            summary="open finding for view test",
            suggested_action=None,
            actor="system:check_runner",
        )

        with conn.cursor() as cur:
            cur.execute(
                "SELECT source, signal_id, summary FROM v_open_quality_signals "
                "WHERE source = 'finding' AND signal_id = %s;",
                (ref.id,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == "finding"
        assert row[2] == "open finding for view test"

        resolve_finding(conn, ref.id, actor="human:michael")

        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM v_open_quality_signals "
                "WHERE source = 'finding' AND signal_id = %s;",
                (ref.id,),
            )
            assert cur.fetchone() is None
