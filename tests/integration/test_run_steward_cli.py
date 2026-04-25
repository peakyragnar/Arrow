"""Integration tests for the steward CLI entrypoint.

Invokes the script as a subprocess so we cover argv parsing, stdout
shape, stderr verbose mode, and exit codes — the user-visible contract.

These tests assume:
  - the test DB has migrations applied (handled by tests/conftest.py)
  - apply_views isn't strictly needed for the runner itself, but the
    CLI imports the steward stack which depends on the schema being live
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
import pytest

from arrow.db.connection import get_conn
from arrow.db.migrations import apply as apply_migrations

REPO_ROOT = Path(__file__).resolve().parents[2]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _reset(conn: psycopg.Connection) -> None:
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE;")
        cur.execute("CREATE SCHEMA public;")
    apply_migrations(conn)


def _insert_run(conn: psycopg.Connection, *, counts: dict, ticker_scope: list[str] | None) -> int:
    finished_at = datetime.now(timezone.utc) - timedelta(minutes=5)
    started_at = finished_at - timedelta(minutes=1)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingest_runs (vendor, run_kind, status, ticker_scope,
                                     started_at, finished_at, counts)
            VALUES ('fmp', 'manual', 'succeeded', %s, %s, %s, %s::jsonb)
            RETURNING id;
            """,
            (ticker_scope, started_at, finished_at, json.dumps(counts)),
        )
        return cur.fetchone()[0]


def _run_cli(*args: str) -> subprocess.CompletedProcess:
    """Invoke `uv run scripts/run_steward.py ...` as a subprocess."""
    cmd = ["uv", "run", "scripts/run_steward.py", *args]
    env = os.environ.copy()
    # Tests already repointed DATABASE_URL at TEST_DATABASE_URL (conftest).
    # Forward that to the subprocess.
    return subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_universe_sweep_emits_json_summary() -> None:
    with get_conn() as conn:
        _reset(conn)
        _insert_run(conn, counts={"rows_processed": 0}, ticker_scope=["TEST"])

    cp = _run_cli()
    assert cp.returncode == 0, f"stderr:\n{cp.stderr}"
    payload = json.loads(cp.stdout)
    assert payload["scope"] == {"tickers": None, "verticals": None, "check_names": None}
    assert payload["actor"] == "human:michael"
    assert "zero_row_runs" in payload["checks_run"]
    assert payload["totals"]["new"] >= 1
    per_check = {r["name"]: r for r in payload["per_check"]}
    assert "zero_row_runs" in per_check
    assert per_check["zero_row_runs"]["error"] is None


def test_ticker_scope_passed_through() -> None:
    with get_conn() as conn:
        _reset(conn)
        _insert_run(conn, counts={"rows_processed": 0}, ticker_scope=["PLTR"])
        _insert_run(conn, counts={"rows_processed": 0}, ticker_scope=["MSFT"])

    cp = _run_cli("--ticker", "pltr")
    assert cp.returncode == 0, f"stderr:\n{cp.stderr}"
    payload = json.loads(cp.stdout)
    assert payload["scope"]["tickers"] == ["PLTR"]  # uppercased

    # Only the PLTR finding should land.
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT ticker FROM data_quality_findings "
            "WHERE source_check = 'zero_row_runs' AND status = 'open' "
            "ORDER BY ticker;"
        )
        tickers = [r[0] for r in cur.fetchall()]
    assert tickers == ["PLTR"]


def test_check_filter_runs_only_named_check() -> None:
    cp = _run_cli("--check", "zero_row_runs")
    assert cp.returncode == 0, f"stderr:\n{cp.stderr}"
    payload = json.loads(cp.stdout)
    assert payload["checks_run"] == ["zero_row_runs"]
    assert len(payload["per_check"]) == 1


def test_unknown_check_filter_yields_empty_run() -> None:
    """Unknown check name doesn't crash; just runs nothing."""
    cp = _run_cli("--check", "no_such_check")
    assert cp.returncode == 0, f"stderr:\n{cp.stderr}"
    payload = json.loads(cp.stdout)
    assert payload["checks_run"] == []
    assert payload["per_check"] == []


def test_verbose_mode_streams_findings_to_stderr() -> None:
    with get_conn() as conn:
        _reset(conn)
        _insert_run(conn, counts={"rows_processed": 0}, ticker_scope=["TEST"])

    cp = _run_cli("--verbose")
    assert cp.returncode == 0, f"stderr:\n{cp.stderr}"
    # JSON still on stdout.
    json.loads(cp.stdout)
    # Per-check lines on stderr.
    assert "zero_row_runs" in cp.stderr
    assert "new" in cp.stderr or "re-observed" in cp.stderr
    # New findings list appears for new ones.
    assert "[TEST]" in cp.stderr or "TEST" in cp.stderr


def test_actor_override_recorded_on_state_changes() -> None:
    with get_conn() as conn:
        _reset(conn)
        _insert_run(conn, counts={"rows_processed": 0}, ticker_scope=["TEST"])

    cp = _run_cli("--actor", "human:test_runner")
    assert cp.returncode == 0, f"stderr:\n{cp.stderr}"

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT created_by, history FROM data_quality_findings "
            "WHERE source_check = 'zero_row_runs' ORDER BY id DESC LIMIT 1;"
        )
        created_by, history = cur.fetchone()
    assert created_by == "human:test_runner"
    assert history[0]["actor"] == "human:test_runner"


def test_exit_code_is_zero_when_all_checks_succeed() -> None:
    cp = _run_cli()
    assert cp.returncode == 0


def test_verbose_listing_excludes_stale_findings_from_prior_runs() -> None:
    """The verbose 'New findings this run' listing must reflect what
    THIS sweep created, not all open findings created by the same
    actor over time. Prior implementation filtered by ``created_by``
    only, which would surface findings from earlier sweeps as if
    they were new — a misleading bug. Fixed by tracking new IDs
    per-CheckResult and filtering by ID list.
    """
    with get_conn() as conn:
        _reset(conn)
        # Seed a zero-row run so the steward will produce a finding.
        _insert_run(conn, counts={"rows_processed": 0}, ticker_scope=["OLDA"])

    # First sweep creates a finding under actor 'human:test_runner'.
    cp1 = _run_cli("--actor", "human:test_runner")
    assert cp1.returncode == 0, f"stderr:\n{cp1.stderr}"
    payload1 = json.loads(cp1.stdout)
    assert payload1["totals"]["new"] == 1

    # Insert ANOTHER zero-row run for a different ticker so the next
    # sweep has something genuinely new to surface.
    with get_conn() as conn:
        _insert_run(conn, counts={"rows_processed": 0}, ticker_scope=["NEWB"])

    # Second sweep with the same actor in verbose mode. The verbose
    # 'New findings' section must list ONLY the NEWB finding (created
    # this run), NOT the OLDA finding (created in the prior sweep).
    cp2 = _run_cli("--actor", "human:test_runner", "--verbose")
    assert cp2.returncode == 0, f"stderr:\n{cp2.stderr}"
    payload2 = json.loads(cp2.stdout)
    assert payload2["totals"]["new"] == 1
    assert payload2["totals"]["unchanged"] == 1  # OLDA re-observed

    # Find the "New findings this run:" block in stderr.
    err = cp2.stderr
    assert "New findings this run" in err, f"verbose mode missing new-findings block:\n{err}"
    new_block = err.split("New findings this run:", 1)[1]
    # NEWB must appear in the new-findings listing (created this run).
    assert "NEWB" in new_block, (
        f"NEWB (created this run) missing from new-findings listing:\n{new_block}"
    )
    # OLDA must NOT appear in the new-findings listing — it existed
    # before this run. The prior buggy implementation would have
    # included it because created_by matched.
    assert "OLDA" not in new_block, (
        f"OLDA (created in prior run) leaked into new-findings listing — "
        f"the verbose-mode stale-finding bug is back:\n{new_block}"
    )


def test_default_actor_uses_user_env() -> None:
    """The CLI should not bake an operator-specific name into the
    default actor. Default reads $USER with 'human:cli' fallback."""
    cp = _run_cli()  # no --actor; default kicks in
    assert cp.returncode == 0
    payload = json.loads(cp.stdout)
    expected_user = os.environ.get("USER", "").strip()
    expected = f"human:{expected_user}" if expected_user else "human:cli"
    assert payload["actor"] == expected, (
        f"default actor leak: got {payload['actor']!r}, expected {expected!r}. "
        f"This catches the previous hardcoded 'human:michael' default."
    )
