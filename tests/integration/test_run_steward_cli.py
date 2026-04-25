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
