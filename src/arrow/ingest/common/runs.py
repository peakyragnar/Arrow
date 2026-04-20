"""ingest_runs lifecycle helpers.

State machine:
  open_run()           -> status='started',   finished_at=NULL
  close_succeeded()    -> status='succeeded', finished_at=now(), counts=...
  close_failed()       -> status='failed',    finished_at=now(), error_*=...
  close_partial()      -> status='partial',   finished_at=now(), counts+error_*

Each transition is its own committed transaction. Decoupling run open/close
from the data-writing transaction means a mid-run crash leaves a row with
status='started' and finished_at=NULL — a detectable orphan for later
sweeping, not lost telemetry.
"""

from __future__ import annotations

import subprocess
from typing import Any

import psycopg
from psycopg.types.json import Jsonb


def _git_sha() -> str | None:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
            timeout=2,
        )
        return out.stdout.strip()
    except Exception:
        return None


def open_run(
    conn: psycopg.Connection,
    *,
    run_kind: str,
    vendor: str,
    ticker_scope: list[str] | None = None,
) -> int:
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ingest_runs (run_kind, vendor, ticker_scope, code_version)
                VALUES (%s, %s, %s, %s)
                RETURNING id;
                """,
                (run_kind, vendor, ticker_scope, _git_sha()),
            )
            run_id = cur.fetchone()[0]
    return run_id


def close_succeeded(
    conn: psycopg.Connection,
    run_id: int,
    *,
    counts: dict[str, Any],
) -> None:
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE ingest_runs
                SET status = 'succeeded', finished_at = now(), counts = %s
                WHERE id = %s;
                """,
                (Jsonb(counts), run_id),
            )


def close_failed(
    conn: psycopg.Connection,
    run_id: int,
    *,
    error_message: str,
    error_details: dict[str, Any] | None = None,
) -> None:
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE ingest_runs
                SET status = 'failed', finished_at = now(),
                    error_message = %s, error_details = %s
                WHERE id = %s;
                """,
                (
                    error_message,
                    Jsonb(error_details) if error_details is not None else None,
                    run_id,
                ),
            )


def close_partial(
    conn: psycopg.Connection,
    run_id: int,
    *,
    counts: dict[str, Any],
    error_message: str,
    error_details: dict[str, Any] | None = None,
) -> None:
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE ingest_runs
                SET status = 'partial', finished_at = now(),
                    counts = %s, error_message = %s, error_details = %s
                WHERE id = %s;
                """,
                (
                    Jsonb(counts),
                    error_message,
                    Jsonb(error_details) if error_details is not None else None,
                    run_id,
                ),
            )
