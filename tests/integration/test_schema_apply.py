"""Integration test for the migration runner.

Warning: these tests DROP and recreate the `public` schema in the
configured DATABASE_URL. Run only against a dev or dedicated test
database — never production.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import psycopg
import pytest

from arrow.db.connection import get_conn
from arrow.db.migrations import MigrationChanged, apply

SCHEMA_DIR = Path(__file__).resolve().parents[2] / "db" / "schema"


def _drop_public_schema(conn: psycopg.Connection) -> None:
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE;")
        cur.execute("CREATE SCHEMA public;")


def _sha256_hex(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def test_migrations_apply_cleanly_from_scratch() -> None:
    with get_conn() as conn:
        _drop_public_schema(conn)
        applied = apply(conn)

        expected = sorted(p.name for p in SCHEMA_DIR.glob("*.sql"))
        assert applied == expected

        with conn.cursor() as cur:
            cur.execute(
                "SELECT filename, checksum FROM schema_migrations ORDER BY filename;"
            )
            rows = cur.fetchall()

        assert [r[0] for r in rows] == expected
        for path, (_, checksum) in zip(sorted(SCHEMA_DIR.glob("*.sql")), rows):
            assert checksum == _sha256_hex(path.read_bytes())


def test_migrations_are_idempotent() -> None:
    with get_conn() as conn:
        _drop_public_schema(conn)
        apply(conn)
        second = apply(conn)
        assert second == []


def test_changed_migration_fails_loudly() -> None:
    with get_conn() as conn:
        _drop_public_schema(conn)
        apply(conn)

        with conn.cursor() as cur:
            cur.execute(
                "UPDATE schema_migrations SET checksum = %s WHERE filename = %s;",
                ("tampered-checksum", "001_extensions.sql"),
            )

        with pytest.raises(MigrationChanged, match="has changed since it was applied"):
            apply(conn)


def test_created_tables_exist() -> None:
    """After applying migrations, both target tables are present with the
    expected columns."""
    with get_conn() as conn:
        _drop_public_schema(conn)
        apply(conn)

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                ORDER BY table_name;
                """
            )
            tables = {row[0] for row in cur.fetchall()}

    assert {
        "ingest_runs",
        "raw_responses",
        "artifacts",
        "artifact_sections",
        "artifact_section_chunks",
        "schema_migrations",
    }.issubset(tables)
    # artifact_chunks created in 005 then dropped in 006
    assert "artifact_chunks" not in tables


def test_raw_responses_body_xor_enforced() -> None:
    """Exactly one of body_jsonb / body_raw must be populated."""
    with get_conn() as conn:
        _drop_public_schema(conn)
        apply(conn)
        # Leave autocommit on (set by _drop_public_schema). Each insert runs in
        # its own transaction so check-constraint failures don't nuke the
        # ingest_run we need to reference.

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ingest_runs (run_kind, vendor, status, finished_at)
                VALUES ('manual', 'test', 'succeeded', now())
                RETURNING id;
                """
            )
            run_id = cur.fetchone()[0]

            # Both populated → must violate body_xor
            with pytest.raises(psycopg.errors.CheckViolation):
                cur.execute(
                    """
                    INSERT INTO raw_responses (
                        ingest_run_id, vendor, endpoint, params_hash,
                        http_status, content_type,
                        body_jsonb, body_raw,
                        raw_hash, canonical_hash
                    )
                    VALUES (%s, 'test', '/x', %s, 200, 'application/json',
                            '{}'::jsonb, '\\x00'::bytea,
                            %s, %s);
                    """,
                    (run_id, b"\x00" * 32, b"\x00" * 32, b"\x00" * 32),
                )

            # Neither populated → must violate body_xor
            with pytest.raises(psycopg.errors.CheckViolation):
                cur.execute(
                    """
                    INSERT INTO raw_responses (
                        ingest_run_id, vendor, endpoint, params_hash,
                        http_status, content_type,
                        raw_hash, canonical_hash
                    )
                    VALUES (%s, 'test', '/x', %s, 200, 'application/json',
                            %s, %s);
                    """,
                    (run_id, b"\x00" * 32, b"\x00" * 32, b"\x00" * 32),
                )

            # Exactly one populated → succeeds
            cur.execute(
                """
                INSERT INTO raw_responses (
                    ingest_run_id, vendor, endpoint, params_hash,
                    http_status, content_type,
                    body_jsonb,
                    raw_hash, canonical_hash
                )
                VALUES (%s, 'test', '/x', %s, 200, 'application/json',
                        '{"a":1}'::jsonb,
                        %s, %s);
                """,
                (run_id, b"\x00" * 32, b"\x01" * 32, b"\x02" * 32),
            )
