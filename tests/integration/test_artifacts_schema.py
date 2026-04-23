"""Integration tests for the artifacts schema.

Warning: these tests DROP and recreate the `public` schema in the
configured DATABASE_URL. Run only against a dev or dedicated test
database — never production.
"""

from __future__ import annotations

from datetime import datetime, timezone

import psycopg
import pytest

from arrow.db.connection import get_conn
from arrow.db.migrations import apply
from arrow.ingest.common.artifacts import write_artifact

H32_A = b"\x01" * 32
H32_B = b"\x02" * 32
H32_C = b"\x03" * 32
H32_D = b"\x04" * 32


def _reset(conn: psycopg.Connection) -> None:
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE;")
        cur.execute("CREATE SCHEMA public;")
    apply(conn)


def _new_run(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ingest_runs (run_kind, vendor, status, finished_at) "
            "VALUES ('manual', 'test', 'succeeded', now()) RETURNING id;"
        )
        return cur.fetchone()[0]


def _insert_artifact(
    conn: psycopg.Connection,
    *,
    ingest_run_id: int | None = None,
    artifact_type: str = "10k",
    source: str = "sec",
    ticker: str | None = "NVDA",
    raw_hash: bytes = H32_A,
    canonical_hash: bytes = H32_B,
    supersedes: int | None = None,
    superseded_at: str | None = None,
    **extra: object,
) -> int:
    cols = [
        "ingest_run_id",
        "artifact_type",
        "source",
        "ticker",
        "raw_hash",
        "canonical_hash",
        "supersedes",
        "superseded_at",
    ]
    vals = [
        ingest_run_id,
        artifact_type,
        source,
        ticker,
        raw_hash,
        canonical_hash,
        supersedes,
        superseded_at,
    ]
    for k, v in extra.items():
        cols.append(k)
        vals.append(v)
    placeholders = ", ".join(["%s"] * len(cols))
    sql = f"INSERT INTO artifacts ({', '.join(cols)}) VALUES ({placeholders}) RETURNING id;"
    with conn.cursor() as cur:
        cur.execute(sql, vals)
        return cur.fetchone()[0]


# ---------- artifact_type CHECK ----------

def test_artifact_type_rejects_unknown_value() -> None:
    with get_conn() as conn:
        _reset(conn)
        run_id = _new_run(conn)
        with pytest.raises(psycopg.errors.CheckViolation):
            _insert_artifact(conn, ingest_run_id=run_id, artifact_type="not_a_type")


def test_artifact_type_accepts_all_declared_values() -> None:
    declared = [
        "10k", "10q", "8k",
        "transcript",
        "press_release", "news_article",
        "presentation", "video_transcript",
        "research_note", "industry_primer", "product_primer", "macro_primer",
        "macro_release",
    ]
    with get_conn() as conn:
        _reset(conn)
        run_id = _new_run(conn)
        for i, t in enumerate(declared):
            _insert_artifact(
                conn,
                ingest_run_id=run_id,
                artifact_type=t,
                raw_hash=bytes([i + 1]) * 32,
                canonical_hash=bytes([100 + i]) * 32,
            )


# ---------- hash length CHECKs ----------

def test_hash_length_rejects_wrong_size() -> None:
    with get_conn() as conn:
        _reset(conn)
        run_id = _new_run(conn)
        with pytest.raises(psycopg.errors.CheckViolation):
            _insert_artifact(conn, ingest_run_id=run_id, raw_hash=b"\x00" * 31)
        with pytest.raises(psycopg.errors.CheckViolation):
            _insert_artifact(conn, ingest_run_id=run_id, canonical_hash=b"\x00" * 33)


# ---------- supersedes semantics ----------

def test_supersedes_cannot_reference_self() -> None:
    """Self-supersession is rejected by CHECK (not by FK, which would allow it)."""
    with get_conn() as conn:
        _reset(conn)
        run_id = _new_run(conn)
        artifact_id = _insert_artifact(conn, ingest_run_id=run_id)
        with pytest.raises(psycopg.errors.CheckViolation):
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE artifacts SET supersedes = %s WHERE id = %s;",
                    (artifact_id, artifact_id),
                )


def test_supersession_chain_and_current_query() -> None:
    """Insert A; insert A' superseding A; query current via superseded_at IS NULL."""
    with get_conn() as conn:
        _reset(conn)
        run_id = _new_run(conn)

        a = _insert_artifact(
            conn,
            ingest_run_id=run_id,
            raw_hash=H32_A,
            canonical_hash=H32_B,
        )
        a_prime = _insert_artifact(
            conn,
            ingest_run_id=run_id,
            raw_hash=H32_C,
            canonical_hash=H32_D,
            supersedes=a,
        )
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE artifacts SET superseded_at = now() WHERE id = %s;", (a,)
            )

            # Current: only A' (A is superseded)
            cur.execute(
                "SELECT id FROM artifacts WHERE superseded_at IS NULL ORDER BY id;"
            )
            assert [row[0] for row in cur.fetchall()] == [a_prime]

            # Chain traversal: A' -> A
            cur.execute("SELECT supersedes FROM artifacts WHERE id = %s;", (a_prime,))
            assert cur.fetchone()[0] == a


def test_write_artifact_uses_replacing_publication_time_for_supersession() -> None:
    with get_conn() as conn:
        _reset(conn)
        first_published = datetime(2024, 1, 1, tzinfo=timezone.utc)
        second_published = datetime(2024, 2, 1, tzinfo=timezone.utc)

        first_id, created = write_artifact(
            conn,
            ingest_run_id=None,
            artifact_type="10k",
            source="sec",
            source_document_id="0001045810-24-000001",
            body=b"old body",
            ticker="NVDA",
            content_type="text/html",
            published_at=first_published,
        )
        assert created is True

        second_id, created = write_artifact(
            conn,
            ingest_run_id=None,
            artifact_type="10k",
            source="sec",
            source_document_id="0001045810-24-000001",
            body=b"new body",
            ticker="NVDA",
            content_type="text/html",
            published_at=second_published,
        )
        assert created is True
        assert second_id != first_id

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, supersedes, superseded_at
                FROM artifacts
                ORDER BY id;
                """
            )
            rows = cur.fetchall()

        assert rows == [
            (first_id, None, second_published),
            (second_id, first_id, None),
        ]


def test_supersedes_fk_blocks_delete_of_referenced_row() -> None:
    """ON DELETE RESTRICT: deleting a row that is referenced by supersedes fails."""
    with get_conn() as conn:
        _reset(conn)
        run_id = _new_run(conn)
        a = _insert_artifact(conn, ingest_run_id=run_id, raw_hash=H32_A, canonical_hash=H32_B)
        _insert_artifact(conn, ingest_run_id=run_id, raw_hash=H32_C, canonical_hash=H32_D, supersedes=a)
        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            with conn.cursor() as cur:
                cur.execute("DELETE FROM artifacts WHERE id = %s;", (a,))


# ---------- period label regex CHECKs ----------

def test_fiscal_period_label_regex_rejects_malformed() -> None:
    with get_conn() as conn:
        _reset(conn)
        run_id = _new_run(conn)
        for bad in ["FY2024Q4", "Q4 FY2024", "fy2024 q4", "FY24 Q4", "FY2024 Q5"]:
            with pytest.raises(psycopg.errors.CheckViolation):
                _insert_artifact(
                    conn,
                    ingest_run_id=run_id,
                    raw_hash=bytes([len(bad)]) * 32,
                    canonical_hash=bytes([len(bad) + 1]) * 32,
                    fiscal_period_label=bad,
                )


def test_fiscal_period_label_regex_accepts_valid() -> None:
    with get_conn() as conn:
        _reset(conn)
        run_id = _new_run(conn)
        _insert_artifact(
            conn,
            ingest_run_id=run_id,
            raw_hash=H32_A,
            canonical_hash=H32_B,
            fiscal_period_label="FY2025 Q4",
            fiscal_year=2025,
            fiscal_quarter=4,
            period_type="quarter",
        )
        _insert_artifact(
            conn,
            ingest_run_id=run_id,
            raw_hash=H32_C,
            canonical_hash=H32_D,
            fiscal_period_label="FY2025",
            fiscal_year=2025,
            period_type="annual",
        )


def test_period_type_quarter_iff_fiscal_quarter_present() -> None:
    """period_type='quarter' requires fiscal_quarter NOT NULL, and vice versa."""
    with get_conn() as conn:
        _reset(conn)
        run_id = _new_run(conn)

        # quarter without fiscal_quarter → reject
        with pytest.raises(psycopg.errors.CheckViolation):
            _insert_artifact(
                conn,
                ingest_run_id=run_id,
                raw_hash=H32_A,
                canonical_hash=H32_B,
                period_type="quarter",
                fiscal_quarter=None,
            )

        # annual with fiscal_quarter set → reject
        with pytest.raises(psycopg.errors.CheckViolation):
            _insert_artifact(
                conn,
                ingest_run_id=run_id,
                raw_hash=H32_C,
                canonical_hash=H32_D,
                period_type="annual",
                fiscal_quarter=4,
            )


# artifact_chunks tests removed: table dropped in migration 006.
# Re-add when chunking is reintroduced against actual ingested documents.
