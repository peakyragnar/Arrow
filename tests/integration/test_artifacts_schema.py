"""Integration tests for the artifacts + artifact_chunks schema.

Warning: these tests DROP and recreate the `public` schema in the
configured DATABASE_URL. Run only against a dev or dedicated test
database — never production.
"""

from __future__ import annotations

import psycopg
import pytest

from arrow.db.connection import get_conn
from arrow.db.migrations import apply

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


# ---------- artifact_chunks FTS + CHECKs ----------

def test_chunk_tsv_generated_from_text_by_default() -> None:
    with get_conn() as conn:
        _reset(conn)
        run_id = _new_run(conn)
        artifact_id = _insert_artifact(conn, ingest_run_id=run_id)

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO artifact_chunks (artifact_id, chunk_type, ordinal, text)
                VALUES (%s, 'paragraph', 0, 'The quick brown fox jumps over the lazy dog')
                RETURNING id;
                """,
                (artifact_id,),
            )
            chunk_id = cur.fetchone()[0]

            cur.execute(
                "SELECT tsv FROM artifact_chunks WHERE id = %s;", (chunk_id,)
            )
            tsv = cur.fetchone()[0]
            # tsv should be populated and searchable
            assert tsv is not None
            assert "fox" in str(tsv)


def test_chunk_tsv_prefers_search_text_when_provided() -> None:
    """If search_text is set, the generated tsv uses it instead of text."""
    with get_conn() as conn:
        _reset(conn)
        run_id = _new_run(conn)
        artifact_id = _insert_artifact(conn, ingest_run_id=run_id)

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO artifact_chunks (artifact_id, chunk_type, ordinal, text, search_text)
                VALUES (%s, 'paragraph', 0, 'noisy table scaffolding', 'revenue grew strongly')
                RETURNING id;
                """,
                (artifact_id,),
            )
            chunk_id = cur.fetchone()[0]

            # FTS matches against search_text terms
            cur.execute(
                "SELECT id FROM artifact_chunks WHERE tsv @@ plainto_tsquery('english', 'revenue');"
            )
            assert [row[0] for row in cur.fetchall()] == [chunk_id]

            # FTS does NOT match against the (raw) text terms when search_text is present
            cur.execute(
                "SELECT id FROM artifact_chunks WHERE tsv @@ plainto_tsquery('english', 'scaffolding');"
            )
            assert cur.fetchall() == []


def test_chunk_unique_ordinal_per_artifact() -> None:
    with get_conn() as conn:
        _reset(conn)
        run_id = _new_run(conn)
        a1 = _insert_artifact(conn, ingest_run_id=run_id)
        a2 = _insert_artifact(
            conn, ingest_run_id=run_id, raw_hash=H32_C, canonical_hash=H32_D
        )
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO artifact_chunks (artifact_id, chunk_type, ordinal, text) "
                "VALUES (%s, 'section', 0, 'first');",
                (a1,),
            )
            # Same ordinal, same artifact → reject
            with pytest.raises(psycopg.errors.UniqueViolation):
                cur.execute(
                    "INSERT INTO artifact_chunks (artifact_id, chunk_type, ordinal, text) "
                    "VALUES (%s, 'section', 0, 'second');",
                    (a1,),
                )
            # Same ordinal in a different artifact → OK
            cur.execute(
                "INSERT INTO artifact_chunks (artifact_id, chunk_type, ordinal, text) "
                "VALUES (%s, 'section', 0, 'other artifact');",
                (a2,),
            )


def test_chunk_type_check_rejects_unknown() -> None:
    with get_conn() as conn:
        _reset(conn)
        run_id = _new_run(conn)
        artifact_id = _insert_artifact(conn, ingest_run_id=run_id)
        with pytest.raises(psycopg.errors.CheckViolation):
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO artifact_chunks (artifact_id, chunk_type, ordinal, text) "
                    "VALUES (%s, 'not_a_chunk_type', 0, 'x');",
                    (artifact_id,),
                )


def test_chunk_timestamp_order_check() -> None:
    with get_conn() as conn:
        _reset(conn)
        run_id = _new_run(conn)
        artifact_id = _insert_artifact(conn, ingest_run_id=run_id, artifact_type="transcript")
        with pytest.raises(psycopg.errors.CheckViolation):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO artifact_chunks (
                        artifact_id, chunk_type, ordinal, text, starts_at, ends_at
                    )
                    VALUES (%s, 'timestamp_span', 0, 'x',
                            interval '10 seconds', interval '5 seconds');
                    """,
                    (artifact_id,),
                )


# ---------- full FTS round-trip ----------

def test_fts_retrieves_chunk_by_phrase() -> None:
    with get_conn() as conn:
        _reset(conn)
        run_id = _new_run(conn)
        artifact_id = _insert_artifact(conn, ingest_run_id=run_id, artifact_type="transcript")
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO artifact_chunks (artifact_id, chunk_type, ordinal, text, speaker)
                VALUES
                    (%s, 'speaker_turn', 0, 'Data center revenue grew 427 percent year over year', 'Jensen Huang'),
                    (%s, 'speaker_turn', 1, 'Gaming revenue was 2.87 billion up 15 percent', 'Colette Kress'),
                    (%s, 'speaker_turn', 2, 'Automotive revenue declined sequentially', 'Colette Kress');
                """,
                (artifact_id, artifact_id, artifact_id),
            )
            cur.execute(
                """
                SELECT ordinal FROM artifact_chunks
                WHERE tsv @@ websearch_to_tsquery('english', 'data center revenue')
                ORDER BY ordinal;
                """
            )
            assert [row[0] for row in cur.fetchall()] == [0]

            cur.execute(
                """
                SELECT ordinal FROM artifact_chunks
                WHERE speaker = 'Colette Kress'
                  AND tsv @@ websearch_to_tsquery('english', 'revenue')
                ORDER BY ordinal;
                """
            )
            assert [row[0] for row in cur.fetchall()] == [1, 2]
