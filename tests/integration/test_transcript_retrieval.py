"""Integration tests for transcript retrieval primitives."""

from __future__ import annotations

import os
from datetime import date, datetime, timezone

import psycopg

from arrow.db.connection import get_conn
from arrow.db.migrations import apply
from arrow.retrieval.transcripts import (
    compare_transcript_mentions,
    get_latest_transcripts,
    get_transcript_context,
    search_transcript_turns,
)

os.environ.setdefault("FMP_API_KEY", "test-key-for-integration")

H32 = b"\x00" * 32


def _reset(conn: psycopg.Connection) -> None:
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE;")
        cur.execute("CREATE SCHEMA public;")
    apply(conn)


def _seed_company(conn: psycopg.Connection, *, ticker: str = "ARRW") -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO companies (cik, ticker, name, fiscal_year_end_md)
            VALUES (9999999, %s, 'Arrow Test Co', '12-31')
            RETURNING id;
            """,
            (ticker,),
        )
        return cur.fetchone()[0]


def _seed_run(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingest_runs (run_kind, vendor, status, finished_at)
            VALUES ('manual', 'fmp', 'succeeded', now())
            RETURNING id;
            """
        )
        return cur.fetchone()[0]


def _seed_transcript(
    conn: psycopg.Connection,
    *,
    company_id: int,
    run_id: int,
    ticker: str = "ARRW",
    fiscal_year: int,
    fiscal_quarter: int,
    period_end: date,
    published_at: datetime,
    turns: list[tuple[str, str]],
    superseded_at: datetime | None = None,
) -> tuple[int, list[int], list[int]]:
    fiscal_period_key = f"FY{fiscal_year} Q{fiscal_quarter}"
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO artifacts (
                ingest_run_id, artifact_type, source, source_document_id,
                raw_hash, canonical_hash, ticker, fiscal_year, fiscal_quarter,
                fiscal_period_label, period_type, period_end, published_at,
                company_id, fiscal_period_key, superseded_at
            ) VALUES (
                %s, 'transcript', 'fmp', %s,
                %s, %s, %s, %s, %s,
                %s, 'quarter', %s, %s,
                %s, %s, %s
            )
            RETURNING id;
            """,
            (
                run_id,
                f"fmp:earning-call-transcript:{ticker}:FY{fiscal_year}-Q{fiscal_quarter}",
                _hash_for(f"{ticker}-{fiscal_year}-{fiscal_quarter}-raw"),
                _hash_for(f"{ticker}-{fiscal_year}-{fiscal_quarter}-canonical"),
                ticker,
                fiscal_year,
                fiscal_quarter,
                fiscal_period_key,
                period_end,
                published_at,
                company_id,
                fiscal_period_key,
                superseded_at,
            ),
        )
        artifact_id = cur.fetchone()[0]
        unit_ids: list[int] = []
        chunk_ids: list[int] = []
        offset = 0
        for ordinal, (speaker, text) in enumerate(turns, start=1):
            full_text = f"{speaker}: {text}"
            cur.execute(
                """
                INSERT INTO artifact_text_units (
                    artifact_id, company_id, fiscal_period_key,
                    unit_ordinal, unit_type, unit_key, unit_title, text,
                    start_offset, end_offset, extractor_version, confidence,
                    extraction_method
                ) VALUES (
                    %s, %s, %s,
                    %s, 'transcript', %s, %s, %s,
                    %s, %s, 'test-transcript-v1', 0.9,
                    'deterministic'
                )
                RETURNING id;
                """,
                (
                    artifact_id,
                    company_id,
                    fiscal_period_key,
                    ordinal,
                    f"turn:{ordinal:03d}",
                    speaker,
                    full_text,
                    offset,
                    offset + len(full_text),
                ),
            )
            unit_id = cur.fetchone()[0]
            unit_ids.append(unit_id)
            cur.execute(
                """
                INSERT INTO artifact_text_chunks (
                    text_unit_id, chunk_ordinal, text, search_text, heading_path,
                    start_offset, end_offset, chunker_version
                ) VALUES (
                    %s, 1, %s, %s, ARRAY[%s]::text[],
                    0, %s, 'test-chunker-v1'
                )
                RETURNING id;
                """,
                (unit_id, full_text, full_text.lower(), speaker, len(full_text)),
            )
            chunk_ids.append(cur.fetchone()[0])
            offset += len(full_text) + 1
    return artifact_id, unit_ids, chunk_ids


def _hash_for(seed: str) -> bytes:
    import hashlib
    return hashlib.sha256(seed.encode()).digest()


def test_get_latest_transcripts_orders_current_artifacts() -> None:
    with get_conn() as conn:
        _reset(conn)
        company_id = _seed_company(conn)
        run_id = _seed_run(conn)
        old_id, _, _ = _seed_transcript(
            conn,
            company_id=company_id,
            run_id=run_id,
            fiscal_year=2024,
            fiscal_quarter=4,
            period_end=date(2024, 12, 31),
            published_at=datetime(2025, 2, 20, tzinfo=timezone.utc),
            turns=[("CFO", "Revenue margin commentary.")],
            superseded_at=datetime(2025, 3, 1, tzinfo=timezone.utc),
        )
        q1_id, _, _ = _seed_transcript(
            conn,
            company_id=company_id,
            run_id=run_id,
            fiscal_year=2025,
            fiscal_quarter=1,
            period_end=date(2025, 3, 31),
            published_at=datetime(2025, 5, 1, tzinfo=timezone.utc),
            turns=[("CFO", "Operating margin expanded.")],
        )
        q2_id, _, _ = _seed_transcript(
            conn,
            company_id=company_id,
            run_id=run_id,
            fiscal_year=2025,
            fiscal_quarter=2,
            period_end=date(2025, 6, 30),
            published_at=datetime(2025, 8, 1, tzinfo=timezone.utc),
            turns=[("CEO", "AI demand stayed strong.")],
        )

        docs = get_latest_transcripts(conn, "arrw", n=5)

    assert [doc.artifact_id for doc in docs] == [q2_id, q1_id]
    assert old_id not in [doc.artifact_id for doc in docs]
    assert docs[0].turn_count == 1
    assert docs[0].chunk_count == 1


def test_search_transcript_turns_filters_period_and_returns_speaker_context() -> None:
    with get_conn() as conn:
        _reset(conn)
        company_id = _seed_company(conn)
        run_id = _seed_run(conn)
        _seed_transcript(
            conn,
            company_id=company_id,
            run_id=run_id,
            fiscal_year=2025,
            fiscal_quarter=1,
            period_end=date(2025, 3, 31),
            published_at=datetime(2025, 5, 1, tzinfo=timezone.utc),
            turns=[("CFO", "Gross margin expanded as data center demand improved.")],
        )
        _, _, q2_chunks = _seed_transcript(
            conn,
            company_id=company_id,
            run_id=run_id,
            fiscal_year=2025,
            fiscal_quarter=2,
            period_end=date(2025, 6, 30),
            published_at=datetime(2025, 8, 1, tzinfo=timezone.utc),
            turns=[
                ("Operator", "Welcome to the call."),
                ("CFO", "Operating margin expanded again due to higher utilization."),
            ],
        )

        rows = search_transcript_turns(
            conn,
            "ARRW",
            '"operating margin"',
            fiscal_period_key="FY2025 Q2",
            limit=5,
        )

    assert len(rows) == 1
    assert rows[0].speaker == "CFO"
    assert rows[0].fiscal_period_key == "FY2025 Q2"
    assert rows[0].chunk_id == q2_chunks[1]
    assert rows[0].rank is not None


def test_get_transcript_context_returns_ordered_turns() -> None:
    with get_conn() as conn:
        _reset(conn)
        company_id = _seed_company(conn)
        run_id = _seed_run(conn)
        artifact_id, _, _ = _seed_transcript(
            conn,
            company_id=company_id,
            run_id=run_id,
            fiscal_year=2025,
            fiscal_quarter=3,
            period_end=date(2025, 9, 30),
            published_at=datetime(2025, 11, 1, tzinfo=timezone.utc),
            turns=[
                ("Operator", "Welcome."),
                ("CEO", "AI demand accelerated."),
                ("CFO", "Margins improved."),
            ],
        )

        context = get_transcript_context(conn, "ARRW", "FY2025 Q3", max_turns=2)

    assert context is not None
    assert context.document.artifact_id == artifact_id
    assert [turn.speaker for turn in context.turns] == ["Operator", "CEO"]
    assert context.turns[1].text.endswith("AI demand accelerated.")


def test_compare_transcript_mentions_counts_latest_periods() -> None:
    with get_conn() as conn:
        _reset(conn)
        company_id = _seed_company(conn)
        run_id = _seed_run(conn)
        _seed_transcript(
            conn,
            company_id=company_id,
            run_id=run_id,
            fiscal_year=2025,
            fiscal_quarter=1,
            period_end=date(2025, 3, 31),
            published_at=datetime(2025, 5, 1, tzinfo=timezone.utc),
            turns=[("CEO", "Demand improved. AI demand was broad.")],
        )
        _seed_transcript(
            conn,
            company_id=company_id,
            run_id=run_id,
            fiscal_year=2025,
            fiscal_quarter=2,
            period_end=date(2025, 6, 30),
            published_at=datetime(2025, 8, 1, tzinfo=timezone.utc),
            turns=[("CEO", "Demand remained strong.")],
        )

        summaries = compare_transcript_mentions(
            conn,
            "ARRW",
            ["demand", "AI"],
            periods=2,
        )

    assert [row.fiscal_period_key for row in summaries] == ["FY2025 Q2", "FY2025 Q1"]
    assert summaries[0].term_counts == {"demand": 1, "AI": 0}
    assert summaries[1].term_counts == {"demand": 2, "AI": 1}

