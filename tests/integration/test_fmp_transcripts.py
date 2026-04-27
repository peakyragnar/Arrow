from __future__ import annotations

import json
import shutil
from datetime import date, datetime, timezone

import psycopg
import pytest

from arrow.agents.fmp_transcripts import (
    CompanyRow,
    MissingFiscalAnchor,
    _normalize_one,
    _source_document_id,
    ingest_transcripts,
)
from arrow.db.connection import get_conn
from arrow.db.migrations import apply as apply_migrations
from arrow.ingest.common.http import Response
from arrow.ingest.fmp.paths import fmp_transcript_dates_path, fmp_transcript_path
from arrow.ingest.fmp.transcripts import Transcript, TranscriptFetch
from arrow.steward.coverage import compute_coverage_matrix
from arrow.steward.registry import Scope
from arrow.steward.runner import run_steward

import arrow.steward.checks  # noqa: F401


def _reset(conn: psycopg.Connection) -> None:
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE;")
        cur.execute("CREATE SCHEMA public;")
    apply_migrations(conn)


def _seed_company(conn: psycopg.Connection, *, ticker: str = "NVDA") -> CompanyRow:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO companies (cik, ticker, name, fiscal_year_end_md)
            VALUES (%s, %s, %s, %s)
            RETURNING id, cik, ticker, fiscal_year_end_md;
            """,
            (1045810, ticker, f"{ticker} Corp", "01-31"),
        )
        row = cur.fetchone()
    return CompanyRow(id=row[0], cik=row[1], ticker=row[2], fiscal_year_end_md=row[3])


def _seed_run(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingest_runs (run_kind, vendor, status, finished_at)
            VALUES ('manual', 'test', 'succeeded', now())
            RETURNING id;
            """
        )
        return cur.fetchone()[0]


def _seed_raw(conn: psycopg.Connection, run_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw_responses (
                ingest_run_id, vendor, endpoint, params_hash, request_url,
                http_status, content_type, body_jsonb, raw_hash, canonical_hash
            ) VALUES (
                %s, 'fmp', 'fixture', %s, 'https://example.test',
                200, 'application/json', '{}'::jsonb, %s, %s
            )
            RETURNING id;
            """,
            (run_id, b"\x01" * 32, b"\x02" * 32, b"\x03" * 32),
        )
        return cur.fetchone()[0]


def _seed_anchor_fact(
    conn: psycopg.Connection,
    *,
    company_id: int,
    fiscal_year: int = 2025,
    fiscal_quarter: int = 2,
    period_end: date = date(2024, 7, 28),
    statement: str = "income_statement",
    extraction_version: str = "fixture-v1",
) -> None:
    run_id = _seed_run(conn)
    raw_id = _seed_raw(conn, run_id)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO financial_facts (
                ingest_run_id, company_id, statement, concept,
                fiscal_year, fiscal_quarter, fiscal_period_label,
                period_end, period_type,
                calendar_year, calendar_quarter, calendar_period_label,
                value, unit, source_raw_response_id, extraction_version,
                published_at,
                dimension_type, dimension_key, dimension_label, dimension_source
            ) VALUES (
                %s, %s, %s, 'revenue',
                %s, %s, %s,
                %s, 'quarter',
                2024, 3, 'CY2024 Q3',
                100.0, 'usd', %s, %s,
                %s,
                %s, %s, %s, %s
            );
            """,
            (
                run_id,
                company_id,
                statement,
                fiscal_year,
                fiscal_quarter,
                f"FY{fiscal_year} Q{fiscal_quarter}",
                period_end,
                raw_id,
                extraction_version,
                datetime(2024, 8, 28, tzinfo=timezone.utc),
                "product" if statement == "segment" else None,
                "data_center" if statement == "segment" else None,
                "Data Center" if statement == "segment" else None,
                "fmp:fixture" if statement == "segment" else None,
            ),
        )


def _fetch(
    *,
    content: str,
    ticker: str = "NVDA",
    fiscal_year: int = 2025,
    fiscal_quarter: int = 2,
    call_date: date = date(2024, 8, 28),
) -> TranscriptFetch:
    row = {
        "symbol": ticker,
        "period": f"Q{fiscal_quarter}",
        "year": fiscal_year,
        "date": call_date.isoformat(),
        "content": content,
    }
    return TranscriptFetch(
        raw_response_id=1,
        transcript=Transcript(
            ticker=ticker,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            call_date=call_date,
            content=content,
            raw_row=row,
        ),
        raw_body=json.dumps([row], sort_keys=True).encode("utf-8"),
    )


def _content(marker: str) -> str:
    return (
        f"Operator: Welcome to the {marker} call.\n"
        "Jane Doe: Revenue grew because demand improved. We expanded capacity.\n"
        "John Smith: Thank you. My question is about margins and supply.\n"
    )


def _artifact_rows(conn: psycopg.Connection) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, source_document_id, fiscal_period_key, period_end,
                   calendar_year, calendar_quarter, supersedes, superseded_at
            FROM artifacts
            ORDER BY id;
            """
        )
        return cur.fetchall()


def test_artifact_text_units_accept_transcript_unit_type_and_reject_unknown() -> None:
    with get_conn() as conn:
        _reset(conn)
        company = _seed_company(conn)
        run_id = _seed_run(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO artifacts (
                    ingest_run_id, artifact_type, source, ticker, company_id,
                    raw_hash, canonical_hash
                ) VALUES (%s, 'transcript', 'fmp', 'NVDA', %s, %s, %s)
                RETURNING id;
                """,
                (run_id, company.id, b"\x04" * 32, b"\x05" * 32),
            )
            artifact_id = cur.fetchone()[0]
            cur.execute(
                """
                INSERT INTO artifact_text_units (
                    artifact_id, company_id, fiscal_period_key,
                    unit_ordinal, unit_type, unit_key, unit_title,
                    text, start_offset, end_offset,
                    extractor_version, confidence, extraction_method
                ) VALUES (
                    %s, %s, 'FY2025 Q2',
                    1, 'transcript', 'turn:001', 'Operator',
                    'Operator: hello', 0, 15,
                    'test', 0.9, 'deterministic'
                );
                """,
                (artifact_id, company.id),
            )
            with pytest.raises(psycopg.errors.CheckViolation):
                cur.execute(
                    """
                    INSERT INTO artifact_text_units (
                        artifact_id, company_id, fiscal_period_key,
                        unit_ordinal, unit_type, unit_key, unit_title,
                        text, start_offset, end_offset,
                        extractor_version, confidence, extraction_method
                    ) VALUES (
                        %s, %s, 'FY2025 Q2',
                        2, 'not_a_unit_type', 'x', 'X',
                        'X: nope', 0, 7,
                        'test', 0.9, 'deterministic'
                    );
                    """,
                    (artifact_id, company.id),
                )


def test_normalize_requires_fiscal_anchor() -> None:
    with get_conn() as conn:
        _reset(conn)
        company = _seed_company(conn)
        run_id = _seed_run(conn)

        with pytest.raises(MissingFiscalAnchor, match="backfill_fmp.py NVDA"):
            _normalize_one(
                conn,
                company=company,
                fetched=_fetch(content=_content("first")),
                ingest_run_id=run_id,
            )


def test_normalize_uses_fact_period_end_for_two_clock_truth() -> None:
    with get_conn() as conn:
        _reset(conn)
        company = _seed_company(conn)
        _seed_anchor_fact(conn, company_id=company.id)
        run_id = _seed_run(conn)

        result = _normalize_one(
            conn,
            company=company,
            fetched=_fetch(content=_content("first")),
            ingest_run_id=run_id,
        )

        assert result.created is True
        rows = _artifact_rows(conn)
        assert len(rows) == 1
        _, source_document_id, fiscal_period_key, period_end, cal_year, cal_quarter, _, _ = rows[0]
        assert source_document_id == _source_document_id("NVDA", 2025, 2)
        assert fiscal_period_key == "FY2025 Q2"
        assert period_end == date(2024, 7, 28)
        assert cal_year == 2024
        assert cal_quarter == 3


def test_normalize_ignores_segment_period_end_when_statement_facts_agree() -> None:
    with get_conn() as conn:
        _reset(conn)
        company = _seed_company(conn)
        _seed_anchor_fact(
            conn,
            company_id=company.id,
            period_end=date(2024, 7, 28),
            statement="income_statement",
            extraction_version="fixture-is-v1",
        )
        _seed_anchor_fact(
            conn,
            company_id=company.id,
            period_end=date(2024, 7, 28),
            statement="balance_sheet",
            extraction_version="fixture-bs-v1",
        )
        _seed_anchor_fact(
            conn,
            company_id=company.id,
            period_end=date(2024, 7, 27),
            statement="segment",
            extraction_version="fixture-segment-v1",
        )
        run_id = _seed_run(conn)

        _normalize_one(
            conn,
            company=company,
            fetched=_fetch(content=_content("first")),
            ingest_run_id=run_id,
        )

        rows = _artifact_rows(conn)
        assert rows[0][3] == date(2024, 7, 28)


def test_normalize_falls_back_to_unparsed_unit_when_parser_fails() -> None:
    with get_conn() as conn:
        _reset(conn)
        company = _seed_company(conn)
        _seed_anchor_fact(conn, company_id=company.id)
        run_id = _seed_run(conn)

        result = _normalize_one(
            conn,
            company=company,
            fetched=_fetch(content="No speaker markers in this transcript."),
            ingest_run_id=run_id,
        )

        assert result.text_units_inserted == 1
        with conn.cursor() as cur:
            cur.execute(
                "SELECT unit_key, extraction_method, confidence FROM artifact_text_units;"
            )
            assert cur.fetchall() == [("unparsed", "unparsed_fallback", 0.0)]


def test_supersession_noop_and_current_coverage() -> None:
    with get_conn() as conn:
        _reset(conn)
        company = _seed_company(conn)
        _seed_anchor_fact(conn, company_id=company.id)
        run_id = _seed_run(conn)

        first = _normalize_one(
            conn,
            company=company,
            fetched=_fetch(content=_content("old")),
            ingest_run_id=run_id,
        )
        same = _normalize_one(
            conn,
            company=company,
            fetched=_fetch(content=_content("old")),
            ingest_run_id=run_id,
        )
        second = _normalize_one(
            conn,
            company=company,
            fetched=_fetch(content=_content("new")),
            ingest_run_id=run_id,
        )

        assert first.created is True
        assert same.created is False
        assert second.created is True
        assert second.superseded_existing is True

        rows = _artifact_rows(conn)
        assert len(rows) == 2
        old_id = rows[0][0]
        new_id = rows[1][0]
        assert rows[0][6] is None
        assert rows[0][7] == datetime(2024, 8, 28, tzinfo=timezone.utc)
        assert rows[1][6] == old_id
        assert rows[1][7] is None

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM artifact_text_units;")
            assert cur.fetchone()[0] == (
                first.text_units_inserted + second.text_units_inserted
            )
            cur.execute(
                """
                SELECT a.id, ch.search_text
                FROM artifact_text_chunks ch
                JOIN artifact_text_units u ON u.id = ch.text_unit_id
                JOIN artifacts a ON a.id = u.artifact_id
                WHERE a.superseded_at IS NULL
                ORDER BY ch.id;
                """
            )
            current_chunks = cur.fetchall()
        assert current_chunks
        assert {row[0] for row in current_chunks} == {new_id}
        assert any("new call" in row[1] for row in current_chunks)
        assert not any("old call" in row[1] for row in current_chunks)

        matrix = compute_coverage_matrix(conn)
        transcript_cell = matrix[0].by_vertical["transcript"]
        assert transcript_cell.row_count == 1
        assert transcript_cell.period_count == 1


def test_transcript_artifact_orphans_check_flags_current_artifact_without_units() -> None:
    with get_conn() as conn:
        _reset(conn)
        company = _seed_company(conn)
        run_id = _seed_run(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO artifacts (
                    ingest_run_id, artifact_type, source, ticker, company_id,
                    fiscal_period_key, raw_hash, canonical_hash
                ) VALUES (
                    %s, 'transcript', 'fmp', 'NVDA', %s,
                    'FY2025 Q2', %s, %s
                );
                """,
                (run_id, company.id, b"\x08" * 32, b"\x09" * 32),
            )

        run_steward(conn, scope=Scope(check_names=["transcript_artifact_orphans"]))
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ticker, vertical, summary
                FROM data_quality_findings
                WHERE source_check = 'transcript_artifact_orphans'
                  AND status = 'open';
                """
            )
            rows = cur.fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "NVDA"
    assert rows[0][1] == "transcript"
    assert "no text units" in rows[0][2]


class _FakeFMPClient:
    def get(self, endpoint: str, **params):
        if endpoint == "earning-call-transcript-dates":
            body = json.dumps([
                {"quarter": 2, "fiscalYear": 2025, "date": "2024-08-28"}
            ]).encode("utf-8")
        elif endpoint == "earning-call-transcript":
            body = json.dumps([
                {
                    "symbol": params["symbol"],
                    "period": "Q2",
                    "year": 2025,
                    "date": "2024-08-28",
                    "content": _content("fetcher"),
                }
            ]).encode("utf-8")
        else:
            raise AssertionError(endpoint)
        return Response(
            status=200,
            headers={"content-type": "application/json"},
            body=body,
            content_type="application/json",
            url=f"https://example.test/{endpoint}",
        )


class _MultiQuarterFakeFMPClient:
    """Fake client that ships transcripts for two quarters; the test seeds
    a financial anchor for only one of them so the orchestrator must skip
    the other (mirrors DELL pre-IPO transcripts that have no anchor)."""

    def get(self, endpoint: str, **params):
        if endpoint == "earning-call-transcript-dates":
            body = json.dumps([
                {"quarter": 4, "fiscalYear": 2014, "date": "2014-12-15"},
                {"quarter": 2, "fiscalYear": 2025, "date": "2024-08-28"},
            ]).encode("utf-8")
        elif endpoint == "earning-call-transcript":
            year = params.get("year")
            quarter = params.get("quarter")
            body = json.dumps([
                {
                    "symbol": params["symbol"],
                    "period": f"Q{quarter}",
                    "year": year,
                    "date": "2014-12-15" if year == 2014 else "2024-08-28",
                    "content": _content("fetcher"),
                }
            ]).encode("utf-8")
        else:
            raise AssertionError(endpoint)
        return Response(
            status=200,
            headers={"content-type": "application/json"},
            body=body,
            content_type="application/json",
            url=f"https://example.test/{endpoint}",
        )


def test_ingest_transcripts_skips_transcripts_without_fiscal_anchor() -> None:
    """When FMP ships a transcript for a period that has no financial
    anchor (e.g., DELL pre-IPO years), the orchestrator must skip it
    silently — counted in `transcripts_skipped_no_anchor` — rather than
    aborting the whole transaction. The anchored transcript still loads."""
    ticker = "DLLTST"
    dates_path = fmp_transcript_dates_path(ticker)
    paths_to_clean = [
        dates_path,
        fmp_transcript_path(ticker, 2014, 4),
        fmp_transcript_path(ticker, 2025, 2),
    ]
    try:
        with get_conn() as conn:
            _reset(conn)
            company = _seed_company(conn, ticker=ticker)
            # Anchor only FY2025 Q2 — FY2014 Q4 will be unanchored.
            _seed_anchor_fact(conn, company_id=company.id)

            counts = ingest_transcripts(
                conn,
                [ticker],
                client=_MultiQuarterFakeFMPClient(),  # type: ignore[arg-type]
            )

            assert counts["transcripts_fetched"] == 2
            assert counts["transcripts_skipped_no_anchor"] == 1
            assert counts["artifacts_inserted"] == 1
    finally:
        for p in paths_to_clean:
            p.unlink(missing_ok=True)
        for p in paths_to_clean:
            ticker_dir = p.parent
            if ticker_dir.exists() and not any(ticker_dir.iterdir()):
                ticker_dir.rmdir()


def test_ingest_transcripts_fetches_raw_cache_and_nonzero_counts() -> None:
    ticker = "ZZTST"
    dates_path = fmp_transcript_dates_path(ticker)
    transcript_path = fmp_transcript_path(ticker, 2025, 2)
    try:
        with get_conn() as conn:
            _reset(conn)
            company = _seed_company(conn, ticker=ticker)
            _seed_anchor_fact(conn, company_id=company.id)

            counts = ingest_transcripts(
                conn,
                [ticker],
                limit_per_ticker=1,
                client=_FakeFMPClient(),  # type: ignore[arg-type]
            )

            assert counts["raw_responses"] == 2
            assert counts["transcripts_fetched"] == 1
            assert counts["artifacts_inserted"] == 1
            assert counts["text_units_inserted"] > 0
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM raw_responses WHERE vendor = 'fmp';")
                assert cur.fetchone()[0] >= 2
    finally:
        dates_path.unlink(missing_ok=True)
        if transcript_path.exists():
            transcript_path.unlink()
        ticker_dir = transcript_path.parent
        if ticker_dir.exists():
            shutil.rmtree(ticker_dir)
        # Leave endpoint directories alone if other real cache files exist.

    assert not dates_path.exists()
    assert not transcript_path.exists()
