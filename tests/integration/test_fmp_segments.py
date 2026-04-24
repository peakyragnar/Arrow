"""Integration tests for FMP revenue segmentation ingest."""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timezone
from unittest.mock import patch

import psycopg

from arrow.db.connection import get_conn
from arrow.db.migrations import apply
from arrow.ingest.common.http import Response

os.environ.setdefault("FMP_API_KEY", "test-key-for-integration")

H32 = b"\x00" * 32


def _reset(conn: psycopg.Connection) -> None:
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE;")
        cur.execute("CREATE SCHEMA public;")
    apply(conn)


def _seed_nvda(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO companies (id, cik, ticker, name, fiscal_year_end_md)
            VALUES (1, 1045810, 'NVDA', 'NVIDIA Corporation', '01-31')
            RETURNING id;
            """
        )
        return cur.fetchone()[0]


def _seed_run(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ingest_runs (run_kind, vendor, status, finished_at) "
            "VALUES ('manual', 'fmp', 'succeeded', now()) RETURNING id;"
        )
        return cur.fetchone()[0]


def _seed_raw_response(conn: psycopg.Connection, run_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw_responses (
                ingest_run_id, vendor, endpoint, params_hash,
                http_status, content_type, body_jsonb,
                raw_hash, canonical_hash
            )
            VALUES (%s, 'fmp', 'income-statement', %s, 200, 'application/json',
                    '{}'::jsonb, %s, %s)
            RETURNING id;
            """,
            (run_id, H32, H32, H32),
        )
        return cur.fetchone()[0]


def _seed_income_revenue(
    conn: psycopg.Connection,
    *,
    company_id: int,
    raw_response_id: int,
    ingest_run_id: int,
) -> None:
    published_at = datetime(2026, 2, 25, 16, 42, 19, tzinfo=timezone.utc)
    with conn.cursor() as cur:
        for fiscal_quarter, fiscal_label, period_type in (
            (4, "FY2026 Q4", "quarter"),
            (None, "FY2026", "annual"),
        ):
            cur.execute(
                """
                INSERT INTO financial_facts (
                    company_id, statement, concept, value, unit,
                    fiscal_year, fiscal_quarter, fiscal_period_label,
                    period_end, period_type,
                    calendar_year, calendar_quarter, calendar_period_label,
                    published_at, source_raw_response_id, extraction_version,
                    ingest_run_id
                ) VALUES (
                    %s, 'income_statement', 'revenue', 68127000000, 'USD',
                    2026, %s, %s,
                    DATE '2026-01-25', %s,
                    2026, 1, 'CY2026 Q1',
                    %s, %s, 'fmp-is-v1',
                    %s
                );
                """,
                (
                    company_id,
                    fiscal_quarter,
                    fiscal_label,
                    period_type,
                    published_at,
                    raw_response_id,
                    ingest_run_id,
                ),
            )


def _fake_fmp_get(self, endpoint: str, **params) -> Response:  # noqa: ARG001
    if endpoint == "revenue-product-segmentation":
        data = {"Data Center": 62314000000, "Gaming": 3727000000}
    elif endpoint == "revenue-geographic-segmentation":
        data = {"UNITED STATES": 66231000000, "TAIWAN, PROVINCE OF CHINA": 12907000000}
    else:
        raise AssertionError(f"unexpected endpoint: {endpoint}")

    period = params["period"]
    row = {
        "symbol": "NVDA",
        "fiscalYear": 2026,
        "period": "FY" if period == "annual" else "Q4",
        "reportedCurrency": None if endpoint == "revenue-product-segmentation" else "USD",
        "date": "2026-01-25",
        "data": data,
    }
    body = json.dumps([row]).encode()
    return Response(
        status=200,
        body=body,
        content_type="application/json",
        headers={"content-type": "application/json"},
        url=f"https://financialmodelingprep.com/stable/{endpoint}?symbol=NVDA&period={period}",
    )


def test_backfill_fmp_segments_writes_dimensioned_facts() -> None:
    from arrow.agents.fmp_segments import backfill_fmp_segments

    with get_conn() as conn:
        _reset(conn)
        company_id = _seed_nvda(conn)
        run_id = _seed_run(conn)
        raw_response_id = _seed_raw_response(conn, run_id)
        _seed_income_revenue(
            conn,
            company_id=company_id,
            raw_response_id=raw_response_id,
            ingest_run_id=run_id,
        )

        with patch("arrow.ingest.fmp.client.FMPClient.get", new=_fake_fmp_get):
            counts = backfill_fmp_segments(conn, ["NVDA"], since_date=date(2026, 1, 1))

        assert counts["raw_responses"] == 4
        assert counts["rows_processed"] == 4
        assert counts["segments_processed"] == 8
        assert counts["facts_written"] == 8

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT dimension_type, dimension_key, dimension_label,
                       dimension_source, period_type, unit, published_at
                FROM financial_facts
                WHERE statement = 'segment'
                ORDER BY dimension_type, dimension_key, period_type;
                """
            )
            rows = cur.fetchall()

        assert (
            "product",
            "data_center",
            "Data Center",
            "fmp:revenue-product-segmentation",
            "annual",
            "USD",
            datetime(2026, 2, 25, 16, 42, 19, tzinfo=timezone.utc),
        ) in rows
        assert (
            "geography",
            "taiwan_province_of_china",
            "TAIWAN, PROVINCE OF CHINA",
            "fmp:revenue-geographic-segmentation",
            "quarter",
            "USD",
            datetime(2026, 2, 25, 16, 42, 19, tzinfo=timezone.utc),
        ) in rows
