"""Integration tests for SEC filing/document ingest.

Real Postgres, mocked SEC HTTP. Verifies recent submissions -> raw_responses
+ artifacts for 10-Q / 8-K paths.
"""

from __future__ import annotations

import json
from datetime import date
from unittest.mock import patch

from arrow.db.connection import get_conn
from arrow.db.migrations import apply
from arrow.ingest.common.http import Response
from arrow.ingest.sec.filings import ingest_recent_sec_filings


def _reset(conn) -> None:
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE;")
        cur.execute("CREATE SCHEMA public;")
    apply(conn)
    conn.autocommit = False


def _seed_nvda(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO companies (cik, ticker, name, fiscal_year_end_md)
            VALUES (1045810, 'NVDA', 'NVIDIA CORP', '01-31')
            RETURNING id;
            """,
        )
        cid = cur.fetchone()[0]
    conn.commit()
    return cid


def test_ingest_recent_10q_writes_raw_and_artifact() -> None:
    submissions = {
        "filings": {
            "recent": {
                "accessionNumber": ["0001045810-26-000111"],
                "form": ["10-Q"],
                "filingDate": ["2025-11-19"],
                "reportDate": ["2025-10-26"],
                "primaryDocument": ["nvda-20251026x10q.htm"],
                "primaryDocDescription": ["Form 10-Q"],
                "items": [""],
                "isXBRL": [1],
                "isInlineXBRL": [1],
            }
        }
    }
    index_payload = {
        "directory": {
            "item": [
                {"name": "nvda-20251026x10q.htm", "type": "10-Q", "description": "Form 10-Q"}
            ]
        }
    }
    ten_q_html = b"<html><body>NVDA 10-Q</body></html>"

    def _fake_get(self, url: str, params=None) -> Response:  # noqa: ARG001
        if "submissions/CIK0001045810.json" in url:
            body = json.dumps(submissions).encode()
            content_type = "application/json"
        elif url.endswith("/index.json"):
            body = json.dumps(index_payload).encode()
            content_type = "application/json"
        elif url.endswith("/nvda-20251026x10q.htm"):
            body = ten_q_html
            content_type = "text/html"
        else:
            raise AssertionError(f"unexpected URL: {url}")
        return Response(
            status=200,
            body=body,
            content_type=content_type,
            headers={"content-type": content_type},
            url=url,
        )

    with get_conn() as conn:
        _reset(conn)
        _seed_nvda(conn)
        with patch("arrow.ingest.common.http.HttpClient.get", new=_fake_get):
            counts = ingest_recent_sec_filings(conn, ["NVDA"])

        assert counts["raw_responses"] == 3
        assert counts["filings_seen"] == 1
        assert counts["artifacts_written"] == 1
        assert counts["artifacts_existing"] == 0

        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM raw_responses WHERE vendor = 'sec';")
            assert cur.fetchone()[0] == 3
            cur.execute(
                """
                SELECT artifact_type, source_document_id, ticker,
                       fiscal_year, fiscal_quarter, fiscal_period_label,
                       period_end, period_type,
                       calendar_year, calendar_quarter, calendar_period_label,
                       artifact_metadata->>'accession_number',
                       artifact_metadata->>'form_type',
                       artifact_metadata->>'filer_cik'
                FROM artifacts;
                """
            )
            row = cur.fetchone()
            assert row == (
                "10q",
                "0001045810-26-000111",
                "NVDA",
                2026,
                3,
                "FY2026 Q3",
                date(2025, 10, 26),
                "quarter",
                2025,
                4,
                "CY2025 Q4",
                "0001045810-26-000111",
                "10-Q",
                "0001045810",
            )


def test_ingest_recent_8k_writes_primary_and_press_release_and_dedupes() -> None:
    submissions = {
        "filings": {
            "recent": {
                "accessionNumber": ["0001045810-26-000222"],
                "form": ["8-K"],
                "filingDate": ["2026-04-01"],
                "reportDate": ["2026-04-01"],
                "primaryDocument": ["nvda-8k.htm"],
                "primaryDocDescription": ["Current report"],
                "items": ["2.02,9.01"],
                "isXBRL": [0],
                "isInlineXBRL": [0],
            }
        }
    }
    index_payload = {
        "directory": {
            "item": [
                {"name": "nvda-8k.htm", "type": "8-K", "description": "Current report"},
                {"name": "ex99-1.htm", "type": "EX-99.1", "description": "Earnings release"},
            ]
        }
    }
    eight_k_html = b"<html><body>8-K body</body></html>"
    press_release_html = b"<html><body>Earnings release body</body></html>"

    def _fake_get(self, url: str, params=None) -> Response:  # noqa: ARG001
        if "submissions/CIK0001045810.json" in url:
            body = json.dumps(submissions).encode()
            content_type = "application/json"
        elif url.endswith("/index.json"):
            body = json.dumps(index_payload).encode()
            content_type = "application/json"
        elif url.endswith("/nvda-8k.htm"):
            body = eight_k_html
            content_type = "text/html"
        elif url.endswith("/ex99-1.htm"):
            body = press_release_html
            content_type = "text/html"
        else:
            raise AssertionError(f"unexpected URL: {url}")
        return Response(
            status=200,
            body=body,
            content_type=content_type,
            headers={"content-type": content_type},
            url=url,
        )

    with get_conn() as conn:
        _reset(conn)
        _seed_nvda(conn)
        with patch("arrow.ingest.common.http.HttpClient.get", new=_fake_get):
            first = ingest_recent_sec_filings(conn, ["NVDA"])
            second = ingest_recent_sec_filings(conn, ["NVDA"])

        assert first["raw_responses"] == 4
        assert first["artifacts_written"] == 2
        assert second["artifacts_written"] == 0
        assert second["artifacts_existing"] == 2

        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM raw_responses WHERE vendor = 'sec';")
            assert cur.fetchone()[0] == 8
            cur.execute(
                """
                SELECT artifact_type, source_document_id, title,
                       artifact_metadata->>'distribution_channel'
                FROM artifacts
                ORDER BY artifact_type, source_document_id;
                """
            )
            rows = cur.fetchall()
        assert rows == [
            ("8k", "0001045810-26-000222", "Current report", None),
            (
                "press_release",
                "0001045810-26-000222:ex99-1.htm",
                "Earnings release",
                "sec_exhibit",
            ),
        ]
