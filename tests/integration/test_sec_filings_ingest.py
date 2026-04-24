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
from arrow.ingest.sec.filings import ingest_recent_sec_filings, ingest_sec_filings
from arrow.ingest.sec.qualitative import (
    CHUNKER_VERSION,
    EXTRACTOR_VERSION,
    TEXT_CHUNKER_VERSION,
    TEXT_UNIT_EXTRACTOR_VERSION,
)


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
    ten_q_html = b"""
    <html><body>
      <div>TABLE OF CONTENTS</div>
      <div>Item 2. Management's Discussion and Analysis ........ 11</div>
      <div>Part I</div>
      <h2>Item 2. Management's Discussion and Analysis of Financial Condition and Results of Operations</h2>
      <p>Revenue accelerated materially in the quarter.</p>
      <h2>Item 3. Quantitative and Qualitative Disclosures About Market Risk</h2>
      <p>Market risk disclosure text.</p>
      <div>Part II</div>
      <h2>Item 1A. Risk Factors</h2>
      <p>Risk factor disclosure text.</p>
    </body></html>
    """

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
                SELECT artifact_type, source_document_id, ticker, company_id,
                       form_family, fiscal_period_key, cik, accession_number,
                       raw_primary_doc_path, effective_at = published_at,
                       fiscal_year, fiscal_quarter, fiscal_period_label,
                       period_end, period_type,
                       calendar_year, calendar_quarter, calendar_period_label,
                       artifact_metadata->>'form_type',
                       artifact_metadata->>'primary_document'
                FROM artifacts;
                """
            )
            row = cur.fetchone()
            assert row == (
                "10q",
                "0001045810-26-000111",
                "NVDA",
                1,
                "10-Q",
                "FY2026 Q3",
                "0001045810",
                "0001045810-26-000111",
                "data/raw/sec/filings/0001045810/0001045810-26-000111/nvda-20251026x10q.htm",
                True,
                2026,
                3,
                "FY2026 Q3",
                date(2025, 10, 26),
                "quarter",
                2025,
                4,
                "CY2025 Q4",
                "10-Q",
                "nvda-20251026x10q.htm",
            )
            cur.execute(
                """
                SELECT section_key, extraction_method
                FROM artifact_sections
                ORDER BY section_key;
                """
            )
            assert cur.fetchall() == [
                ("part1_item2_mda", "deterministic"),
                ("part1_item3_market_risk", "deterministic"),
                ("part2_item1a_risk_factors", "deterministic"),
            ]
            cur.execute("SELECT count(*) FROM artifact_section_chunks;")
            assert cur.fetchone()[0] >= 3
            cur.execute("SELECT id FROM artifacts;")
            artifact_id = cur.fetchone()[0]
            cur.execute("DELETE FROM artifact_sections WHERE artifact_id = %s;", (artifact_id,))
            conn.commit()

        with patch("arrow.ingest.common.http.HttpClient.get", new=_fake_get):
            second = ingest_recent_sec_filings(conn, ["NVDA"])

        assert second["artifacts_written"] == 0
        assert second["artifacts_existing"] == 1
        assert second["sections_written"] == 1
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT section_key, extraction_method
                FROM artifact_sections
                ORDER BY section_key;
                """
            )
            assert cur.fetchall() == [
                ("part1_item2_mda", "deterministic"),
                ("part1_item3_market_risk", "deterministic"),
                ("part2_item1a_risk_factors", "deterministic"),
            ]
            cur.execute(
                "UPDATE artifact_sections SET extractor_version = 'sec_sections_legacy';"
            )
            conn.commit()

        with patch("arrow.ingest.common.http.HttpClient.get", new=_fake_get):
            third = ingest_recent_sec_filings(conn, ["NVDA"])

        assert third["artifacts_written"] == 0
        assert third["artifacts_existing"] == 1
        assert third["sections_written"] == 1
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT extractor_version FROM artifact_sections;")
            assert cur.fetchall() == [(EXTRACTOR_VERSION,)]
            cur.execute(
                "UPDATE artifact_section_chunks SET chunker_version = 'sec_chunks_legacy';"
            )
            conn.commit()

        with patch("arrow.ingest.common.http.HttpClient.get", new=_fake_get):
            fourth = ingest_recent_sec_filings(conn, ["NVDA"])

        assert fourth["artifacts_written"] == 0
        assert fourth["artifacts_existing"] == 1
        assert fourth["sections_written"] == 1
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT chunker_version FROM artifact_section_chunks;")
            assert cur.fetchall() == [(CHUNKER_VERSION,)]


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
                {"name": "q126earningsrelease.htm", "type": "text.gif", "description": ""},
            ]
        }
    }
    eight_k_html = b"<html><body>8-K body</body></html>"
    press_release_html = b"""
    <html><body>
      <h1>NVIDIA Announces Quarterly Financial Results</h1>
      <p>Revenue was $30.0 billion and gross margin expanded.</p>
    </body></html>
    """

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
        elif url.endswith("/q126earningsrelease.htm"):
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
        assert first["text_units_written"] == 2
        assert second["artifacts_written"] == 0
        assert second["artifacts_existing"] == 2
        assert second["text_units_written"] == 0

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
            cur.execute(
                """
                SELECT u.unit_key, u.extractor_version, ch.chunker_version, ch.search_text
                FROM artifact_text_units u
                JOIN artifact_text_chunks ch ON ch.text_unit_id = u.id
                JOIN artifacts a ON a.id = u.artifact_id
                WHERE a.artifact_type = 'press_release'
                ORDER BY u.unit_ordinal, ch.chunk_ordinal;
                """
            )
            unit_rows = cur.fetchall()
        assert rows == [
            ("8k", "0001045810-26-000222", "Current report", None),
            (
                "press_release",
                "0001045810-26-000222:q126earningsrelease.htm",
                "NVDA press release",
                "sec_exhibit",
            ),
        ]
        assert unit_rows == [
            (
                "headline",
                TEXT_UNIT_EXTRACTOR_VERSION,
                TEXT_CHUNKER_VERSION,
                "nvidia announces quarterly financial results",
            ),
            (
                "release_body",
                TEXT_UNIT_EXTRACTOR_VERSION,
                TEXT_CHUNKER_VERSION,
                "revenue was $30.0 billion and gross margin expanded.",
            ),
        ]


def test_sec_qualitative_window_includes_pre_since_quarters_to_complete_fiscal_year() -> None:
    submissions = {
        "filings": {
            "recent": {
                "accessionNumber": [
                    "0001045810-20-000065",
                    "0001045810-20-000147",
                    "0001045810-20-000189",
                    "0001045810-21-000010",
                ],
                "form": ["10-Q", "10-Q", "10-Q", "10-K"],
                "filingDate": ["2020-05-21", "2020-08-19", "2020-11-18", "2021-02-26"],
                "reportDate": ["2020-04-26", "2020-07-26", "2020-10-25", "2021-01-31"],
                "primaryDocument": ["q1.htm", "q2.htm", "q3.htm", "k.htm"],
                "primaryDocDescription": ["Form 10-Q", "Form 10-Q", "Form 10-Q", "Form 10-K"],
                "items": ["", "", "", ""],
                "isXBRL": [1, 1, 1, 1],
                "isInlineXBRL": [1, 1, 1, 1],
            }
        }
    }
    index_payloads = {
        "0001045810-20-000065": "q1.htm",
        "0001045810-20-000147": "q2.htm",
        "0001045810-20-000189": "q3.htm",
        "0001045810-21-000010": "k.htm",
    }
    filing_html = b"""
    <html><body>
      <h2>Item 2. Management's Discussion and Analysis</h2>
      <p>Quarterly operating discussion.</p>
      <h2>Item 3. Quantitative and Qualitative Disclosures About Market Risk</h2>
      <p>Market risk text.</p>
    </body></html>
    """

    def _fake_get(self, url: str, params=None) -> Response:  # noqa: ARG001
        if "submissions/CIK0001045810.json" in url:
            body = json.dumps(submissions).encode()
            content_type = "application/json"
        elif url.endswith("/index.json"):
            compact = url.split("/")[-2]
            accession = f"{compact[:10]}-{compact[10:12]}-{compact[12:]}"
            filename = index_payloads[accession]
            body = json.dumps(
                {"directory": {"item": [{"name": filename, "type": "10-Q", "description": "Filing"}]}}
            ).encode()
            content_type = "application/json"
        elif url.endswith(".htm"):
            body = filing_html
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
            counts = ingest_sec_filings(conn, ["NVDA"], since_date=date(2021, 1, 1))

        assert counts["filings_seen"] == 4
        assert counts["artifacts_by_type"] == {"10q": 3, "10k": 1}
        assert counts["min_fiscal_year_by_ticker"] == {"NVDA": 2021}
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT artifact_type, fiscal_period_key
                FROM artifacts
                ORDER BY published_at;
                """
            )
            assert cur.fetchall() == [
                ("10q", "FY2021 Q1"),
                ("10q", "FY2021 Q2"),
                ("10q", "FY2021 Q3"),
                ("10k", "FY2021"),
            ]


def test_ingest_historical_shard_fetches_full_package_files() -> None:
    submissions = {
        "filings": {
            "recent": {
                "accessionNumber": [],
                "form": [],
                "filingDate": [],
                "reportDate": [],
                "acceptanceDateTime": [],
                "primaryDocument": [],
                "primaryDocDescription": [],
                "items": [],
                "isXBRL": [],
                "isInlineXBRL": [],
            },
            "files": [
                {
                    "name": "CIK0001045810-submissions-001.json",
                    "filingCount": 1,
                    "filingFrom": "2018-02-01",
                    "filingTo": "2018-02-01",
                }
            ],
        }
    }
    shard = {
        "accessionNumber": ["0001045810-18-000001"],
        "form": ["10-K"],
        "filingDate": ["2018-02-01"],
        "reportDate": ["2018-01-28"],
        "acceptanceDateTime": ["20180201160101"],
        "primaryDocument": ["nvda-10k.htm"],
        "primaryDocDescription": ["Form 10-K"],
        "items": [""],
        "isXBRL": [1],
        "isInlineXBRL": [1],
    }
    index_payload = {
        "directory": {
            "item": [
                {"name": "nvda-10k.htm", "type": "10-K", "description": "Form 10-K"},
                {"name": "nvda-20180128.xml", "type": "EX-101.INS", "description": "INSTANCE DOCUMENT"},
                {"name": "nvda-20180128.xsd", "type": "EX-101.SCH", "description": "XBRL TAXONOMY EXTENSION SCHEMA DOCUMENT"},
                {"name": "nvda-20180128_cal.xml", "type": "EX-101.CAL", "description": "XBRL TAXONOMY EXTENSION CALCULATION LINKBASE DOCUMENT"},
                {"name": "nvda-20180128_def.xml", "type": "EX-101.DEF", "description": "XBRL TAXONOMY EXTENSION DEFINITION LINKBASE DOCUMENT"},
                {"name": "nvda-20180128_lab.xml", "type": "EX-101.LAB", "description": "XBRL TAXONOMY EXTENSION LABEL LINKBASE DOCUMENT"},
                {"name": "nvda-20180128_pre.xml", "type": "EX-101.PRE", "description": "XBRL TAXONOMY EXTENSION PRESENTATION LINKBASE DOCUMENT"},
            ]
        }
    }

    def _fake_get(self, url: str, params=None) -> Response:  # noqa: ARG001
        if "submissions/CIK0001045810.json" in url:
            body = json.dumps(submissions).encode()
            content_type = "application/json"
        elif url.endswith("/CIK0001045810-submissions-001.json"):
            body = json.dumps(shard).encode()
            content_type = "application/json"
        elif url.endswith("/index.json"):
            body = json.dumps(index_payload).encode()
            content_type = "application/json"
        elif url.endswith(".htm"):
            body = b"<html><body>10-K body</body></html>"
            content_type = "text/html"
        elif url.endswith(".xml") or url.endswith(".xsd"):
            body = b"<xml />"
            content_type = "application/xml"
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
            counts = ingest_sec_filings(conn, ["NVDA"], since_date=date(2016, 1, 1))

        assert counts["raw_responses"] == 4
        assert counts["filings_seen"] == 1
        assert counts["documents_fetched"] == 1
        assert counts["files_fetched"] == 1
        assert counts["artifacts_written"] == 1

        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM raw_responses WHERE vendor = 'sec';")
            assert cur.fetchone()[0] == 4
            cur.execute(
                """
                SELECT count(*) FROM raw_responses
                WHERE endpoint LIKE 'filings/0001045810/0001045810-18-000001/%';
                """
            )
            assert cur.fetchone()[0] == 2  # index.json + primary filing doc
            cur.execute(
                """
                SELECT artifact_type, source_document_id, fiscal_year, fiscal_quarter, form_family
                FROM artifacts;
                """
            )
            assert cur.fetchone() == ("10k", "0001045810-18-000001", 2018, None, "10-K")


def test_ingest_amendment_links_to_base_filing_without_superseding_sections() -> None:
    submissions = {
        "filings": {
            "recent": {
                "accessionNumber": ["0001045810-26-000111", "0001045810-26-000222"],
                "form": ["10-Q", "10-Q/A"],
                "filingDate": ["2025-11-19", "2025-12-01"],
                "reportDate": ["2025-10-26", "2025-10-26"],
                "acceptanceDateTime": ["20251119120000", "20251201120000"],
                "primaryDocument": ["nvda-20251026x10q.htm", "nvda-20251026x10qa.htm"],
                "primaryDocDescription": ["Form 10-Q", "Form 10-Q/A"],
                "items": ["", ""],
                "isXBRL": [1, 1],
                "isInlineXBRL": [1, 1],
            }
        }
    }
    index_payloads = {
        "0001045810-26-000111": {
            "directory": {
                "item": [
                    {"name": "nvda-20251026x10q.htm", "type": "10-Q", "description": "Form 10-Q"}
                ]
            }
        },
        "0001045810-26-000222": {
            "directory": {
                "item": [
                    {"name": "nvda-20251026x10qa.htm", "type": "10-Q/A", "description": "Form 10-Q/A"}
                ]
            }
        },
    }
    documents = {
        "nvda-20251026x10q.htm": b"""
        <html><body>
          <div>Part I</div>
          <h2>Item 2. Management's Discussion and Analysis of Financial Condition and Results of Operations</h2>
          <p>Base MD&A text.</p>
          <div>Part II</div>
          <h2>Item 1A. Risk Factors</h2>
          <p>Base risk text.</p>
        </body></html>
        """,
        "nvda-20251026x10qa.htm": b"""
        <html><body>
          <div>Part I</div>
          <h2>Item 2. Management's Discussion and Analysis of Financial Condition and Results of Operations</h2>
          <p>Amended MD&A text.</p>
        </body></html>
        """,
    }

    def _fake_get(self, url: str, params=None) -> Response:  # noqa: ARG001
        if "submissions/CIK0001045810.json" in url:
            body = json.dumps(submissions).encode()
            content_type = "application/json"
        elif url.endswith("/index.json"):
            compact = url.split("/")[-2]
            accession = (
                f"{compact[:10]}-{compact[10:12]}-{compact[12:]}"
                if "-" not in compact
                else compact
            )
            body = json.dumps(index_payloads[accession]).encode()
            content_type = "application/json"
        else:
            filename = url.rsplit("/", 1)[-1]
            if filename not in documents:
                raise AssertionError(f"unexpected URL: {url}")
            body = documents[filename]
            content_type = "text/html"
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
            counts = ingest_recent_sec_filings(conn, ["NVDA"], forms=("10-Q", "10-Q/A"), limit_per_ticker=10)

        assert counts["artifacts_written"] == 2
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT accession_number, amends_artifact_id, supersedes
                FROM artifacts
                ORDER BY published_at;
                """
            )
            rows = cur.fetchall()
            assert rows[0] == ("0001045810-26-000111", None, None)
            assert rows[1][0] == "0001045810-26-000222"
            assert rows[1][1] is not None
            assert rows[1][2] is None

            cur.execute(
                """
                SELECT a.accession_number, s.section_key, s.text
                FROM artifact_sections s
                JOIN artifacts a ON a.id = s.artifact_id
                WHERE s.section_key = 'part1_item2_mda'
                ORDER BY a.published_at;
                """
            )
            assert cur.fetchall() == [
                ("0001045810-26-000111", "part1_item2_mda", "Item 2. Management's Discussion and Analysis of Financial Condition and Results of Operations\n\nBase MD&A text."),
                ("0001045810-26-000222", "part1_item2_mda", "Item 2. Management's Discussion and Analysis of Financial Condition and Results of Operations\n\nAmended MD&A text."),
            ]
