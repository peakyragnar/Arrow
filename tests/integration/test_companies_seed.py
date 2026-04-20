"""Integration test for the SEC companies bootstrap.

Real Postgres (resets public schema, reapplies migrations), mocked SEC
HTTP. Asserts:
  - row shape in companies, raw_responses, ingest_runs
  - idempotency on (cik): re-run updates, never duplicates
  - raw_responses append-only on re-fetch (polling-log semantics)

Warning: DROPs and recreates the `public` schema in DATABASE_URL. Run
only against a dev or dedicated test database.
"""

from __future__ import annotations

import json
from unittest.mock import patch

import psycopg

from arrow.db.connection import get_conn
from arrow.db.migrations import apply
from arrow.ingest.common.http import Response
from arrow.ingest.sec.bootstrap import seed_companies

_COMPANY_TICKERS_FAKE = {
    "0": {"cik_str": 1045810, "ticker": "NVDA", "title": "NVIDIA CORP"},
    "1": {"cik_str": 789019, "ticker": "MSFT", "title": "MICROSOFT CORP"},
}

_NVDA_SUBMISSIONS_FAKE = {
    "cik": "1045810",
    "name": "NVIDIA CORP",
    "fiscalYearEnd": "0126",
}

_MSFT_SUBMISSIONS_FAKE = {
    "cik": "789019",
    "name": "MICROSOFT CORP",
    "fiscalYearEnd": "0630",
}


def _reset(conn: psycopg.Connection) -> None:
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE;")
        cur.execute("CREATE SCHEMA public;")
    apply(conn)
    conn.autocommit = False


def _fake_http_get(self, url: str, params=None) -> Response:  # noqa: ARG001 (self)
    if "company_tickers" in url:
        body = json.dumps(_COMPANY_TICKERS_FAKE).encode()
    elif "submissions/CIK0001045810" in url:
        body = json.dumps(_NVDA_SUBMISSIONS_FAKE).encode()
    elif "submissions/CIK0000789019" in url:
        body = json.dumps(_MSFT_SUBMISSIONS_FAKE).encode()
    else:
        raise AssertionError(f"unexpected URL: {url}")
    return Response(
        status=200,
        body=body,
        content_type="application/json",
        headers={"content-type": "application/json"},
        url=url,
    )


def test_seed_single_ticker_inserts_one_company() -> None:
    with get_conn() as conn:
        _reset(conn)
        with patch(
            "arrow.ingest.common.http.HttpClient.get",
            new=_fake_http_get,
        ):
            seeded = seed_companies(conn, ["NVDA"])

        assert len(seeded) == 1
        s = seeded[0]
        assert s.cik == 1045810
        assert s.ticker == "NVDA"
        assert s.name == "NVIDIA CORP"
        assert s.fiscal_year_end_md == "01-26"
        assert s.id > 0

        with conn.cursor() as cur:
            cur.execute("SELECT cik, ticker, name, fiscal_year_end_md, status FROM companies;")
            rows = cur.fetchall()
            assert rows == [(1045810, "NVDA", "NVIDIA CORP", "01-26", "active")]

            cur.execute("SELECT count(*) FROM raw_responses;")
            assert cur.fetchone()[0] == 2

            cur.execute(
                """
                SELECT status, counts, ticker_scope, run_kind, vendor
                FROM ingest_runs;
                """
            )
            row = cur.fetchone()
            assert row[0] == "succeeded"
            assert row[1] == {"companies": 1, "raw_responses": 2}
            assert row[2] == ["NVDA"]
            assert row[3] == "manual"
            assert row[4] == "sec"


def test_seed_multiple_tickers_inserts_all() -> None:
    with get_conn() as conn:
        _reset(conn)
        with patch(
            "arrow.ingest.common.http.HttpClient.get",
            new=_fake_http_get,
        ):
            seeded = seed_companies(conn, ["NVDA", "MSFT"])

        assert [s.ticker for s in seeded] == ["NVDA", "MSFT"]
        assert [s.fiscal_year_end_md for s in seeded] == ["01-26", "06-30"]

        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM companies;")
            assert cur.fetchone()[0] == 2
            cur.execute("SELECT count(*) FROM raw_responses;")
            # company_tickers.json fetched twice (once per ticker loop iter) +
            # 2 submissions fetches = 4 rows.
            assert cur.fetchone()[0] == 4
            cur.execute("SELECT counts FROM ingest_runs;")
            assert cur.fetchone()[0] == {"companies": 2, "raw_responses": 4}


def test_seed_is_idempotent_on_cik() -> None:
    with get_conn() as conn:
        _reset(conn)
        with patch(
            "arrow.ingest.common.http.HttpClient.get",
            new=_fake_http_get,
        ):
            first = seed_companies(conn, ["NVDA"])
            second = seed_companies(conn, ["NVDA"])

        # Same company row reused.
        assert first[0].id == second[0].id

        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM companies;")
            assert cur.fetchone()[0] == 1
            # Two runs = four raw_responses rows (append-only on re-fetch).
            cur.execute("SELECT count(*) FROM raw_responses;")
            assert cur.fetchone()[0] == 4
            cur.execute("SELECT count(*) FROM ingest_runs;")
            assert cur.fetchone()[0] == 2
            # updated_at should move on the second run.
            cur.execute("SELECT created_at, updated_at FROM companies WHERE cik = 1045810;")
            created, updated = cur.fetchone()
            assert updated >= created


def test_ingest_run_records_failure_on_unknown_ticker() -> None:
    with get_conn() as conn:
        _reset(conn)
        with patch(
            "arrow.ingest.common.http.HttpClient.get",
            new=_fake_http_get,
        ):
            try:
                seed_companies(conn, ["NOT_A_TICKER"])
            except LookupError:
                pass
            else:
                raise AssertionError("expected LookupError for unknown ticker")

        with conn.cursor() as cur:
            cur.execute("SELECT status, error_message FROM ingest_runs;")
            status, msg = cur.fetchone()
            assert status == "failed"
            assert "NOT_A_TICKER" in msg
            cur.execute("SELECT count(*) FROM companies;")
            assert cur.fetchone()[0] == 0


def test_raw_response_has_double_hashes_and_jsonb() -> None:
    with get_conn() as conn:
        _reset(conn)
        with patch(
            "arrow.ingest.common.http.HttpClient.get",
            new=_fake_http_get,
        ):
            seed_companies(conn, ["NVDA"])

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT vendor, endpoint,
                       body_jsonb IS NOT NULL AS json_body,
                       body_raw IS NULL AS no_raw_body,
                       octet_length(raw_hash), octet_length(canonical_hash),
                       octet_length(params_hash)
                FROM raw_responses ORDER BY id;
                """
            )
            rows = cur.fetchall()
        assert len(rows) == 2
        for vendor, endpoint, json_body, no_raw_body, rh, ch, ph in rows:
            assert vendor == "sec"
            assert endpoint.endswith(".json")
            assert json_body is True
            assert no_raw_body is True
            assert rh == 32 and ch == 32 and ph == 32
