"""Integration tests for the dashboard /coverage routes.

Covers:
  - GET /coverage with no companies vs with companies
  - GET /coverage/{ticker} detail
  - 404 on tickers that aren't in `companies`
  - Topbar Coverage link present on existing pages

V1.2 simplification: every ticker in `companies` is automatically
tracked. There is no separate membership step, so:
  - no /coverage/add route or form
  - no /coverage/{ticker}/remove route
  - no add_to_coverage / remove_from_coverage action callables
Tests for the removed routes are not present here.
"""

from __future__ import annotations

import os

import psycopg
import pytest
from fastapi.testclient import TestClient

from arrow.db.connection import get_conn
from arrow.db.migrations import apply as apply_migrations
from scripts.dashboard import app


def _reset(conn: psycopg.Connection) -> None:
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE;")
        cur.execute("CREATE SCHEMA public;")
    apply_migrations(conn)


def _seed_company(conn: psycopg.Connection, *, ticker: str, cik: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO companies (cik, ticker, name, fiscal_year_end_md) "
            "VALUES (%s, %s, %s, '12-31') RETURNING id;",
            (cik, ticker, f"{ticker} Inc."),
        )
        return cur.fetchone()[0]


def _seed_facts(
    conn: psycopg.Connection,
    *,
    company_id: int,
    statement: str,
    concept: str,
    n_periods: int = 4,
) -> None:
    """Seed n_periods quarters of one (statement, concept) for the company.
    Used to make verticals report has_data=True."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ingest_runs (vendor, run_kind, status, started_at, finished_at) "
            "VALUES ('fmp','manual','succeeded', now(), now()) RETURNING id;"
        )
        run_id = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO raw_responses (
                ingest_run_id, vendor, endpoint, params_hash, http_status,
                content_type, body_jsonb, raw_hash, canonical_hash
            ) VALUES (%s,'fmp','x',%s,200,'application/json','{}'::jsonb,%s,%s)
            RETURNING id;
            """,
            (run_id, b"\x00"*32, b"\x00"*32, b"\x00"*32),
        )
        raw_id = cur.fetchone()[0]
        for i in range(n_periods):
            year = 2024
            quarter = (i % 4) + 1
            cur.execute(
                """
                INSERT INTO financial_facts (
                    ingest_run_id, company_id, statement, concept,
                    fiscal_year, fiscal_quarter, fiscal_period_label,
                    period_end, period_type,
                    calendar_year, calendar_quarter, calendar_period_label,
                    value, unit, source_raw_response_id, extraction_version,
                    published_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, 'quarter',
                    %s, %s, %s, %s, 'usd', %s, %s, now()
                )
                """,
                (
                    run_id, company_id, statement, concept,
                    year, quarter, f"FY{year} Q{quarter}",
                    f"{year}-{(quarter*3):02d}-{28 if quarter*3 in (3,9) else 30:02d}",
                    year, quarter, f"CY{year} Q{quarter}",
                    100.0 * (i + 1), raw_id, f"fmp-v{i+1}",
                ),
            )


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c


# ---------------------------------------------------------------------------
# GET /coverage list
# ---------------------------------------------------------------------------


def test_coverage_matrix_renders_empty_with_no_companies(client) -> None:
    with get_conn() as conn:
        _reset(conn)

    resp = client.get("/coverage")
    assert resp.status_code == 200
    assert "No companies seeded yet" in resp.text


def test_coverage_matrix_renders_company_with_vertical_columns(client) -> None:
    """Every company in the database appears in the matrix automatically."""
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="PLTR", cik=1001)
        _seed_facts(conn, company_id=cid, statement="income_statement",
                    concept="revenue", n_periods=4)

    resp = client.get("/coverage")
    assert resp.status_code == 200
    assert "PLTR" in resp.text
    # Vertical columns appear in the header.
    for v in ("financials", "segments", "employees", "sec_qual", "press_release"):
        assert v in resp.text
    # PLTR has financials → cell should be a "yes" cell with check + counts.
    assert "cov-yes" in resp.text
    assert "cov-no" in resp.text  # other verticals are no


def test_coverage_matrix_lists_all_companies_no_membership_step(client) -> None:
    """Every company in the database appears in the matrix — no opt-in."""
    with get_conn() as conn:
        _reset(conn)
        _seed_company(conn, ticker="AMZN", cik=2001)
        _seed_company(conn, ticker="MSFT", cik=2002)
        _seed_company(conn, ticker="PLTR", cik=2003)

    resp = client.get("/coverage")
    for t in ("AMZN", "MSFT", "PLTR"):
        assert t in resp.text


def test_coverage_matrix_does_not_show_add_form_or_remove_buttons(client) -> None:
    """Regression check for V1.2: the previous Add/Remove UI was
    removed when membership became automatic."""
    with get_conn() as conn:
        _reset(conn)
        _seed_company(conn, ticker="PLTR", cik=1001)

    resp = client.get("/coverage")
    # No "Add to coverage" form — adding now means seeding via CLI.
    assert "Add to coverage" not in resp.text
    # The /coverage/add route shouldn't be referenced anywhere.
    assert "/coverage/add" not in resp.text


# ---------------------------------------------------------------------------
# GET /coverage/{ticker}
# ---------------------------------------------------------------------------


def test_coverage_ticker_detail_renders(client) -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="PLTR", cik=1001)
        _seed_facts(conn, company_id=cid, statement="income_statement",
                    concept="revenue", n_periods=4)

    resp = client.get("/coverage/PLTR")
    assert resp.status_code == 200
    assert "PLTR" in resp.text
    assert "Vertical summary" in resp.text
    assert "financials" in resp.text


def test_coverage_ticker_detail_404_when_not_in_companies(client) -> None:
    """Ticker that isn't seeded yet → 404 with hint to run ingest."""
    with get_conn() as conn:
        _reset(conn)

    resp = client.get("/coverage/UNKNOWN")
    assert resp.status_code == 404


def test_coverage_ticker_detail_omits_remove_button(client) -> None:
    """V1.2 regression check: no Remove button on detail page."""
    with get_conn() as conn:
        _reset(conn)
        _seed_company(conn, ticker="PLTR", cik=1001)

    resp = client.get("/coverage/PLTR")
    assert "/coverage/PLTR/remove" not in resp.text
    assert "btn-dismiss" not in resp.text


# ---------------------------------------------------------------------------
# Cross-page nav (Coverage link should appear in topbars)
# ---------------------------------------------------------------------------


def test_coverage_link_appears_in_findings_topbar(client) -> None:
    with get_conn() as conn:
        _reset(conn)

    resp = client.get("/findings")
    assert resp.status_code == 200
    assert 'href="/coverage"' in resp.text


def test_coverage_link_appears_in_no_data_landing(client) -> None:
    with get_conn() as conn:
        _reset(conn)  # No companies → / lands on the no-data page.

    resp = client.get("/", follow_redirects=False)
    if resp.status_code == 200:
        assert 'href="/coverage"' in resp.text
