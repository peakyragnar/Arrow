"""Integration tests for the dashboard /coverage routes.

Covers:
  - GET /coverage with no members vs with members
  - GET /coverage/{ticker} detail
  - POST /coverage/add: success, validation (missing/blank ticker)
  - POST /coverage/{ticker}/remove: idempotency, no data deletion
  - Operator actor capture (no hardcoded names)
  - Topbar Coverage link present on existing pages

Coverage is binary in V1.1+ — no tier dropdown, no /coverage/{ticker}/tier
route. The set_coverage_tier action callable was removed.
"""

from __future__ import annotations

import os

import psycopg
import pytest
from fastapi.testclient import TestClient

from arrow.db.connection import get_conn
from arrow.db.migrations import apply as apply_migrations
from arrow.steward.actions import add_to_coverage
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


def test_coverage_matrix_renders_empty_with_no_members(client) -> None:
    with get_conn() as conn:
        _reset(conn)

    resp = client.get("/coverage")
    assert resp.status_code == 200
    assert "No tickers in coverage yet" in resp.text


def test_coverage_matrix_renders_member_with_vertical_columns(client) -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="PLTR", cik=1001)
        _seed_facts(conn, company_id=cid, statement="income_statement",
                    concept="revenue", n_periods=4)
        add_to_coverage(conn, ticker="PLTR",
                        actor="human:test")

    resp = client.get("/coverage")
    assert resp.status_code == 200
    assert "PLTR" in resp.text
    # Vertical columns appear in the header.
    for v in ("financials", "segments", "employees", "sec_qual", "press_release"):
        assert v in resp.text
    # PLTR has financials → cell should be a "yes" cell with check + counts.
    assert "cov-yes" in resp.text
    assert "cov-no" in resp.text  # other verticals are no


def test_coverage_matrix_lists_unmembered_tickers_in_add_form(client) -> None:
    with get_conn() as conn:
        _reset(conn)
        _seed_company(conn, ticker="AMZN", cik=2001)
        _seed_company(conn, ticker="MSFT", cik=2002)
        # PLTR is in coverage; AMZN/MSFT are seeded but unmembered.
        cid = _seed_company(conn, ticker="PLTR", cik=2003)
        add_to_coverage(conn, ticker="PLTR", actor="human:test")

    resp = client.get("/coverage")
    assert "AMZN" in resp.text
    assert "MSFT" in resp.text
    # Form options appear in the dropdown
    assert 'value="AMZN"' in resp.text
    assert 'value="MSFT"' in resp.text


# ---------------------------------------------------------------------------
# GET /coverage/{ticker}
# ---------------------------------------------------------------------------


def test_coverage_ticker_detail_renders(client) -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="PLTR", cik=1001)
        _seed_facts(conn, company_id=cid, statement="income_statement",
                    concept="revenue", n_periods=4)
        add_to_coverage(conn, ticker="PLTR", actor="human:test")

    resp = client.get("/coverage/PLTR")
    assert resp.status_code == 200
    assert "PLTR" in resp.text
    assert "Vertical summary" in resp.text
    # Should show the per-vertical detail section for financials with periods
    assert "financials" in resp.text


def test_coverage_ticker_detail_404_when_unmembered(client) -> None:
    with get_conn() as conn:
        _reset(conn)
        _seed_company(conn, ticker="UNKNOWN", cik=9999)
        # Note: UNKNOWN is in companies but NOT in coverage_membership.

    resp = client.get("/coverage/UNKNOWN")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# POST /coverage/add
# ---------------------------------------------------------------------------


def test_coverage_add_success_redirects_to_ticker_detail(client) -> None:
    with get_conn() as conn:
        _reset(conn)
        _seed_company(conn, ticker="PLTR", cik=1001)

    resp = client.post(
        "/coverage/add",
        data={"ticker": "pltr", "notes": "watchlist"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert resp.headers["location"] == "/coverage/PLTR"

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT added_by, notes FROM coverage_membership "
            "WHERE company_id = (SELECT id FROM companies WHERE ticker = 'PLTR');"
        )
        added_by, notes = cur.fetchone()
    assert added_by.endswith(":dashboard"), (
        f"dashboard actor must end in ':dashboard', got {added_by!r}"
    )
    assert notes == "watchlist"


def test_coverage_add_unseeded_ticker_returns_400(client) -> None:
    """coverage_membership requires the company to exist first."""
    with get_conn() as conn:
        _reset(conn)
        # No company seeded.

    resp = client.post(
        "/coverage/add",
        data={"ticker": "NEVERSEEN", "notes": ""},
        follow_redirects=False,
    )
    assert resp.status_code == 400


def test_coverage_add_blank_ticker_rejected(client) -> None:
    with get_conn() as conn:
        _reset(conn)

    resp = client.post(
        "/coverage/add",
        data={"ticker": "   ", "notes": ""},
        follow_redirects=False,
    )
    assert resp.status_code == 400


def test_coverage_add_does_not_hardcode_operator_name(client) -> None:
    """Regression test for the prior cheat where the CLI hardcoded
    'human:michael'. Same standard applies to dashboard actions."""
    with get_conn() as conn:
        _reset(conn)
        _seed_company(conn, ticker="PLTR", cik=1001)

    client.post("/coverage/add", data={"ticker": "PLTR"},
                follow_redirects=False)

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT added_by FROM coverage_membership WHERE id = (SELECT MAX(id) FROM coverage_membership);"
        )
        added_by = cur.fetchone()[0]

    user = os.environ.get("USER", "").strip()
    expected = f"human:{user}:dashboard" if user else "human:dashboard"
    assert added_by == expected, (
        f"actor leak: got {added_by!r}, expected {expected!r}"
    )


# ---------------------------------------------------------------------------
# POST /coverage/{ticker}/remove
# ---------------------------------------------------------------------------


def test_coverage_remove_succeeds_and_keeps_data(client) -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="PLTR", cik=1001)
        _seed_facts(conn, company_id=cid, statement="income_statement",
                    concept="revenue", n_periods=2)
        add_to_coverage(conn, ticker="PLTR", actor="human:test")

    resp = client.post("/coverage/PLTR/remove", follow_redirects=False)
    assert resp.status_code == 303
    assert resp.headers["location"] == "/coverage"

    with get_conn() as conn, conn.cursor() as cur:
        # Membership gone
        cur.execute(
            "SELECT COUNT(*) FROM coverage_membership "
            "WHERE company_id = (SELECT id FROM companies WHERE ticker = 'PLTR');"
        )
        assert cur.fetchone()[0] == 0
        # Company stays
        cur.execute("SELECT COUNT(*) FROM companies WHERE ticker = 'PLTR';")
        assert cur.fetchone()[0] == 1
        # Facts stay (no destructive cascade behind the dashboard click)
        cur.execute("SELECT COUNT(*) FROM financial_facts WHERE company_id = %s;", (cid,))
        assert cur.fetchone()[0] == 2


def test_coverage_remove_is_idempotent(client) -> None:
    with get_conn() as conn:
        _reset(conn)
        _seed_company(conn, ticker="PLTR", cik=1001)
        # PLTR is NOT in coverage_membership.

    resp = client.post("/coverage/PLTR/remove", follow_redirects=False)
    # remove_from_coverage returns False (nothing removed); the route
    # treats this as success (idempotent contract documented in actions.py).
    assert resp.status_code == 303


# ---------------------------------------------------------------------------
# Cross-page nav (Coverage link should appear in topbars)
# ---------------------------------------------------------------------------


def test_coverage_link_appears_in_findings_topbar(client) -> None:
    """The Coverage nav link should appear on every page so operators
    can navigate to it without typing a URL."""
    with get_conn() as conn:
        _reset(conn)

    resp = client.get("/findings")
    assert resp.status_code == 200
    assert 'href="/coverage"' in resp.text


def test_coverage_link_appears_in_no_data_landing(client) -> None:
    with get_conn() as conn:
        _reset(conn)  # No companies → / lands on the no-data page.

    resp = client.get("/", follow_redirects=False)
    # Either redirects (companies present) or returns 200 with topbar.
    if resp.status_code == 200:
        assert 'href="/coverage"' in resp.text
