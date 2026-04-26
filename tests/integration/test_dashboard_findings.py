"""Integration tests for the dashboard /findings pane and lifecycle POSTs.

Covers:
  - GET /findings list (default open status, filters, pagination cap)
  - GET /findings/{id} detail (404 path, history rendering)
  - POST /findings/{id}/{resolve,suppress,dismiss}
  - Validation: invalid status / severity / suppress without reason / bad date
  - Operator actor capture (no hardcoded names)
  - PRG redirect behavior (POST → 303 → GET)
  - Action errors surface as 400 (e.g. closing already-closed finding)
  - Topbar flag chip points at filtered findings (no broken link from dash)

Uses FastAPI's TestClient (httpx-backed) against the configured test DB.
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone

import psycopg
import pytest
from fastapi.testclient import TestClient

from arrow.db.connection import get_conn
from arrow.db.migrations import apply as apply_migrations
from arrow.steward.actions import (
    open_finding,
    suppress_finding,
)
from arrow.steward.fingerprint import fingerprint

# Import the dashboard module under its FastAPI ASGI name. The lifespan
# hook applies views on startup; we want it to do that against the test DB.
from scripts.dashboard import app


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _reset(conn: psycopg.Connection) -> None:
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE;")
        cur.execute("CREATE SCHEMA public;")
    apply_migrations(conn)


def _seed_company(conn: psycopg.Connection, *, ticker: str = "TEST", cik: int = 9999) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO companies (cik, ticker, name, fiscal_year_end_md) "
            "VALUES (%s, %s, %s, '12-31') RETURNING id;",
            (cik, ticker, f"{ticker} Inc."),
        )
        return cur.fetchone()[0]


def _seed_finding(
    conn: psycopg.Connection,
    *,
    company_id: int,
    ticker: str,
    severity: str = "warning",
    finding_type: str = "test_check",
    summary: str = "test summary",
    fp_seed: str = "default",
) -> int:
    fp = fingerprint(finding_type, {"seed": fp_seed, "ticker": ticker}, {})
    ref = open_finding(
        conn,
        fingerprint=fp,
        finding_type=finding_type,
        severity=severity,
        company_id=company_id,
        ticker=ticker,
        vertical="financials",
        fiscal_period_key="FY2024",
        source_check=finding_type,
        evidence={"seed": fp_seed},
        summary=summary,
        suggested_action={
            "kind": "test",
            "params": {},
            "command": "echo test",
            "prose": "test prose",
        },
        actor="system:check_runner",
    )
    return ref.id


@pytest.fixture
def client():
    """A TestClient instance. The lifespan hook applies the view stack
    on startup, so the test DB must already have migrations applied
    (handled by tests/conftest.py)."""
    with TestClient(app) as c:
        yield c


# ---------------------------------------------------------------------------
# GET /findings list
# ---------------------------------------------------------------------------


def test_findings_list_renders_with_no_findings(client) -> None:
    with get_conn() as conn:
        _reset(conn)

    resp = client.get("/findings")
    assert resp.status_code == 200
    assert "Inbox empty" in resp.text or "No findings" in resp.text
    # Topbar present
    assert "Findings" in resp.text


def test_findings_list_shows_open_findings_default(client) -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="PLTR")
        fid = _seed_finding(
            conn, company_id=cid, ticker="PLTR",
            summary="zero-row run for PLTR", fp_seed="zr1",
        )

    resp = client.get("/findings")
    assert resp.status_code == 200
    assert "PLTR" in resp.text
    assert "zero-row run for PLTR" in resp.text
    assert f"#{fid}" in resp.text
    assert f"/findings/{fid}" in resp.text  # link to detail


def test_findings_list_default_excludes_closed(client) -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="MSFT")
        fid = _seed_finding(
            conn, company_id=cid, ticker="MSFT",
            summary="closed in another life", fp_seed="closed1",
        )
        suppress_finding(
            conn, fid, actor="human:test",
            reason="known", expires=None,
        )

    resp = client.get("/findings")  # default status=open
    assert "closed in another life" not in resp.text


def test_findings_list_status_closed_includes_closed(client) -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="MSFT")
        fid = _seed_finding(
            conn, company_id=cid, ticker="MSFT",
            summary="closed in another life", fp_seed="closed2",
        )
        suppress_finding(
            conn, fid, actor="human:test",
            reason="known", expires=None,
        )

    resp = client.get("/findings?status=closed")
    assert resp.status_code == 200
    assert "closed in another life" in resp.text


def test_findings_list_ticker_filter(client) -> None:
    with get_conn() as conn:
        _reset(conn)
        cid_a = _seed_company(conn, ticker="PLTR", cik=1001)
        cid_b = _seed_company(conn, ticker="MSFT", cik=1002)
        _seed_finding(conn, company_id=cid_a, ticker="PLTR",
                      summary="PLTR-only finding", fp_seed="t1")
        _seed_finding(conn, company_id=cid_b, ticker="MSFT",
                      summary="MSFT-only finding", fp_seed="t2")

    resp = client.get("/findings?ticker=pltr")  # case-insensitive
    assert "PLTR-only finding" in resp.text
    assert "MSFT-only finding" not in resp.text


def test_findings_list_severity_filter(client) -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="PLTR")
        _seed_finding(conn, company_id=cid, ticker="PLTR",
                      severity="warning", summary="warn one", fp_seed="s1")
        _seed_finding(conn, company_id=cid, ticker="PLTR",
                      severity="investigate", summary="investigate one", fp_seed="s2")

    resp = client.get("/findings?severity=investigate")
    assert "investigate one" in resp.text
    assert "warn one" not in resp.text


def test_findings_list_invalid_status_returns_400(client) -> None:
    resp = client.get("/findings?status=bogus")
    assert resp.status_code == 400


def test_findings_list_invalid_severity_returns_400(client) -> None:
    resp = client.get("/findings?severity=critical")
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# GET /findings/{id} detail
# ---------------------------------------------------------------------------


def test_finding_detail_renders_open_finding(client) -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="PLTR")
        fid = _seed_finding(conn, company_id=cid, ticker="PLTR",
                            summary="hello detail", fp_seed="d1")

    resp = client.get(f"/findings/{fid}")
    assert resp.status_code == 200
    assert "hello detail" in resp.text
    assert "Suggested action" in resp.text
    assert "test prose" in resp.text  # from suggested_action
    assert "Resolve" in resp.text  # action buttons present on open
    assert "Suppress" in resp.text
    assert "Dismiss" in resp.text
    assert "History" in resp.text


def test_finding_detail_404_when_missing(client) -> None:
    resp = client.get("/findings/999999")
    assert resp.status_code == 404


def test_finding_detail_prefills_note_inputs_for_each_lifecycle_action(client) -> None:
    """The Resolve / Suppress / Dismiss forms must pre-fill their note
    inputs with structured Action / Cause / Expected templates derived
    from the finding's suggested_action.prose.

    Why this matters for V2 training:
      The operator isn't expected to author technical notes from blank
      inputs (they've stated they rely on Claude for the database
      depth). Pre-fill turns 'authoring' into 'approving' — the
      operator reads, optionally edits, clicks. The audit trail still
      captures them as the actor; V2 trains on consistent structured
      notes instead of empty / hand-wave free text.

    This test would have failed against the prior implementation
    (plain text inputs with placeholder='', empty value).
    """
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="PLTR")
        fid = _seed_finding(conn, company_id=cid, ticker="PLTR",
                            summary="prefill check", fp_seed="prefill1")

    resp = client.get(f"/findings/{fid}")
    assert resp.status_code == 200
    # All three lifecycle forms should have textareas (not bare inputs)
    # with non-empty pre-filled content following the Action/Cause/Expected
    # shape.
    for marker in ("Action:", "Cause:", "Expected:"):
        # Each marker should appear at least three times: once per
        # lifecycle form (resolve / suppress / dismiss).
        assert resp.text.count(marker) >= 3, (
            f"{marker!r} appears {resp.text.count(marker)}× — expected ≥3 "
            f"(one per lifecycle form). Pre-fill regression."
        )
    # Verify structured templates landed in textareas, not plain inputs.
    assert "<textarea" in resp.text


def test_finding_detail_omits_action_buttons_when_closed(client) -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="PLTR")
        fid = _seed_finding(conn, company_id=cid, ticker="PLTR",
                            summary="closed detail", fp_seed="d2")
        suppress_finding(
            conn, fid, actor="human:test",
            reason="known issue", expires=None,
        )

    resp = client.get(f"/findings/{fid}")
    assert resp.status_code == 200
    # The lifecycle action forms shouldn't appear on a closed finding —
    # they're guarded by {% if f.status == 'open' %} in the template.
    # Check by looking for the Suppress *button*, not the literal word
    # (which appears in the closed-reason text "(suppressed)").
    assert "btn-suppress" not in resp.text


# ---------------------------------------------------------------------------
# POST /findings/{id}/resolve
# ---------------------------------------------------------------------------


def test_post_resolve_transitions_finding_and_redirects(client) -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="PLTR")
        fid = _seed_finding(conn, company_id=cid, ticker="PLTR",
                            summary="to be resolved", fp_seed="r1")

    resp = client.post(
        f"/findings/{fid}/resolve",
        data={"note": "ran reingest"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert resp.headers["location"] == f"/findings/{fid}"

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT status, closed_reason, closed_by, closed_note "
            "FROM data_quality_findings WHERE id = %s;",
            (fid,),
        )
        status, reason, closed_by, note = cur.fetchone()
    assert status == "closed"
    assert reason == "resolved"
    assert closed_by.startswith("human:")
    assert closed_by.endswith(":dashboard"), (
        f"dashboard actor must end in ':dashboard' for audit traceability, got {closed_by!r}"
    )
    assert note == "ran reingest"


def test_post_resolve_does_not_hardcode_operator_name(client) -> None:
    """Regression test for the prior cheat where the CLI hardcoded
    'human:michael'. The dashboard reads $USER for actor; this test
    ensures it doesn't fall back to a baked-in name."""
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="PLTR")
        fid = _seed_finding(conn, company_id=cid, ticker="PLTR",
                            summary="actor check", fp_seed="r2")

    resp = client.post(
        f"/findings/{fid}/resolve",
        data={"note": ""},
        follow_redirects=False,
    )
    assert resp.status_code == 303

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT closed_by FROM data_quality_findings WHERE id = %s;", (fid,))
        closed_by = cur.fetchone()[0]

    user = os.environ.get("USER", "").strip()
    expected = f"human:{user}:dashboard" if user else "human:dashboard"
    assert closed_by == expected, (
        f"actor leak: got {closed_by!r}, expected {expected!r}. "
        f"This catches any regression to a hardcoded operator name."
    )


def test_post_resolve_already_closed_returns_400(client) -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="PLTR")
        fid = _seed_finding(conn, company_id=cid, ticker="PLTR",
                            summary="double close", fp_seed="r3")

    # First resolve: succeeds.
    r1 = client.post(f"/findings/{fid}/resolve", data={"note": "first"},
                     follow_redirects=False)
    assert r1.status_code == 303

    # Second resolve on the same finding: action callable raises
    # StewardActionError, which the route surfaces as 400.
    r2 = client.post(f"/findings/{fid}/resolve", data={"note": "second"},
                     follow_redirects=False)
    assert r2.status_code == 400


# ---------------------------------------------------------------------------
# POST /findings/{id}/suppress
# ---------------------------------------------------------------------------


def test_post_suppress_with_reason_and_no_expiry(client) -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="AVGO")
        fid = _seed_finding(conn, company_id=cid, ticker="AVGO",
                            summary="suppress me", fp_seed="sup1")

    resp = client.post(
        f"/findings/{fid}/suppress",
        data={"reason": "AVGO segment reorg confirmed", "expires": ""},
        follow_redirects=False,
    )
    assert resp.status_code == 303

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT status, closed_reason, closed_note, suppressed_until "
            "FROM data_quality_findings WHERE id = %s;",
            (fid,),
        )
        status, reason, note, suppressed_until = cur.fetchone()
    assert status == "closed"
    assert reason == "suppressed"
    assert note == "AVGO segment reorg confirmed"
    assert suppressed_until is None


def test_post_suppress_with_expiry_date(client) -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="AVGO")
        fid = _seed_finding(conn, company_id=cid, ticker="AVGO",
                            summary="suppress me", fp_seed="sup2")

    expires = (date.today() + timedelta(days=180)).isoformat()
    resp = client.post(
        f"/findings/{fid}/suppress",
        data={"reason": "temporary", "expires": expires},
        follow_redirects=False,
    )
    assert resp.status_code == 303

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT suppressed_until::date FROM data_quality_findings WHERE id = %s;",
            (fid,),
        )
        suppressed_until = cur.fetchone()[0]
    assert suppressed_until.isoformat() == expires


def test_post_suppress_without_reason_returns_400(client) -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="AVGO")
        fid = _seed_finding(conn, company_id=cid, ticker="AVGO",
                            summary="suppress me", fp_seed="sup3")

    # Empty string reason — route validates BEFORE the action callable.
    resp = client.post(
        f"/findings/{fid}/suppress",
        data={"reason": "   ", "expires": ""},
        follow_redirects=False,
    )
    assert resp.status_code == 400

    # Missing reason field altogether — FastAPI Form(...) returns 422.
    resp2 = client.post(
        f"/findings/{fid}/suppress",
        data={"expires": ""},
        follow_redirects=False,
    )
    assert resp2.status_code in (400, 422)


def test_post_suppress_with_invalid_expiry_date_returns_400(client) -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="AVGO")
        fid = _seed_finding(conn, company_id=cid, ticker="AVGO",
                            summary="suppress me", fp_seed="sup4")

    resp = client.post(
        f"/findings/{fid}/suppress",
        data={"reason": "x", "expires": "not-a-date"},
        follow_redirects=False,
    )
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# POST /findings/{id}/dismiss
# ---------------------------------------------------------------------------


def test_post_dismiss_transitions_finding(client) -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="PLTR")
        fid = _seed_finding(conn, company_id=cid, ticker="PLTR",
                            summary="false positive", fp_seed="dis1")

    resp = client.post(
        f"/findings/{fid}/dismiss",
        data={"note": "extractor regex was correct"},
        follow_redirects=False,
    )
    assert resp.status_code == 303

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT status, closed_reason, closed_note "
            "FROM data_quality_findings WHERE id = %s;",
            (fid,),
        )
        status, reason, note = cur.fetchone()
    assert status == "closed"
    assert reason == "dismissed"
    assert note == "extractor regex was correct"


# ---------------------------------------------------------------------------
# Cross-page linking (regression: the topbar flag chip is now a link)
# ---------------------------------------------------------------------------


def test_dashboard_ticker_topbar_links_to_findings(client) -> None:
    """The ticker dashboard's flag chip used to be plain text. Now it
    links to /findings filtered to that ticker. Even the no-data
    landing keeps the topbar so operators are never stranded
    without nav."""
    with get_conn() as conn:
        _reset(conn)
        _seed_company(conn, ticker="PLTR")

    # _reset cascade-dropped the view stack; routes that read views
    # (like /t/{ticker}) need them reapplied.
    from scripts.apply_views import main as apply_views_main
    apply_views_main()

    resp = client.get("/t/PLTR")
    # No facts seeded → the no-data path renders, but it now carries
    # the topbar with the Findings link (regression check for the
    # prior bare-HTML empty state that lost all nav).
    assert resp.status_code == 200
    assert "Findings" in resp.text
    assert "/findings?status=open" in resp.text
