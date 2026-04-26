"""Integration tests for the expected_coverage check.

Validates the end-to-end loop: coverage_membership row + actual data
state + expectations → finding-or-no-finding. Covers each rule kind
both succeeding and failing, plus the per-ticker override path and
the suppression-respect path.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import psycopg
import pytest

from arrow.db.connection import get_conn
from arrow.db.migrations import apply as apply_migrations
from arrow.steward.actions import (
    add_to_coverage,
    suppress_finding,
)
from arrow.steward.registry import REGISTRY, Scope
from arrow.steward.runner import run_steward

# Self-register all checks (importing the package fires @register).
import arrow.steward.checks  # noqa: F401


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _reset(conn: psycopg.Connection) -> None:
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE;")
        cur.execute("CREATE SCHEMA public;")
    apply_migrations(conn)


def _seed_company(conn: psycopg.Connection, *, ticker: str, cik: int = 9999) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO companies (cik, ticker, name, fiscal_year_end_md) "
            "VALUES (%s, %s, %s, '12-31') RETURNING id;",
            (cik, ticker, f"{ticker} Inc."),
        )
        return cur.fetchone()[0]


def _seed_run(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ingest_runs (vendor, run_kind, status, started_at, finished_at) "
            "VALUES ('fmp','manual','succeeded', now(), now()) RETURNING id;"
        )
        return cur.fetchone()[0]


def _seed_raw(conn: psycopg.Connection, run_id: int) -> int:
    with conn.cursor() as cur:
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
        return cur.fetchone()[0]


def _seed_quarterly_facts(
    conn: psycopg.Connection,
    *,
    company_id: int,
    statement: str = "income_statement",
    concept: str = "revenue",
    n_periods: int = 4,
    most_recent_period_end: date | None = None,
) -> None:
    """Seed `n_periods` of quarterly financial_facts ending at
    `most_recent_period_end` (default today's end-of-quarter)."""
    if most_recent_period_end is None:
        most_recent_period_end = date.today()
    run_id = _seed_run(conn)
    raw_id = _seed_raw(conn, run_id)
    with conn.cursor() as cur:
        for i in range(n_periods):
            # Walk back i quarters from the most-recent.
            quarters_back = i
            year = most_recent_period_end.year - (quarters_back // 4)
            quarter = ((most_recent_period_end.month - 1) // 3 + 1) - (quarters_back % 4)
            while quarter < 1:
                quarter += 4
                year -= 1
            month_end = quarter * 3
            day_end = 31 if month_end in (3, 12) else 30
            try:
                period_end = date(year, month_end, day_end)
            except ValueError:
                period_end = date(year, month_end, 28)
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
                    period_end,
                    year, quarter, f"CY{year} Q{quarter}",
                    100.0 * (i + 1), raw_id, f"fmp-v{i+1}",
                ),
            )


def _findings(conn: psycopg.Connection) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, ticker, vertical, severity, summary, evidence "
            "FROM data_quality_findings "
            "WHERE source_check = 'expected_coverage' AND status = 'open' "
            "ORDER BY id;"
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Trigger cases
# ---------------------------------------------------------------------------


def test_unmembered_company_yields_no_findings() -> None:
    """Companies not in coverage_membership are not evaluated."""
    with get_conn() as conn:
        _reset(conn)
        _seed_company(conn, ticker="UNMEMBERED")

        run_steward(conn, scope=Scope(check_names=["expected_coverage"]))
        assert _findings(conn) == []


def test_core_member_with_zero_data_fires_for_each_required_vertical() -> None:
    """A bare core member has no facts → fires for every required
    vertical: financials, segments, employees, sec_qual.
    """
    with get_conn() as conn:
        _reset(conn)
        _seed_company(conn, ticker="EMPTY")
        add_to_coverage(conn, ticker="EMPTY", tier="core", actor="human:test")

        run_steward(conn, scope=Scope(check_names=["expected_coverage"]))
        rows = _findings(conn)

    by_vertical = {r["vertical"] for r in rows}
    # All four core verticals should have at least one finding.
    assert "financials" in by_vertical
    assert "segments" in by_vertical
    assert "employees" in by_vertical
    assert "sec_qual" in by_vertical


def test_core_member_with_full_financials_no_financials_finding() -> None:
    """20 quarterly periods of financials clears the financials
    expectation. Other verticals still fire (no segment / employee /
    sec_qual data seeded), but financials is gone."""
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="OK")
        # Need 20+ distinct period_end values. Seed all three statements
        # to make COUNT(DISTINCT period_end) ≥ 20.
        _seed_quarterly_facts(conn, company_id=cid, statement="income_statement",
                              concept="revenue", n_periods=20)
        add_to_coverage(conn, ticker="OK", tier="core", actor="human:test")

        run_steward(conn, scope=Scope(check_names=["expected_coverage"]))
        rows = _findings(conn)

    verticals_failing = {r["vertical"] for r in rows}
    assert "financials" not in verticals_failing
    # Other verticals still fail because no data was seeded for them.
    assert "segments" in verticals_failing


def test_extended_member_lighter_threshold() -> None:
    """Extended tier expects only 8 quarterly periods. Seed 10 → no
    financials finding."""
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="EXT")
        _seed_quarterly_facts(conn, company_id=cid, n_periods=10)
        add_to_coverage(conn, ticker="EXT", tier="extended", actor="human:test")

        run_steward(conn, scope=Scope(check_names=["expected_coverage"]))
        rows = _findings(conn)

    verticals_failing = {r["vertical"] for r in rows}
    # Extended tier defaults: financials min_periods=8, sec_qual present.
    # 10 periods → financials passes.
    assert "financials" not in verticals_failing
    # sec_qual not seeded → present check fails.
    assert "sec_qual" in verticals_failing


def test_per_ticker_override_relaxes_threshold() -> None:
    """CRWV's override sets financials min_periods=4 (vs core default 20).
    With 5 periods it passes; without override it would fail."""
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="CRWV", cik=4242)
        _seed_quarterly_facts(conn, company_id=cid, n_periods=5)
        add_to_coverage(conn, ticker="CRWV", tier="core", actor="human:test")

        run_steward(conn, scope=Scope(check_names=["expected_coverage"]))
        rows = _findings(conn)

    verticals_failing = {r["vertical"] for r in rows}
    # CRWV's financials override (count=4) means 5 ≥ 4 → financials passes.
    assert "financials" not in verticals_failing


# ---------------------------------------------------------------------------
# Severity assignment
# ---------------------------------------------------------------------------


def test_missing_entirely_is_investigate_severity() -> None:
    with get_conn() as conn:
        _reset(conn)
        _seed_company(conn, ticker="EMPTY")
        add_to_coverage(conn, ticker="EMPTY", tier="core", actor="human:test")

        run_steward(conn, scope=Scope(check_names=["expected_coverage"]))
        rows = _findings(conn)

    # 'present' rule failing on segments → has_data=False → investigate.
    seg = next(r for r in rows if r["vertical"] == "segments")
    assert seg["severity"] == "investigate"


def test_partial_count_is_warning_severity() -> None:
    """min_periods failing on partial data is a warning (not investigate)."""
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="PART")
        # Has financials data but only 5 periods (need 20 for core).
        _seed_quarterly_facts(conn, company_id=cid, n_periods=5)
        add_to_coverage(conn, ticker="PART", tier="core", actor="human:test")

        run_steward(conn, scope=Scope(check_names=["expected_coverage"]))
        rows = _findings(conn)

    fin = next(r for r in rows if r["vertical"] == "financials")
    assert fin["severity"] == "warning"
    assert fin["evidence"]["actual"] == 5
    assert fin["evidence"]["expected"] == 20


# ---------------------------------------------------------------------------
# Lifecycle: auto-resolve, suppression
# ---------------------------------------------------------------------------


def test_finding_auto_resolves_when_data_lands() -> None:
    """Seed empty member → finding fires for financials. Backfill
    enough data → re-run → finding auto-closes."""
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="GROW")
        add_to_coverage(conn, ticker="GROW", tier="core", actor="human:test")

        # First run: financials missing → finding open.
        run_steward(conn, scope=Scope(check_names=["expected_coverage"]))
        rows = _findings(conn)
        assert any(r["vertical"] == "financials" for r in rows)

        # Backfill 20 periods of financials.
        _seed_quarterly_facts(conn, company_id=cid, n_periods=20)

        # Second run: financials no longer surfaces → auto-resolve.
        run_steward(conn, scope=Scope(check_names=["expected_coverage"]))
        rows = _findings(conn)
        assert not any(r["vertical"] == "financials" for r in rows)


def test_suppression_respected_across_sweeps() -> None:
    """Suppress a financials finding → next sweep doesn't reopen it."""
    with get_conn() as conn:
        _reset(conn)
        _seed_company(conn, ticker="SUPP")
        add_to_coverage(conn, ticker="SUPP", tier="core", actor="human:test")

        run_steward(conn, scope=Scope(check_names=["expected_coverage"]))
        rows = _findings(conn)
        fin = next(r for r in rows if r["vertical"] == "financials")
        suppress_finding(
            conn, fin["id"], actor="human:test",
            reason="vendor offline; will retry next quarter",
            expires=None,
        )

        # Re-run; financials finding stays suppressed (not reopened).
        run_steward(conn, scope=Scope(check_names=["expected_coverage"]))
        rows_after = _findings(conn)
        assert not any(r["vertical"] == "financials" for r in rows_after)


# ---------------------------------------------------------------------------
# Scope handling
# ---------------------------------------------------------------------------


def test_ticker_scoped_run_only_evaluates_in_scope() -> None:
    with get_conn() as conn:
        _reset(conn)
        _seed_company(conn, ticker="A", cik=100)
        _seed_company(conn, ticker="B", cik=200)
        add_to_coverage(conn, ticker="A", tier="core", actor="human:test")
        add_to_coverage(conn, ticker="B", tier="core", actor="human:test")

        # Scope to A only — B's findings should NOT appear.
        run_steward(conn, scope=Scope(
            check_names=["expected_coverage"],
            tickers=["A"],
        ))
        rows = _findings(conn)
        tickers_with_findings = {r["ticker"] for r in rows}
    assert "A" in tickers_with_findings
    assert "B" not in tickers_with_findings
