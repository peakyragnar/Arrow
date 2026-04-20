"""Integration tests for Layer 3 period arithmetic (Q1+Q2+Q3+Q4 ≈ FY).

Uses the DB directly — seed a company, insert synthetic facts via raw SQL,
call the verifier, assert outcomes. Avoids the full ingest pipeline so we
can probe corner cases (missing quarters, off-by-bucket, tolerance).

Warning: DROPs and recreates the `public` schema.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import psycopg

from arrow.db.connection import get_conn
from arrow.db.migrations import apply
from arrow.normalize.financials.verify_period_arithmetic import (
    verify_period_arithmetic,
)


def _reset(conn: psycopg.Connection) -> None:
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE;")
        cur.execute("CREATE SCHEMA public;")
    apply(conn)
    conn.autocommit = False


def _seed_company_and_run(conn: psycopg.Connection) -> tuple[int, int, int]:
    """Return (company_id, ingest_run_id, raw_response_id)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO companies (cik, ticker, name, fiscal_year_end_md)
            VALUES (1045810, 'NVDA', 'NVIDIA CORP', '01-31') RETURNING id;
            """,
        )
        cid = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO ingest_runs (run_kind, vendor, status)
            VALUES ('manual', 'test', 'started') RETURNING id;
            """,
        )
        rid = cur.fetchone()[0]
        # One raw_response to satisfy FK.
        cur.execute(
            """
            INSERT INTO raw_responses (
                ingest_run_id, vendor, endpoint, params, params_hash,
                request_url, http_status, content_type,
                body_jsonb, raw_hash, canonical_hash
            ) VALUES (
                %s, 'test', '/x', '{}'::jsonb, decode(repeat('00',32),'hex'),
                'https://test', 200, 'application/json',
                '{}'::jsonb, decode(repeat('00',32),'hex'), decode(repeat('00',32),'hex')
            ) RETURNING id;
            """,
            (rid,),
        )
        raw_id = cur.fetchone()[0]
    conn.commit()
    return cid, rid, raw_id


def _insert_fact(
    conn: psycopg.Connection,
    *,
    company_id: int,
    ingest_run_id: int,
    source_raw_response_id: int,
    concept: str,
    value: Decimal,
    fiscal_year: int,
    fiscal_quarter: int | None,
    period_type: str,
) -> None:
    # Derive a plausible period_end per fiscal_quarter for NVDA's calendar.
    period_end_by_q = {
        1: date(fiscal_year - 1, 4, 28),
        2: date(fiscal_year - 1, 7, 28),
        3: date(fiscal_year - 1, 10, 27),
        4: date(fiscal_year, 1, 26),
        None: date(fiscal_year, 1, 26),
    }
    period_end = period_end_by_q[fiscal_quarter]
    if fiscal_quarter is not None:
        label = f"FY{fiscal_year} Q{fiscal_quarter}"
    else:
        label = f"FY{fiscal_year}"
    cal_year = period_end.year
    cal_q = (period_end.month - 1) // 3 + 1
    with conn.cursor() as cur:
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
                %s, 'income_statement', %s, %s, 'USD',
                %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, 'fmp-is-v1',
                %s
            );
            """,
            (
                company_id, concept, value,
                fiscal_year, fiscal_quarter, label,
                period_end, period_type,
                cal_year, cal_q, f"CY{cal_year} Q{cal_q}",
                datetime(fiscal_year, 2, 1, tzinfo=timezone.utc),
                source_raw_response_id,
                ingest_run_id,
            ),
        )
    conn.commit()


def test_complete_and_matching_identities_pass() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid, rid, raw_id = _seed_company_and_run(conn)
        # Q1-Q4 sum to FY cleanly.
        for q, val in [(1, 100), (2, 200), (3, 300), (4, 400)]:
            _insert_fact(conn, company_id=cid, ingest_run_id=rid,
                         source_raw_response_id=raw_id,
                         concept="revenue", value=Decimal(f"{val}000000"),
                         fiscal_year=2025, fiscal_quarter=q, period_type="quarter")
        _insert_fact(conn, company_id=cid, ingest_run_id=rid,
                     source_raw_response_id=raw_id,
                     concept="revenue", value=Decimal("1000000000"),  # 100+200+300+400 MM
                     fiscal_year=2025, fiscal_quarter=None, period_type="annual")

        failures = verify_period_arithmetic(conn, company_id=cid,
                                             extraction_version="fmp-is-v1")
        assert failures == []


def test_sum_mismatch_fails() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid, rid, raw_id = _seed_company_and_run(conn)
        for q, val in [(1, 100), (2, 200), (3, 300), (4, 400)]:
            _insert_fact(conn, company_id=cid, ingest_run_id=rid,
                         source_raw_response_id=raw_id,
                         concept="revenue", value=Decimal(f"{val}000000"),
                         fiscal_year=2025, fiscal_quarter=q, period_type="quarter")
        # Annual off by 5M — well beyond the $1M tolerance.
        _insert_fact(conn, company_id=cid, ingest_run_id=rid,
                     source_raw_response_id=raw_id,
                     concept="revenue", value=Decimal("1005000000"),
                     fiscal_year=2025, fiscal_quarter=None, period_type="annual")

        failures = verify_period_arithmetic(conn, company_id=cid,
                                             extraction_version="fmp-is-v1")
        assert len(failures) == 1
        f = failures[0]
        assert f.concept == "revenue"
        assert f.fiscal_year == 2025
        assert f.quarters_sum == Decimal("1000000000")
        assert f.annual == Decimal("1005000000")
        assert f.delta == Decimal("5000000")


def test_missing_quarter_skips_identity_not_failure() -> None:
    """If we only have Q1-Q3 + FY (no Q4), skip the identity, don't fail."""
    with get_conn() as conn:
        _reset(conn)
        cid, rid, raw_id = _seed_company_and_run(conn)
        for q, val in [(1, 100), (2, 200), (3, 300)]:
            _insert_fact(conn, company_id=cid, ingest_run_id=rid,
                         source_raw_response_id=raw_id,
                         concept="revenue", value=Decimal(f"{val}000000"),
                         fiscal_year=2025, fiscal_quarter=q, period_type="quarter")
        _insert_fact(conn, company_id=cid, ingest_run_id=rid,
                     source_raw_response_id=raw_id,
                     concept="revenue", value=Decimal("1000000000"),
                     fiscal_year=2025, fiscal_quarter=None, period_type="annual")

        failures = verify_period_arithmetic(conn, company_id=cid,
                                             extraction_version="fmp-is-v1")
        assert failures == []  # incomplete set is skipped, not failed


def test_multiple_concepts_checked_independently() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid, rid, raw_id = _seed_company_and_run(conn)
        # revenue: ties. cogs: off by 10M (fails).
        for q, rev, cogs in [(1, 100, 40), (2, 200, 80), (3, 300, 120), (4, 400, 160)]:
            _insert_fact(conn, company_id=cid, ingest_run_id=rid,
                         source_raw_response_id=raw_id,
                         concept="revenue", value=Decimal(f"{rev}000000"),
                         fiscal_year=2025, fiscal_quarter=q, period_type="quarter")
            _insert_fact(conn, company_id=cid, ingest_run_id=rid,
                         source_raw_response_id=raw_id,
                         concept="cogs", value=Decimal(f"{cogs}000000"),
                         fiscal_year=2025, fiscal_quarter=q, period_type="quarter")
        _insert_fact(conn, company_id=cid, ingest_run_id=rid,
                     source_raw_response_id=raw_id,
                     concept="revenue", value=Decimal("1000000000"),
                     fiscal_year=2025, fiscal_quarter=None, period_type="annual")
        _insert_fact(conn, company_id=cid, ingest_run_id=rid,
                     source_raw_response_id=raw_id,
                     concept="cogs", value=Decimal("410000000"),  # 400+10M off
                     fiscal_year=2025, fiscal_quarter=None, period_type="annual")

        failures = verify_period_arithmetic(conn, company_id=cid,
                                             extraction_version="fmp-is-v1")
        assert len(failures) == 1
        assert failures[0].concept == "cogs"


def test_per_share_and_share_count_buckets_are_not_checked() -> None:
    """eps_basic, shares_*, etc are not additive across quarters — skip them."""
    with get_conn() as conn:
        _reset(conn)
        cid, rid, raw_id = _seed_company_and_run(conn)
        # EPS values don't sum to the annual one; must not trigger a failure.
        for q, eps in [(1, 1), (2, 1), (3, 1), (4, 1)]:
            _insert_fact(conn, company_id=cid, ingest_run_id=rid,
                         source_raw_response_id=raw_id,
                         concept="eps_basic", value=Decimal(f"{eps}.00"),
                         fiscal_year=2025, fiscal_quarter=q, period_type="quarter")
        _insert_fact(conn, company_id=cid, ingest_run_id=rid,
                     source_raw_response_id=raw_id,
                     concept="eps_basic", value=Decimal("3.95"),  # NOT 4.00
                     fiscal_year=2025, fiscal_quarter=None, period_type="annual")

        failures = verify_period_arithmetic(conn, company_id=cid,
                                             extraction_version="fmp-is-v1")
        assert failures == []
