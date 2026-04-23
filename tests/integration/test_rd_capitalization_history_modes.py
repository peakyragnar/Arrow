"""Integration tests for R&D capitalization history modes.

These tests lock down the distinction between:

1. bounded/manual fixture arithmetic
2. full-history production arithmetic

Both are valid, but they should not be compared as if they were the same
mode. The live SQL view `v_rd_derived` must use all quarterly `rd` facts
present in `financial_facts`, up to the 20-quarter cap.

Warning: DROPs and recreates the `public` schema.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import psycopg

from arrow.db.connection import get_conn
from arrow.db.migrations import apply
from scripts.apply_views import main as apply_views_main


def _reset(conn: psycopg.Connection) -> None:
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE;")
        cur.execute("CREATE SCHEMA public;")
    apply(conn)
    conn.autocommit = False
    apply_views_main()


def _seed_company(conn: psycopg.Connection, *, cik: int, ticker: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO companies (cik, ticker, name, fiscal_year_end_md)
            VALUES (%s, %s, %s, '12-31')
            RETURNING id;
            """,
            (cik, ticker, f"{ticker} INC"),
        )
        company_id = cur.fetchone()[0]
    conn.commit()
    return company_id


def _seed_run_and_raw(conn: psycopg.Connection) -> tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingest_runs (run_kind, vendor, status)
            VALUES ('manual', 'test', 'started')
            RETURNING id;
            """,
        )
        run_id = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO raw_responses (
                ingest_run_id, vendor, endpoint, params, params_hash,
                request_url, http_status, content_type,
                body_jsonb, raw_hash, canonical_hash
            ) VALUES (
                %s, 'test', '/rd', '{}'::jsonb, decode(repeat('00',32),'hex'),
                'https://test/rd', 200, 'application/json',
                '{}'::jsonb, decode(repeat('00',32),'hex'), decode(repeat('00',32),'hex')
            )
            RETURNING id;
            """,
            (run_id,),
        )
        raw_id = cur.fetchone()[0]
    conn.commit()
    return run_id, raw_id


def _quarter_end(year: int, quarter: int) -> date:
    month_day = {
        1: (3, 31),
        2: (6, 30),
        3: (9, 30),
        4: (12, 31),
    }
    month, day = month_day[quarter]
    return date(year, month, day)


def _insert_rd_quarter(
    conn: psycopg.Connection,
    *,
    company_id: int,
    ingest_run_id: int,
    raw_response_id: int,
    fiscal_year: int,
    fiscal_quarter: int,
    rd_value: Decimal,
) -> None:
    period_end = _quarter_end(fiscal_year, fiscal_quarter)
    calendar_quarter = (period_end.month - 1) // 3 + 1
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
                %s, 'income_statement', 'rd', %s, 'USD',
                %s, %s, %s,
                %s, 'quarter',
                %s, %s, %s,
                %s, %s, 'test-rd-v1',
                %s
            );
            """,
            (
                company_id,
                rd_value,
                fiscal_year,
                fiscal_quarter,
                f"FY{fiscal_year} Q{fiscal_quarter}",
                period_end,
                period_end.year,
                calendar_quarter,
                f"CY{period_end.year} Q{calendar_quarter}",
                datetime(period_end.year, period_end.month, min(period_end.day, 28), tzinfo=timezone.utc),
                raw_response_id,
                ingest_run_id,
            ),
        )
    conn.commit()


def _fetch_rd_row(
    conn: psycopg.Connection,
    *,
    ticker: str,
    period_end: date,
) -> tuple[Decimal, Decimal, Decimal, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                r.rd_q,
                r.rd_amortization_q,
                r.rd_asset_q,
                r.rd_coverage_quarters
            FROM v_rd_derived r
            JOIN companies c ON c.id = r.company_id
            WHERE c.ticker = %s
              AND r.period_end = %s;
            """,
            (ticker, period_end),
        )
        row = cur.fetchone()
    assert row is not None
    return row


def _expected_amort(values: list[Decimal]) -> Decimal:
    return sum(values, Decimal("0")) / Decimal("20")


def _expected_asset(values: list[Decimal]) -> Decimal:
    start_weight = 21 - len(values)
    total = Decimal("0")
    for idx, value in enumerate(values):
        total += value * Decimal(start_weight + idx) / Decimal("20")
    return total


def test_v_rd_derived_partial_history_uses_available_quarters_and_reports_coverage() -> None:
    with get_conn() as conn:
        _reset(conn)
        company_id = _seed_company(conn, cik=1001, ticker="BOUND")
        run_id, raw_id = _seed_run_and_raw(conn)

        values = [Decimal(str(v)) for v in range(210, 300, 10)]  # 9 quarters
        anchor_period_end = _quarter_end(2022, 4)
        quarter_pairs = [
            (2020, 4),
            (2021, 1),
            (2021, 2),
            (2021, 3),
            (2021, 4),
            (2022, 1),
            (2022, 2),
            (2022, 3),
            (2022, 4),
        ]

        for (fy, fq), value in zip(quarter_pairs, values, strict=True):
            _insert_rd_quarter(
                conn,
                company_id=company_id,
                ingest_run_id=run_id,
                raw_response_id=raw_id,
                fiscal_year=fy,
                fiscal_quarter=fq,
                rd_value=value,
            )

        rd_q, amort, asset, coverage = _fetch_rd_row(
            conn,
            ticker="BOUND",
            period_end=anchor_period_end,
        )

        assert rd_q == Decimal("290")
        assert coverage == 9
        assert amort == _expected_amort(values)
        assert asset == _expected_asset(values)


def test_v_rd_derived_full_history_includes_older_quarters_not_visible_in_bounded_fixture() -> None:
    with get_conn() as conn:
        _reset(conn)
        bounded_company_id = _seed_company(conn, cik=1002, ticker="BOUND")
        full_company_id = _seed_company(conn, cik=1003, ticker="FULL")
        run_id, raw_id = _seed_run_and_raw(conn)

        full_values = [Decimal(str(v)) for v in range(100, 300, 10)]  # 20 quarters
        visible_tail = full_values[-9:]
        anchor_period_end = _quarter_end(2022, 4)

        quarter_pairs = []
        for year in range(2018, 2023):
            for quarter in range(1, 5):
                quarter_pairs.append((year, quarter))
        assert len(quarter_pairs) == 20

        for (fy, fq), value in zip(quarter_pairs, full_values, strict=True):
            _insert_rd_quarter(
                conn,
                company_id=full_company_id,
                ingest_run_id=run_id,
                raw_response_id=raw_id,
                fiscal_year=fy,
                fiscal_quarter=fq,
                rd_value=value,
            )

        for (fy, fq), value in zip(quarter_pairs[-9:], visible_tail, strict=True):
            _insert_rd_quarter(
                conn,
                company_id=bounded_company_id,
                ingest_run_id=run_id,
                raw_response_id=raw_id,
                fiscal_year=fy,
                fiscal_quarter=fq,
                rd_value=value,
            )

        bounded_row = _fetch_rd_row(conn, ticker="BOUND", period_end=anchor_period_end)
        full_row = _fetch_rd_row(conn, ticker="FULL", period_end=anchor_period_end)

        _, bounded_amort, bounded_asset, bounded_coverage = bounded_row
        _, full_amort, full_asset, full_coverage = full_row

        assert bounded_coverage == 9
        assert full_coverage == 20

        assert bounded_amort == _expected_amort(visible_tail)
        assert bounded_asset == _expected_asset(visible_tail)
        assert full_amort == _expected_amort(full_values)
        assert full_asset == _expected_asset(full_values)

        assert full_amort > bounded_amort
        assert full_asset > bounded_asset
