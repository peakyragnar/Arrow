"""Integration tests for metric-view guard behavior.

Focus:
  - suppress on missing / zero denominators
  - suppress on missing history where the SQL contract says to suppress
  - retain explicit fallbacks where the spec says to fallback

Warning: DROPs and recreates the `public` schema.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import psycopg
from psycopg.rows import dict_row

from arrow.db.connection import get_conn
from arrow.db.migrations import apply
from scripts.apply_views import main as apply_views_main


CONCEPT_STATEMENT = {
    "revenue": "income_statement",
    "cogs": "income_statement",
    "gross_profit": "income_statement",
    "rd": "income_statement",
    "total_opex": "income_statement",
    "operating_income": "income_statement",
    "interest_expense": "income_statement",
    "ebt_incl_unusual": "income_statement",
    "tax": "income_statement",
    "net_income": "income_statement",
    "shares_diluted_weighted_avg": "income_statement",
    "accounts_receivable": "balance_sheet",
    "inventory": "balance_sheet",
    "accounts_payable": "balance_sheet",
    "total_assets": "balance_sheet",
    "total_equity": "balance_sheet",
    "current_portion_lt_debt": "balance_sheet",
    "long_term_debt": "balance_sheet",
    "current_portion_leases_operating": "balance_sheet",
    "long_term_leases_operating": "balance_sheet",
    "cash_and_equivalents": "balance_sheet",
    "short_term_investments": "balance_sheet",
    "cfo": "cash_flow",
    "capital_expenditures": "cash_flow",
    "dna_cf": "cash_flow",
    "sbc": "cash_flow",
    "cash_paid_for_interest": "cash_flow",
    "acquisitions": "cash_flow",
    "total_employees": "metrics",
}

UNSCALED_CONCEPTS = {"shares_diluted_weighted_avg", "total_employees"}
MONEY_SCALE = Decimal("1000000")


def _d(value: str | int | float) -> Decimal:
    return Decimal(str(value))


def _m(value: str | int | float) -> Decimal:
    return _d(value) * MONEY_SCALE


def _quarter_end(year: int, quarter: int) -> date:
    month_day = {1: (3, 31), 2: (6, 30), 3: (9, 30), 4: (12, 31)}
    month, day = month_day[quarter]
    return date(year, month, day)


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
                %s, 'test', '/guards', '{}'::jsonb, decode(repeat('00',32),'hex'),
                'https://test/guards', 200, 'application/json',
                '{}'::jsonb, decode(repeat('00',32),'hex'), decode(repeat('00',32),'hex')
            )
            RETURNING id;
            """,
            (run_id,),
        )
        raw_id = cur.fetchone()[0]
    conn.commit()
    return run_id, raw_id


def _insert_fact(
    conn: psycopg.Connection,
    *,
    company_id: int,
    ingest_run_id: int,
    raw_response_id: int,
    concept: str,
    value: Decimal,
    fiscal_year: int,
    period_type: str,
    fiscal_quarter: int | None = None,
) -> None:
    if period_type == "quarter":
        assert fiscal_quarter is not None
        period_end = _quarter_end(fiscal_year, fiscal_quarter)
        fiscal_label = f"FY{fiscal_year} Q{fiscal_quarter}"
    else:
        period_end = date(fiscal_year, 12, 31)
        fiscal_label = f"FY{fiscal_year}"
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
                %s, %s, %s, %s, 'USD',
                %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, 'test-metric-guards-v1',
                %s
            );
            """,
            (
                company_id,
                CONCEPT_STATEMENT[concept],
                concept,
                value,
                fiscal_year,
                fiscal_quarter,
                fiscal_label,
                period_end,
                period_type,
                period_end.year,
                calendar_quarter,
                f"CY{period_end.year} Q{calendar_quarter}",
                datetime(period_end.year, period_end.month, min(period_end.day, 28), tzinfo=timezone.utc),
                raw_response_id,
                ingest_run_id,
            ),
        )
    conn.commit()


def _fetch_one(conn: psycopg.Connection, query: str, params: tuple) -> dict:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, params)
        row = cur.fetchone()
    assert row is not None
    return row


def _base_row(
    *,
    revenue: int,
    gross_profit: int,
    operating_income: int,
    ebt: int,
    tax: int,
    net_income: int,
    total_assets: int = 200,
    total_equity: int = 100,
    ar: int | None = 10,
    inv: int | None = 20,
    ap: int | None = 15,
) -> dict[str, Decimal]:
    row: dict[str, Decimal] = {
        "revenue": _m(revenue),
        "cogs": _m(revenue - gross_profit),
        "gross_profit": _m(gross_profit),
        "rd": _m(0),
        "total_opex": _m(gross_profit - operating_income),
        "operating_income": _m(operating_income),
        "interest_expense": _m(2),
        "ebt_incl_unusual": _m(ebt),
        "tax": _m(tax),
        "net_income": _m(net_income),
        "shares_diluted_weighted_avg": _d(50),
        "total_assets": _m(total_assets),
        "total_equity": _m(total_equity),
        "current_portion_lt_debt": _m(5),
        "long_term_debt": _m(30),
        "current_portion_leases_operating": _m(2),
        "long_term_leases_operating": _m(8),
        "cash_and_equivalents": _m(20),
        "short_term_investments": _m(10),
        "cfo": _m(max(net_income, 0)),
        "capital_expenditures": _m(-2),
        "dna_cf": _m(2),
        "sbc": _m(1),
        "cash_paid_for_interest": _m(2),
        "acquisitions": _m(0),
    }
    if ar is not None:
        row["accounts_receivable"] = _m(ar)
    if inv is not None:
        row["inventory"] = _m(inv)
    if ap is not None:
        row["accounts_payable"] = _m(ap)
    return row


def _seed_quarter_series(conn: psycopg.Connection, *, ticker: str, cik: int, rows: list[tuple[int, int, dict[str, Decimal]]]) -> None:
    company_id = _seed_company(conn, cik=cik, ticker=ticker)
    run_id, raw_id = _seed_run_and_raw(conn)
    for fiscal_year, fiscal_quarter, row in rows:
        for concept, value in row.items():
            _insert_fact(
                conn,
                company_id=company_id,
                ingest_run_id=run_id,
                raw_response_id=raw_id,
                concept=concept,
                value=value,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                period_type="quarter",
            )


def test_tax_rate_ttm_and_roic_suppress_without_full_window_or_prior_quarter() -> None:
    with get_conn() as conn:
        _reset(conn)
        _seed_quarter_series(
            conn,
            ticker="GUARD0",
            cik=3001,
            rows=[
                (2022, 1, _base_row(revenue=100, gross_profit=60, operating_income=20, ebt=18, tax=4, net_income=14)),
                (2022, 2, _base_row(revenue=110, gross_profit=66, operating_income=22, ebt=20, tax=5, net_income=15)),
                (2022, 3, _base_row(revenue=120, gross_profit=72, operating_income=24, ebt=22, tax=5, net_income=17)),
            ],
        )

        tax_row = _fetch_one(
            conn,
            """
            SELECT tax_rate_ttm FROM v_tax_rate_ttm t
            JOIN companies c ON c.id = t.company_id
            WHERE c.ticker = 'GUARD0' AND t.period_end = %s;
            """,
            (date(2022, 9, 30),),
        )
        assert tax_row["tax_rate_ttm"] is None

        roic_row = _fetch_one(
            conn,
            """
            SELECT roic FROM v_metrics_roic
            WHERE ticker = 'GUARD0' AND period_end = %s;
            """,
            (date(2022, 3, 31),),
        )
        assert roic_row["roic"] is None


def test_q_and_ttm_metrics_suppress_on_zero_prior_or_missing_employee_history() -> None:
    with get_conn() as conn:
        _reset(conn)
        _seed_quarter_series(
            conn,
            ticker="GUARD1",
            cik=3002,
            rows=[
                (2022, 1, _base_row(revenue=0, gross_profit=0, operating_income=5, ebt=4, tax=1, net_income=3)),
                (2022, 2, _base_row(revenue=100, gross_profit=50, operating_income=10, ebt=8, tax=2, net_income=6)),
                (2022, 3, _base_row(revenue=110, gross_profit=55, operating_income=11, ebt=9, tax=2, net_income=7)),
                (2022, 4, _base_row(revenue=120, gross_profit=60, operating_income=12, ebt=10, tax=2, net_income=8)),
            ],
        )

        q1 = _fetch_one(
            conn,
            """
            SELECT gross_margin FROM v_metrics_q
            WHERE ticker = 'GUARD1' AND period_end = %s;
            """,
            (date(2022, 3, 31),),
        )
        assert q1["gross_margin"] is None

        q2 = _fetch_one(
            conn,
            """
            SELECT revenue_qoq_annualized FROM v_metrics_q
            WHERE ticker = 'GUARD1' AND period_end = %s;
            """,
            (date(2022, 6, 30),),
        )
        assert q2["revenue_qoq_annualized"] is None

        ttm = _fetch_one(
            conn,
            """
            SELECT revenue_per_employee FROM v_metrics_ttm
            WHERE ticker = 'GUARD1' AND period_end = %s;
            """,
            (date(2022, 12, 31),),
        )
        assert ttm["revenue_per_employee"] is None


def test_yoy_metrics_suppress_when_prior_year_ttm_denominator_nonpositive() -> None:
    with get_conn() as conn:
        _reset(conn)
        _seed_quarter_series(
            conn,
            ticker="GUARD2",
            cik=3003,
            rows=[
                (2021, 1, _base_row(revenue=0, gross_profit=0, operating_income=1, ebt=1, tax=0, net_income=1)),
                (2021, 2, _base_row(revenue=0, gross_profit=0, operating_income=1, ebt=1, tax=0, net_income=1)),
                (2021, 3, _base_row(revenue=0, gross_profit=0, operating_income=1, ebt=1, tax=0, net_income=1)),
                (2021, 4, _base_row(revenue=0, gross_profit=0, operating_income=1, ebt=1, tax=0, net_income=1)),
                (2022, 1, _base_row(revenue=100, gross_profit=50, operating_income=10, ebt=8, tax=2, net_income=6)),
                (2022, 2, _base_row(revenue=110, gross_profit=55, operating_income=11, ebt=9, tax=2, net_income=7)),
                (2022, 3, _base_row(revenue=120, gross_profit=60, operating_income=12, ebt=10, tax=2, net_income=8)),
                (2022, 4, _base_row(revenue=130, gross_profit=65, operating_income=13, ebt=11, tax=2, net_income=9)),
            ],
        )

        yoy = _fetch_one(
            conn,
            """
            SELECT revenue_yoy_ttm, gross_profit_yoy_ttm
            FROM v_metrics_ttm_yoy
            WHERE ticker = 'GUARD2' AND period_end = %s;
            """,
            (date(2022, 12, 31),),
        )
        assert yoy["revenue_yoy_ttm"] is None
        assert yoy["gross_profit_yoy_ttm"] is None


def test_roiic_suppresses_when_invested_capital_delta_is_near_zero() -> None:
    with get_conn() as conn:
        _reset(conn)
        rows = []
        for fiscal_year, fiscal_quarter in [
            (2021, 1), (2021, 2), (2021, 3), (2021, 4),
            (2022, 1), (2022, 2), (2022, 3), (2022, 4),
        ]:
            total_equity = 100 if fiscal_year == 2021 else 100
            rows.append(
                (fiscal_year, fiscal_quarter, _base_row(
                    revenue=100 + fiscal_quarter,
                    gross_profit=60 + fiscal_quarter,
                    operating_income=20 + fiscal_quarter,
                    ebt=18 + fiscal_quarter,
                    tax=4,
                    net_income=14,
                    total_assets=200,
                    total_equity=total_equity,
                ))
            )
        _seed_quarter_series(conn, ticker="GUARD3", cik=3004, rows=rows)

        roic = _fetch_one(
            conn,
            """
            SELECT roiic FROM v_metrics_roic
            WHERE ticker = 'GUARD3' AND period_end = %s;
            """,
            (date(2022, 12, 31),),
        )
        assert roic["roiic"] is None


def test_cfo_and_fcf_to_nopat_suppress_when_adjusted_nopat_is_zero() -> None:
    with get_conn() as conn:
        _reset(conn)
        _seed_quarter_series(
            conn,
            ticker="GUARD4",
            cik=3005,
            rows=[
                (2022, 1, _base_row(revenue=100, gross_profit=40, operating_income=0, ebt=0, tax=0, net_income=0)),
                (2022, 2, _base_row(revenue=100, gross_profit=40, operating_income=0, ebt=0, tax=0, net_income=0)),
                (2022, 3, _base_row(revenue=100, gross_profit=40, operating_income=0, ebt=0, tax=0, net_income=0)),
                (2022, 4, _base_row(revenue=100, gross_profit=40, operating_income=0, ebt=0, tax=0, net_income=0)),
            ],
        )

        row = _fetch_one(
            conn,
            """
            SELECT cfo_to_nopat, fcf_to_nopat
            FROM v_metrics_ttm
            WHERE ticker = 'GUARD4' AND period_end = %s;
            """,
            (date(2022, 12, 31),),
        )
        assert row["cfo_to_nopat"] is None
        assert row["fcf_to_nopat"] is None
