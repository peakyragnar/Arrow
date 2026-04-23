"""Integration regression for amendment-detect BS co-supersession.

Warning: DROPs and recreates the `public` schema.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import psycopg

from arrow.agents.amendment_detect import (
    BS_AMENDMENT_VERSION,
    IS_AMENDMENT_VERSION,
    detect_and_apply_amendments,
)
from arrow.db.connection import get_conn
from arrow.db.migrations import apply
from arrow.ingest.sec.company_facts import CompanyFactsFetch
from arrow.normalize.financials.load import BS_EXTRACTION_VERSION, IS_EXTRACTION_VERSION


def _reset(conn: psycopg.Connection) -> None:
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE;")
        cur.execute("CREATE SCHEMA public;")
    apply(conn)
    conn.autocommit = False


def _seed(conn: psycopg.Connection) -> tuple[int, int, int, int]:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO companies (cik, ticker, name, fiscal_year_end_md) "
            "VALUES (1837240, 'SYM', 'Symbotic Inc.', '09-28') RETURNING id;"
        )
        company_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO ingest_runs (run_kind, vendor, status) "
            "VALUES ('manual', 'test', 'started') RETURNING id;"
        )
        run_id = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO raw_responses (
                ingest_run_id, vendor, endpoint, params, params_hash,
                request_url, http_status, content_type, body_jsonb,
                raw_hash, canonical_hash
            ) VALUES (
                %s, 'test', '/fmp', '{}'::jsonb, decode(repeat('00',32),'hex'),
                'https://test/fmp', 200, 'application/json', '{}'::jsonb,
                decode(repeat('00',32),'hex'), decode(repeat('00',32),'hex')
            ) RETURNING id;
            """,
            (run_id,),
        )
        fmp_raw_id = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO raw_responses (
                ingest_run_id, vendor, endpoint, params, params_hash,
                request_url, http_status, content_type, body_jsonb,
                raw_hash, canonical_hash
            ) VALUES (
                %s, 'sec', '/companyfacts', '{}'::jsonb, decode(repeat('11',32),'hex'),
                'https://test/sec', 200, 'application/json', '{}'::jsonb,
                decode(repeat('11',32),'hex'), decode(repeat('11',32),'hex')
            ) RETURNING id;
            """,
            (run_id,),
        )
        sec_raw_id = cur.fetchone()[0]
    conn.commit()
    return company_id, run_id, fmp_raw_id, sec_raw_id


def _ins(
    conn: psycopg.Connection,
    *,
    company_id: int,
    run_id: int,
    raw_id: int,
    statement: str,
    concept: str,
    value: Decimal,
    period_end: date,
    period_type: str,
    fiscal_year: int,
    fiscal_quarter: int | None,
    extraction_version: str,
) -> None:
    fiscal_label = (
        f"FY{fiscal_year} Q{fiscal_quarter}"
        if fiscal_quarter is not None
        else f"FY{fiscal_year}"
    )
    calendar_quarter = ((period_end.month - 1) // 3) + 1
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
                %s, %s, %s,
                %s
            );
            """,
            (
                company_id, statement, concept, value,
                fiscal_year, fiscal_quarter, fiscal_label,
                period_end, period_type,
                period_end.year, calendar_quarter, f"CY{period_end.year} Q{calendar_quarter}",
                datetime(2024, 11, 1, tzinfo=timezone.utc),
                raw_id, extraction_version,
                run_id,
            ),
        )
    conn.commit()


def test_is_restatement_also_supersedes_balance_sheet_instants(monkeypatch) -> None:
    with get_conn() as conn:
        _reset(conn)
        company_id, run_id, fmp_raw_id, sec_raw_id = _seed(conn)

        # Layer 3 failure on IS revenue: stored quarters sum to 400, FY is 410.
        for quarter, period_end in (
            (1, date(2023, 12, 30)),
            (2, date(2024, 3, 30)),
            (3, date(2024, 6, 29)),
            (4, date(2024, 9, 28)),
        ):
            _ins(
                conn,
                company_id=company_id,
                run_id=run_id,
                raw_id=fmp_raw_id,
                statement="income_statement",
                concept="revenue",
                value=Decimal("100000000"),
                period_end=period_end,
                period_type="quarter",
                fiscal_year=2024,
                fiscal_quarter=quarter,
                extraction_version=IS_EXTRACTION_VERSION,
            )
        _ins(
            conn,
            company_id=company_id,
            run_id=run_id,
            raw_id=fmp_raw_id,
            statement="income_statement",
            concept="revenue",
            value=Decimal("410000000"),
            period_end=date(2024, 9, 28),
            period_type="annual",
            fiscal_year=2024,
            fiscal_quarter=None,
            extraction_version=IS_EXTRACTION_VERSION,
        )

        # Matching BS snapshot at the restated Q1 end date. These are the rows
        # that used to stay stale because BS was never pulled into scope.
        for concept, value in (
            ("cash_and_equivalents", Decimal("1000000000")),
            ("total_current_assets", Decimal("1000000000")),
            ("total_assets", Decimal("1000000000")),
            ("accounts_payable", Decimal("400000000")),
            ("total_current_liabilities", Decimal("400000000")),
            ("total_liabilities", Decimal("400000000")),
            ("common_stock", Decimal("600000000")),
            ("total_equity", Decimal("600000000")),
            ("total_liabilities_and_equity", Decimal("1000000000")),
        ):
            _ins(
                conn,
                company_id=company_id,
                run_id=run_id,
                raw_id=fmp_raw_id,
                statement="balance_sheet",
                concept=concept,
                value=value,
                period_end=date(2023, 12, 30),
                period_type="quarter",
                fiscal_year=2024,
                fiscal_quarter=1,
                extraction_version=BS_EXTRACTION_VERSION,
            )

        companyfacts = {
            "cik": 1837240,
            "entityName": "Symbotic",
            "facts": {
                "us-gaap": {
                    "Revenues": {
                        "units": {
                            "USD": [
                                {
                                    "start": "2023-10-01",
                                    "end": "2023-12-30",
                                    "val": 110000000,
                                    "accn": "0001837240-24-999999",
                                    "fy": 2024,
                                    "fp": "Q1",
                                    "form": "10-K",
                                    "filed": "2024-11-01",
                                },
                            ],
                        },
                    },
                    "CashAndCashEquivalentsAtCarryingValue": {
                        "units": {
                            "USD": [
                                {
                                    "end": "2023-12-30",
                                    "val": 900000000,
                                    "accn": "0001837240-24-999999",
                                    "fy": 2024,
                                    "fp": "Q1",
                                    "form": "10-K",
                                    "filed": "2024-11-01",
                                },
                            ],
                        },
                    },
                    "AssetsCurrent": {
                        "units": {
                            "USD": [
                                {
                                    "end": "2023-12-30",
                                    "val": 900000000,
                                    "accn": "0001837240-24-999999",
                                    "fy": 2024,
                                    "fp": "Q1",
                                    "form": "10-K",
                                    "filed": "2024-11-01",
                                },
                            ],
                        },
                    },
                    "Assets": {
                        "units": {
                            "USD": [
                                {
                                    "end": "2023-12-30",
                                    "val": 900000000,
                                    "accn": "0001837240-24-999999",
                                    "fy": 2024,
                                    "fp": "Q1",
                                    "form": "10-K",
                                    "filed": "2024-11-01",
                                },
                            ],
                        },
                    },
                    "AccountsPayableCurrent": {
                        "units": {
                            "USD": [
                                {
                                    "end": "2023-12-30",
                                    "val": 350000000,
                                    "accn": "0001837240-24-999999",
                                    "fy": 2024,
                                    "fp": "Q1",
                                    "form": "10-K",
                                    "filed": "2024-11-01",
                                },
                            ],
                        },
                    },
                    "LiabilitiesCurrent": {
                        "units": {
                            "USD": [
                                {
                                    "end": "2023-12-30",
                                    "val": 350000000,
                                    "accn": "0001837240-24-999999",
                                    "fy": 2024,
                                    "fp": "Q1",
                                    "form": "10-K",
                                    "filed": "2024-11-01",
                                },
                            ],
                        },
                    },
                    "Liabilities": {
                        "units": {
                            "USD": [
                                {
                                    "end": "2023-12-30",
                                    "val": 350000000,
                                    "accn": "0001837240-24-999999",
                                    "fy": 2024,
                                    "fp": "Q1",
                                    "form": "10-K",
                                    "filed": "2024-11-01",
                                },
                            ],
                        },
                    },
                    "CommonStockValue": {
                        "units": {
                            "USD": [
                                {
                                    "end": "2023-12-30",
                                    "val": 550000000,
                                    "accn": "0001837240-24-999999",
                                    "fy": 2024,
                                    "fp": "Q1",
                                    "form": "10-K",
                                    "filed": "2024-11-01",
                                },
                            ],
                        },
                    },
                    "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest": {
                        "units": {
                            "USD": [
                                {
                                    "end": "2023-12-30",
                                    "val": 550000000,
                                    "accn": "0001837240-24-999999",
                                    "fy": 2024,
                                    "fp": "Q1",
                                    "form": "10-K",
                                    "filed": "2024-11-01",
                                },
                            ],
                        },
                    },
                    "LiabilitiesAndStockholdersEquity": {
                        "units": {
                            "USD": [
                                {
                                    "end": "2023-12-30",
                                    "val": 900000000,
                                    "accn": "0001837240-24-999999",
                                    "fy": 2024,
                                    "fp": "Q1",
                                    "form": "10-K",
                                    "filed": "2024-11-01",
                                },
                            ],
                        },
                    },
                },
            },
        }

        def _fake_fetch_company_facts(conn_, *, cik, ingest_run_id, http):
            assert conn_ is conn
            assert cik == 1837240
            assert ingest_run_id == run_id
            return CompanyFactsFetch(raw_response_id=sec_raw_id, payload=companyfacts)

        monkeypatch.setattr(
            "arrow.agents.amendment_detect.fetch_company_facts",
            _fake_fetch_company_facts,
        )

        result = detect_and_apply_amendments(
            conn,
            company_id=company_id,
            company_cik=1837240,
            ingest_run_id=run_id,
        )

        assert result.status == "amended"

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT statement, concept, value, extraction_version, supersedes_fact_id
                FROM financial_facts
                WHERE company_id = %s
                  AND period_end = %s
                  AND superseded_at IS NULL
                  AND concept IN ('revenue', 'cash_and_equivalents', 'total_current_assets',
                                  'total_assets', 'accounts_payable',
                                  'total_current_liabilities', 'total_liabilities',
                                  'common_stock', 'total_equity',
                                  'total_liabilities_and_equity')
                ORDER BY statement, concept;
                """,
                (company_id, date(2023, 12, 30)),
            )
            rows = cur.fetchall()

        current = {(stmt, concept): (value, version) for stmt, concept, value, version, _supersedes_id in rows}
        assert current == {
            ("balance_sheet", "accounts_payable"): (Decimal("350000000.0000"), BS_AMENDMENT_VERSION),
            ("balance_sheet", "cash_and_equivalents"): (Decimal("900000000.0000"), BS_AMENDMENT_VERSION),
            ("balance_sheet", "common_stock"): (Decimal("550000000.0000"), BS_AMENDMENT_VERSION),
            ("balance_sheet", "total_assets"): (Decimal("900000000.0000"), BS_AMENDMENT_VERSION),
            ("balance_sheet", "total_current_assets"): (Decimal("900000000.0000"), BS_AMENDMENT_VERSION),
            ("balance_sheet", "total_current_liabilities"): (Decimal("350000000.0000"), BS_AMENDMENT_VERSION),
            ("balance_sheet", "total_equity"): (Decimal("550000000.0000"), BS_AMENDMENT_VERSION),
            ("balance_sheet", "total_liabilities"): (Decimal("350000000.0000"), BS_AMENDMENT_VERSION),
            ("balance_sheet", "total_liabilities_and_equity"): (Decimal("900000000.0000"), BS_AMENDMENT_VERSION),
            ("income_statement", "revenue"): (Decimal("110000000.0000"), IS_AMENDMENT_VERSION),
        }
        assert all(supersedes_id is not None for *_prefix, supersedes_id in rows)

        expected_published_at = datetime(2024, 11, 1, tzinfo=timezone.utc)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT published_at
                FROM financial_facts
                WHERE company_id = %s
                  AND extraction_version IN (%s, %s)
                ORDER BY statement, concept;
                """,
                (company_id, BS_AMENDMENT_VERSION, IS_AMENDMENT_VERSION),
            )
            amendment_published_at = [row[0] for row in cur.fetchall()]
            cur.execute(
                """
                SELECT superseded_at
                FROM financial_facts
                WHERE company_id = %s
                  AND period_end = %s
                  AND extraction_version IN (%s, %s)
                  AND concept IN ('revenue', 'cash_and_equivalents', 'total_current_assets',
                                  'total_assets', 'accounts_payable',
                                  'total_current_liabilities', 'total_liabilities',
                                  'common_stock', 'total_equity',
                                  'total_liabilities_and_equity')
                ORDER BY statement, concept;
                """,
                (company_id, date(2023, 12, 30), BS_EXTRACTION_VERSION, IS_EXTRACTION_VERSION),
            )
            superseded_at = [row[0] for row in cur.fetchall()]

        assert len(amendment_published_at) == 10
        assert amendment_published_at == [expected_published_at] * 10
        assert superseded_at == [expected_published_at] * 10
