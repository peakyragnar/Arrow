"""Integration test for FMP income-statement backfill.

Real Postgres + mocked FMP HTTP. Fixture uses real NVDA FY2026 quarterly
and annual rows (from the cached FMP response) so verification ties pass.

Asserts:
  - raw_responses rows (one per period_type)
  - financial_facts: 18 verified buckets per period; two-clocks columns correct;
    PIT columns set
  - ingest_runs success with counts
  - Idempotency: re-running supersedes old rows + writes new ones, net zero
    change in "current" row count
  - Verification failure path: broken gross_profit -> ingest_run failed,
    no facts written from that payload (transaction rolled back)

Warning: DROPs and recreates the `public` schema in DATABASE_URL. Run
only against a dev or dedicated test database.
"""

from __future__ import annotations

import json
import os
from unittest.mock import patch

import psycopg
import pytest

from arrow.db.connection import get_conn
from arrow.db.migrations import apply
from arrow.ingest.common.http import Response


# Make sure FMPClient can be constructed without a real key.
os.environ.setdefault("FMP_API_KEY", "test-key-for-integration")


# ---------------------------------------------------------------------------
# Fixtures — real NVDA FY2026 rows from the FMP cache. Verification ties
# hold by construction (they were verified in fmp_mapping.md).
# ---------------------------------------------------------------------------


def _q4_row() -> dict:
    return {
        "date": "2026-01-25",
        "period": "Q4",
        "fiscalYear": "2026",
        "symbol": "NVDA",
        "filingDate": "2026-02-25",
        "acceptedDate": "2026-02-25 16:42:19",
        "revenue": 68127000000,
        "costOfRevenue": 17034000000,
        "grossProfit": 51093000000,
        "researchAndDevelopmentExpenses": 5512000000,
        "sellingGeneralAndAdministrativeExpenses": 1282000000,
        "operatingExpenses": 6794000000,
        "operatingIncome": 44299000000,
        "interestIncome": 568000000,
        "interestExpense": 73000000,
        "incomeBeforeTax": 50398000000,
        "incomeTaxExpense": 7438000000,
        "netIncomeFromContinuingOperations": 42960000000,
        "netIncomeFromDiscontinuedOperations": 0,
        "netIncome": 42960000000,
        "eps": 1.77,
        "epsDiluted": 1.76,
        "weightedAverageShsOut": 24304000000,
        "weightedAverageShsOutDil": 24432000000,
    }


def _fy_row() -> dict:
    return {
        "date": "2026-01-25",
        "period": "FY",
        "fiscalYear": "2026",
        "symbol": "NVDA",
        "filingDate": "2026-02-25",
        "acceptedDate": "2026-02-25 16:42:19",
        "revenue": 215938000000,
        "costOfRevenue": 62475000000,
        "grossProfit": 153463000000,
        "researchAndDevelopmentExpenses": 18497000000,
        "sellingGeneralAndAdministrativeExpenses": 4579000000,
        "operatingExpenses": 23076000000,
        "operatingIncome": 130387000000,
        "interestIncome": 2300000000,
        "interestExpense": 259000000,
        "incomeBeforeTax": 141450000000,
        "incomeTaxExpense": 21383000000,
        "netIncomeFromContinuingOperations": 120067000000,
        "netIncomeFromDiscontinuedOperations": 0,
        "netIncome": 120067000000,
        "eps": 4.93,
        "epsDiluted": 4.9,
        "weightedAverageShsOut": 24304000000,
        "weightedAverageShsOutDil": 24432000000,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _reset(conn: psycopg.Connection) -> None:
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE;")
        cur.execute("CREATE SCHEMA public;")
    apply(conn)
    conn.autocommit = False


def _seed_nvda(conn: psycopg.Connection) -> int:
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


def _fake_fmp_get(self, endpoint: str, **params) -> Response:  # noqa: ARG001
    """Mocked FMPClient.get — serves quarter vs annual based on params."""
    if params.get("period") == "quarter":
        body = json.dumps([_q4_row()]).encode()
    elif params.get("period") == "annual":
        body = json.dumps([_fy_row()]).encode()
    else:
        raise AssertionError(f"unexpected params: {params}")
    return Response(
        status=200,
        body=body,
        content_type="application/json",
        headers={"content-type": "application/json"},
        url=f"https://financialmodelingprep.com/stable/{endpoint}?symbol=NVDA&period={params['period']}",
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_backfill_writes_raw_and_facts_end_to_end() -> None:
    from arrow.agents.fmp_ingest import backfill_fmp_is

    with get_conn() as conn:
        _reset(conn)
        company_id = _seed_nvda(conn)

        with patch("arrow.ingest.fmp.client.FMPClient.get", new=_fake_fmp_get):
            counts = backfill_fmp_is(conn, ["NVDA"])

        assert counts["raw_responses"] == 2  # quarter + annual
        assert counts["rows_processed"] == 2  # 1 row per payload in this fixture
        assert counts["financial_facts_written"] == 36  # 18 buckets * 2 periods
        assert counts["financial_facts_superseded"] == 0  # first run

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*) FROM financial_facts
                WHERE company_id = %s AND superseded_at IS NULL;
                """,
                (company_id,),
            )
            assert cur.fetchone()[0] == 36

            # Two-clocks columns populated for the Q4 quarter row.
            cur.execute(
                """
                SELECT fiscal_year, fiscal_quarter, fiscal_period_label,
                       calendar_year, calendar_quarter, calendar_period_label,
                       period_type
                FROM financial_facts
                WHERE company_id = %s AND concept = 'revenue'
                  AND period_type = 'quarter';
                """,
                (company_id,),
            )
            fy, fq, fl, cy, cq, cl, pt = cur.fetchone()
            assert fy == 2026
            assert fq == 4
            assert fl == "FY2026 Q4"
            assert cy == 2026
            assert cq == 1
            assert cl == "CY2026 Q1"
            assert pt == "quarter"

            # Annual row: fiscal_quarter is NULL, label is "FY2026".
            cur.execute(
                """
                SELECT fiscal_quarter, fiscal_period_label, period_type, value
                FROM financial_facts
                WHERE company_id = %s AND concept = 'revenue'
                  AND period_type = 'annual';
                """,
                (company_id,),
            )
            fq_ann, fl_ann, pt_ann, val_ann = cur.fetchone()
            assert fq_ann is None
            assert fl_ann == "FY2026"
            assert pt_ann == "annual"
            assert val_ann == 215938000000

            # published_at picked up from acceptedDate
            cur.execute(
                """
                SELECT DISTINCT published_at FROM financial_facts
                WHERE company_id = %s;
                """,
                (company_id,),
            )
            rows = cur.fetchall()
            assert len(rows) == 1  # both periods share the same filing
            assert rows[0][0].isoformat().startswith("2026-02-25T16:42:19")

            # Source raw_response ids point at real raw_responses.
            cur.execute(
                """
                SELECT COUNT(DISTINCT source_raw_response_id) FROM financial_facts
                WHERE company_id = %s;
                """,
                (company_id,),
            )
            assert cur.fetchone()[0] == 2  # quarter + annual

            # Ingest run succeeded.
            cur.execute(
                "SELECT status, counts, vendor, run_kind FROM ingest_runs ORDER BY id DESC LIMIT 1;"
            )
            status, run_counts, vendor, run_kind = cur.fetchone()
            assert status == "succeeded"
            assert vendor == "fmp"
            assert run_kind == "manual"
            assert run_counts["financial_facts_written"] == 36


def test_rerun_supersedes_old_rows_and_writes_new_ones() -> None:
    from arrow.agents.fmp_ingest import backfill_fmp_is

    with get_conn() as conn:
        _reset(conn)
        company_id = _seed_nvda(conn)

        with patch("arrow.ingest.fmp.client.FMPClient.get", new=_fake_fmp_get):
            backfill_fmp_is(conn, ["NVDA"])
            counts = backfill_fmp_is(conn, ["NVDA"])  # second run

        # Second run: superseded 36 from run 1, wrote 36 new.
        assert counts["financial_facts_superseded"] == 36
        assert counts["financial_facts_written"] == 36

        with conn.cursor() as cur:
            # Current rows: still 36 (the new ones).
            cur.execute(
                """
                SELECT count(*) FROM financial_facts
                WHERE company_id = %s AND superseded_at IS NULL;
                """,
                (company_id,),
            )
            assert cur.fetchone()[0] == 36

            # Total rows: 72 (36 superseded + 36 current).
            cur.execute(
                "SELECT count(*) FROM financial_facts WHERE company_id = %s;",
                (company_id,),
            )
            assert cur.fetchone()[0] == 72

            # Superseded rows have superseded_at set.
            cur.execute(
                """
                SELECT count(*) FROM financial_facts
                WHERE company_id = %s AND superseded_at IS NOT NULL;
                """,
                (company_id,),
            )
            assert cur.fetchone()[0] == 36


def test_verification_failure_rolls_back_and_marks_run_failed() -> None:
    from arrow.agents.fmp_ingest import backfill_fmp_is
    from arrow.normalize.financials.load import VerificationFailed

    def _bad_fmp_get(self, endpoint: str, **params) -> Response:  # noqa: ARG001
        broken = _q4_row()
        # Move gross_profit $5B off — well beyond tolerance.
        broken["grossProfit"] = broken["grossProfit"] + 5_000_000_000
        body = json.dumps([broken if params["period"] == "quarter" else _fy_row()]).encode()
        return Response(
            status=200, body=body, content_type="application/json",
            headers={"content-type": "application/json"},
            url="https://example/x",
        )

    with get_conn() as conn:
        _reset(conn)
        company_id = _seed_nvda(conn)

        with patch("arrow.ingest.fmp.client.FMPClient.get", new=_bad_fmp_get):
            with pytest.raises(VerificationFailed):
                backfill_fmp_is(conn, ["NVDA"])

        with conn.cursor() as cur:
            # Quarter payload transaction rolled back -> no facts from that raw_response.
            # (Annual may not have been reached.)
            cur.execute(
                "SELECT count(*) FROM financial_facts WHERE company_id = %s;",
                (company_id,),
            )
            fact_count = cur.fetchone()[0]
            assert fact_count == 0  # nothing persisted

            # raw_responses row for quarter was also rolled back (same txn).
            cur.execute("SELECT count(*) FROM raw_responses WHERE vendor = 'fmp';")
            assert cur.fetchone()[0] == 0

            # Ingest run status = failed, with structured error_details.
            cur.execute(
                "SELECT status, error_message, error_details FROM ingest_runs "
                "ORDER BY id DESC LIMIT 1;"
            )
            status, msg, details = cur.fetchone()
            assert status == "failed"
            assert "verification failed" in msg.lower()
            assert details["kind"] == "verification_failed"
            assert details["period_label"] == "FY2026 Q4"
            assert len(details["failed_ties"]) >= 1


def test_company_not_seeded_fails_cleanly() -> None:
    from arrow.agents.fmp_ingest import CompanyNotSeeded, backfill_fmp_is

    with get_conn() as conn:
        _reset(conn)
        # NOT seeding NVDA

        with patch("arrow.ingest.fmp.client.FMPClient.get", new=_fake_fmp_get):
            with pytest.raises(CompanyNotSeeded) as exc_info:
                backfill_fmp_is(conn, ["NVDA"])

        assert "NVDA" in str(exc_info.value)

        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, error_message FROM ingest_runs ORDER BY id DESC LIMIT 1;"
            )
            status, msg = cur.fetchone()
            assert status == "failed"
            assert "NVDA" in msg
