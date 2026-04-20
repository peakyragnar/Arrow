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


def _bs_q4_row() -> dict:
    """NVDA FY2026 Q4 balance sheet (2026-01-25). All subtotals tie."""
    return {
        "date": "2026-01-25", "period": "Q4", "fiscalYear": "2026",
        "symbol": "NVDA", "filingDate": "2026-02-25",
        "acceptedDate": "2026-02-25 16:42:19",
        "cashAndCashEquivalents": 10605000000,
        "shortTermInvestments": 51951000000,
        "accountsReceivables": 38466000000,
        "inventory": 21403000000,
        "prepaids": 0,
        "otherCurrentAssets": 3180000000,
        "totalCurrentAssets": 125605000000,
        "propertyPlantEquipmentNet": 13250000000,
        "longTermInvestments": 22251000000,
        "goodwill": 20832000000,
        "intangibleAssets": 3306000000,
        "taxAssets": 13258000000,
        "otherNonCurrentAssets": 8301000000,
        "totalAssets": 206803000000,
        "accountPayables": 9812000000,
        "otherPayables": 2669000000,
        "accruedExpenses": 9239000000,
        "shortTermDebt": 999000000,
        "capitalLeaseObligationsCurrent": 372000000,
        "deferredRevenue": 1379000000,
        "otherCurrentLiabilities": 7693000000,
        "totalCurrentLiabilities": 32163000000,
        "longTermDebt": 7469000000,
        "capitalLeaseObligationsNonCurrent": 2572000000,
        "deferredRevenueNonCurrent": 1193000000,
        "deferredTaxLiabilitiesNonCurrent": 1774000000,
        "otherNonCurrentLiabilities": 4339000000,
        "totalLiabilities": 49510000000,
        "preferredStock": 0,
        "commonStock": 24000000,
        "additionalPaidInCapital": 10118000000,
        "retainedEarnings": 146973000000,
        "treasuryStock": 0,
        "accumulatedOtherComprehensiveIncomeLoss": 178000000,
        "minorityInterest": 0,
        "totalEquity": 157293000000,
        "totalLiabilitiesAndTotalEquity": 206803000000,
    }


def _fake_fmp_get(self, endpoint: str, **params) -> Response:  # noqa: ARG001
    """Mocked FMPClient.get — routes by endpoint + period."""
    if endpoint == "income-statement":
        if params.get("period") == "quarter":
            rows = [_q4_row()]
        elif params.get("period") == "annual":
            rows = [_fy_row()]
        else:
            raise AssertionError(f"unexpected IS params: {params}")
    elif endpoint == "balance-sheet-statement":
        # periods.md § 6.3: the 10-K's BS IS the Q4 BS snapshot. FMP reports
        # the same numbers under period=Q4 (quarter endpoint) and period=FY
        # (annual endpoint); we emit a 'quarter'/Q4 row and an 'annual' row
        # at the same period_end.
        bs_row = _bs_q4_row()
        if params.get("period") == "annual":
            bs_row = dict(bs_row)
            bs_row["period"] = "FY"
        rows = [bs_row]
    else:
        raise AssertionError(f"unexpected endpoint: {endpoint}")
    body = json.dumps(rows).encode()
    return Response(
        status=200,
        body=body,
        content_type="application/json",
        headers={"content-type": "application/json"},
        url=f"https://financialmodelingprep.com/stable/{endpoint}?symbol=NVDA&period={params.get('period','?')}",
    )


def _empty_xbrl_fetch(conn, *, cik, ingest_run_id, http):  # noqa: ARG001
    """Stub fetch_company_facts that writes a raw_responses row + returns an
    empty companyfacts payload (no us-gaap facts). Reconcile will skip every
    comparison — valid pass-through for tests that aren't exercising XBRL
    matching specifically."""
    from arrow.ingest.common.raw_responses import write_raw_response
    from arrow.ingest.sec.company_facts import (
        CompanyFactsFetch,
        COMPANY_FACTS_ENDPOINT_TEMPLATE,
    )
    payload = {"cik": cik, "entityName": "NVDA", "facts": {"us-gaap": {}}}
    body = json.dumps(payload).encode()
    endpoint = COMPANY_FACTS_ENDPOINT_TEMPLATE.format(cik10=f"{cik:010d}")
    raw_id = write_raw_response(
        conn,
        ingest_run_id=ingest_run_id,
        vendor="sec",
        endpoint=endpoint,
        params={"cik": cik},
        request_url=f"https://data.sec.gov/{endpoint}",
        http_status=200,
        content_type="application/json",
        response_headers={"content-type": "application/json"},
        body=body,
        cache_path=None,
    )
    return CompanyFactsFetch(raw_response_id=raw_id, payload=payload)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_backfill_writes_raw_and_facts_end_to_end() -> None:
    from arrow.agents.fmp_ingest import backfill_fmp_statements

    with get_conn() as conn:
        _reset(conn)
        company_id = _seed_nvda(conn)

        with patch("arrow.ingest.fmp.client.FMPClient.get", new=_fake_fmp_get), patch(
            "arrow.agents.fmp_ingest.fetch_company_facts", new=_empty_xbrl_fetch
        ):
            counts = backfill_fmp_statements(conn, ["NVDA"])

        # 5 raw_responses = FMP IS quarter + IS annual + BS quarter + BS annual + SEC companyfacts
        assert counts["raw_responses"] == 5
        # 4 FMP rows (1 per payload × 4 payloads)
        assert counts["rows_processed"] == 4
        # IS: 18 buckets × 2 periods = 36
        assert counts["is_facts_written"] == 36
        assert counts["is_facts_superseded"] == 0
        # BS: all populated buckets × 2 periods. NVDA Q4 FY26 populates every
        # bucket in the mapper (some with 0).
        assert counts["bs_facts_written"] > 0
        assert counts["bs_facts_superseded"] == 0

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*) FROM financial_facts
                WHERE company_id = %s AND superseded_at IS NULL
                  AND statement = 'income_statement';
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
            # 4 distinct: IS quarter, IS annual, BS quarter, BS annual.
            cur.execute(
                """
                SELECT COUNT(DISTINCT source_raw_response_id) FROM financial_facts
                WHERE company_id = %s;
                """,
                (company_id,),
            )
            assert cur.fetchone()[0] == 4

            # Ingest run succeeded.
            cur.execute(
                "SELECT status, counts, vendor, run_kind FROM ingest_runs ORDER BY id DESC LIMIT 1;"
            )
            status, run_counts, vendor, run_kind = cur.fetchone()
            assert status == "succeeded"
            assert vendor == "fmp"
            assert run_kind == "manual"
            assert run_counts["is_facts_written"] == 36
            assert run_counts["bs_facts_written"] > 0


def test_rerun_supersedes_old_rows_and_writes_new_ones() -> None:
    from arrow.agents.fmp_ingest import backfill_fmp_statements

    with get_conn() as conn:
        _reset(conn)
        company_id = _seed_nvda(conn)

        with patch("arrow.ingest.fmp.client.FMPClient.get", new=_fake_fmp_get), patch(
            "arrow.agents.fmp_ingest.fetch_company_facts", new=_empty_xbrl_fetch
        ):
            first_counts = backfill_fmp_statements(conn, ["NVDA"])
            counts = backfill_fmp_statements(conn, ["NVDA"])  # second run

        # Second run supersedes everything from run 1 and writes the same count new.
        assert counts["is_facts_superseded"] == first_counts["is_facts_written"]
        assert counts["is_facts_written"] == first_counts["is_facts_written"]
        assert counts["bs_facts_superseded"] == first_counts["bs_facts_written"]
        assert counts["bs_facts_written"] == first_counts["bs_facts_written"]

        with conn.cursor() as cur:
            total_current = first_counts["is_facts_written"] + first_counts["bs_facts_written"]

            cur.execute(
                """
                SELECT count(*) FROM financial_facts
                WHERE company_id = %s AND superseded_at IS NULL;
                """,
                (company_id,),
            )
            assert cur.fetchone()[0] == total_current  # still just the new ones

            cur.execute(
                "SELECT count(*) FROM financial_facts WHERE company_id = %s;",
                (company_id,),
            )
            assert cur.fetchone()[0] == 2 * total_current  # new + superseded

            cur.execute(
                """
                SELECT count(*) FROM financial_facts
                WHERE company_id = %s AND superseded_at IS NOT NULL;
                """,
                (company_id,),
            )
            assert cur.fetchone()[0] == total_current


def test_verification_failure_rolls_back_and_marks_run_failed() -> None:
    from arrow.agents.fmp_ingest import backfill_fmp_statements
    from arrow.normalize.financials.load import VerificationFailed

    def _bad_fmp_get(self, endpoint: str, **params) -> Response:  # noqa: ARG001
        # IS payload with grossProfit broken → Layer 1 IS fires before BS
        # ingest even starts.
        if endpoint == "income-statement":
            broken = _q4_row()
            broken["grossProfit"] = broken["grossProfit"] + 5_000_000_000
            rows = [broken if params["period"] == "quarter" else _fy_row()]
        elif endpoint == "balance-sheet-statement":
            rows = [_bs_q4_row()]
        else:
            raise AssertionError(f"unexpected endpoint: {endpoint}")
        body = json.dumps(rows).encode()
        return Response(
            status=200, body=body, content_type="application/json",
            headers={"content-type": "application/json"},
            url="https://example/x",
        )

    with get_conn() as conn:
        _reset(conn)
        company_id = _seed_nvda(conn)

        with patch("arrow.ingest.fmp.client.FMPClient.get", new=_bad_fmp_get), patch(
            "arrow.agents.fmp_ingest.fetch_company_facts", new=_empty_xbrl_fetch
        ):
            with pytest.raises(VerificationFailed):
                backfill_fmp_statements(conn, ["NVDA"])

        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM financial_facts WHERE company_id = %s;",
                (company_id,),
            )
            assert cur.fetchone()[0] == 0  # IS txn rolled back; BS never reached

            cur.execute("SELECT count(*) FROM raw_responses WHERE vendor = 'fmp';")
            assert cur.fetchone()[0] == 0  # IS raw_response rolled back too

            cur.execute(
                "SELECT status, error_message, error_details FROM ingest_runs "
                "ORDER BY id DESC LIMIT 1;"
            )
            status, msg, details = cur.fetchone()
            assert status == "failed"
            assert "verification failed" in msg.lower()
            assert details["kind"] == "is_verification_failed"
            assert details["period_label"] == "FY2026 Q4"
            assert len(details["failed_ties"]) >= 1


def test_company_not_seeded_fails_cleanly() -> None:
    from arrow.agents.fmp_ingest import CompanyNotSeeded, backfill_fmp_statements

    with get_conn() as conn:
        _reset(conn)
        # NOT seeding NVDA

        with patch("arrow.ingest.fmp.client.FMPClient.get", new=_fake_fmp_get), patch(
            "arrow.agents.fmp_ingest.fetch_company_facts", new=_empty_xbrl_fetch
        ):
            with pytest.raises(CompanyNotSeeded) as exc_info:
                backfill_fmp_statements(conn, ["NVDA"])

        assert "NVDA" in str(exc_info.value)

        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, error_message FROM ingest_runs ORDER BY id DESC LIMIT 1;"
            )
            status, msg = cur.fetchone()
            assert status == "failed"
            assert "NVDA" in msg
