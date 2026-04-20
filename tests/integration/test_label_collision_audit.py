"""Regression: no two period_ends should share a fiscal_period_label.

A mis-derivation in periods.derive can silently produce two rows with
the same fiscal_period_label but different period_ends (different
filings). This test would have caught the 52/53-week drift bug found
post-Slice-2a. Cheap to run; runs in the integration layer because it
needs actual rows in financial_facts.

Also re-verifies the four Layer-1 IS subtotal ties hold per (period_end,
period_type) tuple — catches any drift between ingest-time verification
and stored values.

Warning: DROPs and recreates the `public` schema.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from decimal import Decimal
from unittest.mock import patch

import psycopg

from arrow.db.connection import get_conn
from arrow.db.migrations import apply
from arrow.ingest.common.http import Response
from arrow.ingest.common.raw_responses import write_raw_response
from arrow.ingest.sec.company_facts import (
    CompanyFactsFetch,
    COMPANY_FACTS_ENDPOINT_TEMPLATE,
)
from arrow.normalize.financials.verify_is import verify_is_ties


def _empty_xbrl_fetch(conn, *, cik, ingest_run_id, http):  # noqa: ARG001
    import json as _json
    payload = {"cik": cik, "entityName": "NVDA", "facts": {"us-gaap": {}}}
    body = _json.dumps(payload).encode()
    endpoint = COMPANY_FACTS_ENDPOINT_TEMPLATE.format(cik10=f"{cik:010d}")
    raw_id = write_raw_response(
        conn, ingest_run_id=ingest_run_id, vendor="sec", endpoint=endpoint,
        params={"cik": cik}, request_url=f"https://data.sec.gov/{endpoint}",
        http_status=200, content_type="application/json",
        response_headers={"content-type": "application/json"},
        body=body, cache_path=None,
    )
    return CompanyFactsFetch(raw_response_id=raw_id, payload=payload)


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


def _drift_stress_rows() -> list[dict]:
    """Three periods that historically collided under the +1 algorithm:
    FY2000 Q2 (drift to Aug 1) + FY2000 Q3 (canonical Oct 31) + FY2006 Q1
    (drift to May 1). Numbers are consistent so all four ties pass."""
    def mk(date_: str, period: str, fiscal_year: str) -> dict:
        rev = 1000000000
        cogs = 400000000
        gp = rev - cogs
        opex = 200000000
        oi = gp - opex
        interest_exp = 10000000
        interest_inc = 5000000
        ebt = oi - interest_exp + interest_inc  # 395M
        tax = 80000000
        cont = ebt - tax
        disc = 0
        ni = cont + disc
        return {
            "date": date_, "period": period, "fiscalYear": fiscal_year,
            "symbol": "NVDA",
            "filingDate": date_, "acceptedDate": f"{date_} 12:00:00",
            "revenue": rev, "costOfRevenue": cogs, "grossProfit": gp,
            "researchAndDevelopmentExpenses": 100000000,
            "sellingGeneralAndAdministrativeExpenses": 50000000,
            "operatingExpenses": opex, "operatingIncome": oi,
            "interestIncome": interest_inc, "interestExpense": interest_exp,
            "incomeBeforeTax": ebt, "incomeTaxExpense": tax,
            "netIncomeFromContinuingOperations": cont,
            "netIncomeFromDiscontinuedOperations": disc,
            "netIncome": ni, "eps": 0.5, "epsDiluted": 0.49,
            "weightedAverageShsOut": 2000000000,
            "weightedAverageShsOutDil": 2050000000,
        }
    return [
        mk("1999-08-01", "Q2", "2000"),  # drift case: would have been mis-labeled Q3
        mk("1999-10-31", "Q3", "2000"),  # canonical: would have collided with drift
        mk("2005-05-01", "Q1", "2006"),  # drift case: would have been mis-labeled Q2
    ]


def test_no_two_periods_share_a_fiscal_period_label() -> None:
    from arrow.agents.fmp_ingest import backfill_fmp_is

    rows = _drift_stress_rows()

    def _fake_get(self, endpoint: str, **params) -> Response:  # noqa: ARG001
        import json
        if params["period"] == "quarter":
            body = json.dumps(rows).encode()
        else:
            body = b"[]"  # no annual rows for this fixture
        return Response(
            status=200, body=body, content_type="application/json",
            headers={"content-type": "application/json"}, url="https://mock/",
        )

    with get_conn() as conn:
        _reset(conn)
        _seed_nvda(conn)
        with patch("arrow.ingest.fmp.client.FMPClient.get", new=_fake_get), patch(
            "arrow.agents.fmp_ingest.fetch_company_facts", new=_empty_xbrl_fetch
        ):
            # since_date override to admit the pre-2021 drift fixtures.
            backfill_fmp_is(conn, ["NVDA"], since_date=date(1999, 1, 1))

        with conn.cursor() as cur:
            cur.execute("""
                SELECT fiscal_period_label, period_type,
                       count(DISTINCT period_end) AS n,
                       array_agg(DISTINCT period_end) AS ends
                FROM financial_facts
                WHERE superseded_at IS NULL
                GROUP BY fiscal_period_label, period_type
                HAVING count(DISTINCT period_end) > 1;
            """)
            collisions = cur.fetchall()

    assert collisions == [], (
        f"Label collisions found (period derivation bug regression): {collisions}"
    )


def test_stored_facts_still_tie_after_ingest() -> None:
    from arrow.agents.fmp_ingest import backfill_fmp_is

    rows = _drift_stress_rows()

    def _fake_get(self, endpoint: str, **params) -> Response:  # noqa: ARG001
        import json
        body = json.dumps(rows if params["period"] == "quarter" else []).encode()
        return Response(
            status=200, body=body, content_type="application/json",
            headers={"content-type": "application/json"}, url="https://mock/",
        )

    with get_conn() as conn:
        _reset(conn)
        _seed_nvda(conn)
        with patch("arrow.ingest.fmp.client.FMPClient.get", new=_fake_get), patch(
            "arrow.agents.fmp_ingest.fetch_company_facts", new=_empty_xbrl_fetch
        ):
            # since_date override to admit the pre-2021 drift fixtures.
            backfill_fmp_is(conn, ["NVDA"], since_date=date(1999, 1, 1))

        with conn.cursor() as cur:
            cur.execute("""
                SELECT period_end, period_type, concept, value
                FROM financial_facts
                WHERE superseded_at IS NULL AND statement = 'income_statement';
            """)
            rows_out = cur.fetchall()

    by_period: dict[tuple, dict[str, Decimal]] = defaultdict(dict)
    for pe, pt, concept, value in rows_out:
        by_period[(pe, pt)][concept] = value

    failures = []
    for key, values in by_period.items():
        fails = verify_is_ties(values)
        if fails:
            failures.append((key, fails))

    assert failures == [], f"stored facts do not tie: {failures}"
