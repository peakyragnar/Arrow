"""Integration tests for standalone FMP↔XBRL reconciliation (Build Order 9.5).

Seeds a company + inserts matching/diverging facts, then runs
reconcile_fmp_vs_xbrl with a synthetic XBRL payload. Verifies:
  - clean run succeeds with matched counts
  - divergent data surfaces in the `divergences` list + marks run failed
  - missing company raises cleanly
  - re-run is read-only (no financial_facts mutation)

Warning: DROPs and recreates the `public` schema.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import patch

import psycopg
import pytest

from arrow.agents.fmp_reconcile import CompanyNotSeeded, reconcile_fmp_vs_xbrl
from arrow.db.connection import get_conn
from arrow.db.migrations import apply
from arrow.ingest.common.raw_responses import write_raw_response
from arrow.ingest.sec.company_facts import (
    COMPANY_FACTS_ENDPOINT_TEMPLATE,
    CompanyFactsFetch,
)


def _reset(conn: psycopg.Connection) -> None:
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE;")
        cur.execute("CREATE SCHEMA public;")
    apply(conn)
    conn.autocommit = False


def _seed(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO companies (cik, ticker, name, fiscal_year_end_md) "
            "VALUES (1045810, 'NVDA', 'NVIDIA CORP', '01-31') RETURNING id;"
        )
        cid = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO ingest_runs (run_kind, vendor, status) "
            "VALUES ('manual', 'test', 'started') RETURNING id;"
        )
        rid = cur.fetchone()[0]
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


def _insert_is_fact(conn, cid, rid, raw_id, concept, value, period_end, period_type,
                    fiscal_year, fiscal_quarter) -> None:
    label = (f"FY{fiscal_year} Q{fiscal_quarter}" if fiscal_quarter
             else f"FY{fiscal_year}")
    cy = period_end.year
    cq = (period_end.month - 1) // 3 + 1
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
            (cid, concept, value,
             fiscal_year, fiscal_quarter, label,
             period_end, period_type,
             cy, cq, f"CY{cy} Q{cq}",
             datetime(2025, 2, 1, tzinfo=timezone.utc),
             raw_id, rid),
        )
    conn.commit()


def _stub_fetch(xbrl_payload: dict):
    """Factory for a fetch_company_facts stub that returns our payload."""
    def _stub(conn, *, cik, ingest_run_id, http):  # noqa: ARG001
        body = json.dumps(xbrl_payload).encode()
        endpoint = COMPANY_FACTS_ENDPOINT_TEMPLATE.format(cik10=f"{cik:010d}")
        raw_id = write_raw_response(
            conn, ingest_run_id=ingest_run_id, vendor="sec",
            endpoint=endpoint, params={"cik": cik},
            request_url=f"https://data.sec.gov/{endpoint}",
            http_status=200, content_type="application/json",
            response_headers={"content-type": "application/json"},
            body=body, cache_path=None,
        )
        return CompanyFactsFetch(raw_response_id=raw_id, payload=xbrl_payload)
    return _stub


# ---------------------------------------------------------------------------


def test_clean_reconcile_succeeds_and_marks_run_succeeded() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid, rid, raw_id = _seed(conn)
        # Store a single revenue anchor for FY2026 Q3.
        _insert_is_fact(conn, cid, rid, raw_id, "revenue",
                        Decimal("57006000000"), date(2025, 10, 26),
                        "quarter", 2026, 3)

        xbrl = {
            "cik": 1045810, "entityName": "NVDA",
            "facts": {"us-gaap": {
                "Revenues": {"units": {"USD": [
                    {"start": "2025-07-28", "end": "2025-10-26",
                     "val": 57006000000, "accn": "X", "fy": 2026,
                     "fp": "Q3", "form": "10-Q", "filed": "2025-11-19"},
                ]}},
            }},
        }
        with patch(
            "arrow.agents.fmp_reconcile.fetch_company_facts",
            new=_stub_fetch(xbrl),
        ):
            result = reconcile_fmp_vs_xbrl(conn, ["NVDA"])

        assert result["status"] == "succeeded"
        assert result["is_anchors_matched"] == 1
        assert result["is_anchors_checked"] == 1
        assert result["divergences"] == []

        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, counts FROM ingest_runs WHERE id = %s;",
                (result["ingest_run_id"],),
            )
            status, counts = cur.fetchone()
            assert status == "succeeded"
            assert counts["is_anchors_matched"] == 1


def test_divergence_surfaces_and_marks_run_failed() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid, rid, raw_id = _seed(conn)
        _insert_is_fact(conn, cid, rid, raw_id, "revenue",
                        Decimal("57006000000"), date(2025, 10, 26),
                        "quarter", 2026, 3)

        xbrl = {
            "cik": 1045810, "entityName": "NVDA",
            "facts": {"us-gaap": {
                "Revenues": {"units": {"USD": [
                    {"start": "2025-07-28", "end": "2025-10-26",
                     "val": 57100000000,  # $94M off
                     "accn": "X", "fy": 2026, "fp": "Q3",
                     "form": "10-Q", "filed": "2025-11-19"},
                ]}},
            }},
        }
        with patch(
            "arrow.agents.fmp_reconcile.fetch_company_facts",
            new=_stub_fetch(xbrl),
        ):
            result = reconcile_fmp_vs_xbrl(conn, ["NVDA"])

        assert result["status"] == "failed"
        assert len(result["divergences"]) == 1
        d = result["divergences"][0]
        assert d["statement"] == "income_statement"
        assert d["concept"] == "revenue"
        assert d["fmp_value"] == "57006000000.0000"
        assert d["xbrl_value"] == "57100000000"

        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, error_message, error_details FROM ingest_runs "
                "WHERE id = %s;",
                (result["ingest_run_id"],),
            )
            status, msg, details = cur.fetchone()
            assert status == "failed"
            assert "divergence" in msg.lower()
            assert details["kind"] == "reconciliation_divergences"
            assert len(details["divergences"]) == 1


def test_unknown_ticker_raises() -> None:
    with get_conn() as conn:
        _reset(conn)

        with patch(
            "arrow.agents.fmp_reconcile.fetch_company_facts",
            new=_stub_fetch({"cik": 0, "facts": {"us-gaap": {}}}),
        ):
            with pytest.raises(CompanyNotSeeded):
                reconcile_fmp_vs_xbrl(conn, ["NVDA"])


def test_reconcile_is_read_only_financial_facts() -> None:
    """Reconciliation must not insert/modify rows in financial_facts."""
    with get_conn() as conn:
        _reset(conn)
        cid, rid, raw_id = _seed(conn)
        _insert_is_fact(conn, cid, rid, raw_id, "revenue",
                        Decimal("57006000000"), date(2025, 10, 26),
                        "quarter", 2026, 3)

        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM financial_facts;")
            before = cur.fetchone()[0]

        xbrl = {"cik": 1045810, "facts": {"us-gaap": {
            "Revenues": {"units": {"USD": [
                {"start": "2025-07-28", "end": "2025-10-26",
                 "val": 57006000000, "accn": "X", "fy": 2026,
                 "fp": "Q3", "form": "10-Q", "filed": "2025-11-19"},
            ]}},
        }}}
        with patch(
            "arrow.agents.fmp_reconcile.fetch_company_facts",
            new=_stub_fetch(xbrl),
        ):
            reconcile_fmp_vs_xbrl(conn, ["NVDA"])

        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM financial_facts;")
            after = cur.fetchone()[0]
        assert before == after  # no mutation
