"""Integration tests for Layer 5 — FMP-vs-SEC-XBRL reconciliation.

Seeds a company and a minimal set of financial_facts via raw SQL, then
calls reconcile_company with a synthetic companyfacts payload.
Exercises the matching logic (concept lookup, duration window, latest-
filed tie-break) and the divergence detection.

Warning: DROPs and recreates the `public` schema.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import psycopg

from arrow.db.connection import get_conn
from arrow.db.migrations import apply
from arrow.reconcile.fmp_vs_xbrl import reconcile_company


def _reset(conn: psycopg.Connection) -> None:
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE;")
        cur.execute("CREATE SCHEMA public;")
    apply(conn)
    conn.autocommit = False


def _seed(conn: psycopg.Connection) -> tuple[int, int, int]:
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


def _ins(conn, company_id, rid, raw_id, concept, value, period_end, period_type,
         fiscal_year, fiscal_quarter, unit="USD"):
    fy = fiscal_year
    label = f"FY{fy} Q{fiscal_quarter}" if fiscal_quarter else f"FY{fy}"
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
                %s, 'income_statement', %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, 'fmp-is-v1',
                %s
            );
            """,
            (company_id, concept, value, unit,
             fy, fiscal_quarter, label, period_end, period_type,
             cy, cq, f"CY{cy} Q{cq}",
             datetime(2025, 2, 1, tzinfo=timezone.utc),
             raw_id, rid),
        )
    conn.commit()


def _companyfacts_with(entries: dict) -> dict:
    """Build a minimal companyfacts payload: {tag: [entries]}.

    Each entry dict is inserted verbatim into units["USD"] (or units["shares"]
    or units["USD/shares"] based on the tag).
    """
    # Route units by tag (ugly but fine for tests).
    us_gaap = {}
    for tag, entry_list in entries.items():
        if tag in ("EarningsPerShareBasic", "EarningsPerShareDiluted"):
            us_gaap[tag] = {"units": {"USD/shares": entry_list}}
        elif tag.startswith("WeightedAverageNumberOf"):
            us_gaap[tag] = {"units": {"shares": entry_list}}
        else:
            us_gaap[tag] = {"units": {"USD": entry_list}}
    return {"cik": 1045810, "entityName": "NVIDIA", "facts": {"us-gaap": us_gaap}}


def test_matching_q3_revenue_reconciles() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid, rid, raw_id = _seed(conn)
        # Store NVDA Q3 FY2026 revenue (3-month discrete).
        _ins(conn, cid, rid, raw_id, "revenue", Decimal("57006000000"),
             date(2025, 10, 26), "quarter", 2026, 3)

        # XBRL has 3-month revenue for the same period_end.
        cf = _companyfacts_with({
            "Revenues": [
                {"start": "2025-07-28", "end": "2025-10-26", "val": 57006000000,
                 "accn": "X", "fy": 2026, "fp": "Q3", "form": "10-Q",
                 "filed": "2025-11-19"},
                # YTD 9-month (must be ignored — duration ~90 days window won't include 89+):
                {"start": "2025-01-27", "end": "2025-10-26", "val": 147811000000,
                 "accn": "X", "fy": 2026, "fp": "Q3", "form": "10-Q",
                 "filed": "2025-11-19"},
            ],
        })
        result = reconcile_company(conn, company_id=cid,
                                    extraction_version="fmp-is-v1", companyfacts=cf)
    assert result.checked == 1
    assert result.matched == 1
    assert result.divergences == []


def test_divergence_beyond_tolerance_is_recorded() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid, rid, raw_id = _seed(conn)
        # FMP stored as 57.006B; XBRL pretends it's 57.100B (94M delta, beyond tolerance)
        _ins(conn, cid, rid, raw_id, "revenue", Decimal("57006000000"),
             date(2025, 10, 26), "quarter", 2026, 3)
        cf = _companyfacts_with({
            "Revenues": [
                {"start": "2025-07-28", "end": "2025-10-26", "val": 57100000000,
                 "accn": "X", "fy": 2026, "fp": "Q3", "form": "10-Q",
                 "filed": "2025-11-19"},
            ],
        })
        result = reconcile_company(conn, company_id=cid,
                                    extraction_version="fmp-is-v1", companyfacts=cf)
    assert result.checked == 1
    assert result.matched == 0
    assert len(result.divergences) == 1
    d = result.divergences[0]
    assert d.concept == "revenue"
    assert d.xbrl_tag == "Revenues"
    assert d.delta == Decimal("94000000")


def test_q4_is_skipped_not_checked() -> None:
    """Q4 isn't separately reported in XBRL (only Q1/Q2/Q3 + FY)."""
    with get_conn() as conn:
        _reset(conn)
        cid, rid, raw_id = _seed(conn)
        _ins(conn, cid, rid, raw_id, "revenue", Decimal("68127000000"),
             date(2026, 1, 25), "quarter", 2026, 4)
        # Even with a matching XBRL fact, Q4 is explicitly skipped.
        cf = _companyfacts_with({
            "Revenues": [
                {"start": "2025-10-27", "end": "2026-01-25", "val": 68127000000,
                 "accn": "Y", "fy": 2026, "fp": "Q4", "form": "10-K",
                 "filed": "2026-02-25"},
            ],
        })
        result = reconcile_company(conn, company_id=cid,
                                    extraction_version="fmp-is-v1", companyfacts=cf)
    assert result.skipped_q4 == 1
    assert result.checked == 0


def test_missing_xbrl_concept_is_suppressed_not_failure() -> None:
    """Filers omit optional concepts. Missing XBRL entry = skip, not divergence."""
    with get_conn() as conn:
        _reset(conn)
        cid, rid, raw_id = _seed(conn)
        _ins(conn, cid, rid, raw_id, "rd", Decimal("5000000000"),
             date(2025, 10, 26), "quarter", 2026, 3)
        cf = _companyfacts_with({})  # empty — no facts
        result = reconcile_company(conn, company_id=cid,
                                    extraction_version="fmp-is-v1", companyfacts=cf)
    assert result.skipped_no_xbrl == 1
    assert result.divergences == []


def test_multiple_tags_tried_in_order() -> None:
    """Revenue can be Revenues or RevenueFromContractWithCustomerExcludingAssessedTax."""
    with get_conn() as conn:
        _reset(conn)
        cid, rid, raw_id = _seed(conn)
        _ins(conn, cid, rid, raw_id, "revenue", Decimal("1000000000"),
             date(2021, 7, 31), "quarter", 2022, 2)
        # Only the fallback tag is present in XBRL (older ASC 606 concept).
        cf = _companyfacts_with({
            "RevenueFromContractWithCustomerExcludingAssessedTax": [
                {"start": "2021-05-03", "end": "2021-07-31", "val": 1000000000,
                 "accn": "X", "fy": 2022, "fp": "Q2", "form": "10-Q",
                 "filed": "2021-08-18"},
            ],
        })
        result = reconcile_company(conn, company_id=cid,
                                    extraction_version="fmp-is-v1", companyfacts=cf)
    assert result.matched == 1
    assert result.divergences[-0:] == []


def test_latest_filed_wins_on_restatement() -> None:
    """When XBRL has multiple entries for the same (concept, period_end),
    we use the one with the latest `filed` — representing the most recent
    authoritative value (original or restatement)."""
    with get_conn() as conn:
        _reset(conn)
        cid, rid, raw_id = _seed(conn)
        _ins(conn, cid, rid, raw_id, "revenue", Decimal("1000000000"),
             date(2025, 10, 26), "quarter", 2026, 3)
        cf = _companyfacts_with({
            "Revenues": [
                # First filing: would disagree with FMP if chosen.
                {"start": "2025-07-28", "end": "2025-10-26", "val": 999000000,
                 "accn": "X1", "fy": 2026, "fp": "Q3", "form": "10-Q",
                 "filed": "2025-11-19"},
                # Later restatement: matches FMP.
                {"start": "2025-07-28", "end": "2025-10-26", "val": 1000000000,
                 "accn": "X2", "fy": 2026, "fp": "Q3", "form": "10-Q/A",
                 "filed": "2026-01-10"},
            ],
        })
        result = reconcile_company(conn, company_id=cid,
                                    extraction_version="fmp-is-v1", companyfacts=cf)
    assert result.matched == 1
    assert result.divergences == []
