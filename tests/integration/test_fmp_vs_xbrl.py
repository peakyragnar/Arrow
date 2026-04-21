"""Integration tests for Layer 5 — anchor cross-check of FMP vs SEC XBRL.

Seeds a company and inserts financial_facts via raw SQL, then calls
reconcile_anchors with a synthetic companyfacts payload. Exercises:
  - Direct XBRL lookup for Q1/Q2/Q3/annual anchors
  - Duration-window filtering (picks 3-month over 9-month YTD)
  - Q4 derivation (FY − 9M YTD)
  - Divergence detection + debug info
  - Anchors with no XBRL counterpart reported as informational
  - Non-anchor concepts (e.g. rd, eps_basic) are not checked

Warning: DROPs and recreates the `public` schema.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import psycopg

from arrow.db.connection import get_conn
from arrow.db.migrations import apply
from arrow.reconcile.fmp_vs_xbrl import IS_ANCHORS, reconcile_anchors


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


def _ins(
    conn, company_id, rid, raw_id, concept, value, period_end, period_type,
    fiscal_year, fiscal_quarter, unit="USD",
) -> None:
    label = (
        f"FY{fiscal_year} Q{fiscal_quarter}"
        if fiscal_quarter is not None
        else f"FY{fiscal_year}"
    )
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
            (
                company_id, concept, value, unit,
                fiscal_year, fiscal_quarter, label, period_end, period_type,
                cy, cq, f"CY{cy} Q{cq}",
                datetime(2025, 2, 1, tzinfo=timezone.utc),
                raw_id, rid,
            ),
        )
    conn.commit()


def _companyfacts(entries: dict) -> dict:
    """Minimal companyfacts with default USD unit for all tags."""
    us_gaap = {tag: {"units": {"USD": e}} for tag, e in entries.items()}
    return {"cik": 1045810, "entityName": "NVIDIA", "facts": {"us-gaap": us_gaap}}


# ---------------------------------------------------------------------------
# Direct Q1-Q3 anchor match
# ---------------------------------------------------------------------------


def test_q3_revenue_anchor_matches_xbrl_direct() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid, rid, raw_id = _seed(conn)
        _ins(conn, cid, rid, raw_id, "revenue", Decimal("57006000000"),
             date(2025, 10, 26), "quarter", 2026, 3)

        cf = _companyfacts({
            "Revenues": [
                {"start": "2025-07-28", "end": "2025-10-26", "val": 57006000000,
                 "accn": "X", "fy": 2026, "fp": "Q3", "form": "10-Q",
                 "filed": "2025-11-19"},
                # 9-month YTD (must be ignored for Q3 direct match):
                {"start": "2025-01-27", "end": "2025-10-26", "val": 147811000000,
                 "accn": "X", "fy": 2026, "fp": "Q3", "form": "10-Q",
                 "filed": "2025-11-19"},
            ],
        })
        result = reconcile_anchors(
            conn, company_id=cid, extraction_version="fmp-is-v1",
            companyfacts=cf,
        )
    assert result.anchors_checked == 1
    assert result.anchors_matched == 1
    assert result.divergences == []


def test_annual_anchor_matches_xbrl_direct() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid, rid, raw_id = _seed(conn)
        _ins(conn, cid, rid, raw_id, "net_income", Decimal("120067000000"),
             date(2026, 1, 25), "annual", 2026, None)

        cf = _companyfacts({
            "NetIncomeLoss": [
                {"start": "2025-01-27", "end": "2026-01-25", "val": 120067000000,
                 "accn": "Y", "fy": 2026, "fp": "FY", "form": "10-K",
                 "filed": "2026-02-25"},
            ],
        })
        result = reconcile_anchors(
            conn, company_id=cid, extraction_version="fmp-is-v1",
            companyfacts=cf,
        )
    assert result.anchors_checked == 1
    assert result.anchors_matched == 1


# ---------------------------------------------------------------------------
# Q4 derivation (FY − 9M YTD)
# ---------------------------------------------------------------------------


def test_q4_anchor_derived_as_fy_minus_9m() -> None:
    """Q4 discrete = FY − 9M YTD from XBRL, matched by end-date.

    The reconciler looks up the stored Q3 period_end for this company+FY
    in financial_facts, then finds the XBRL 9M YTD fact ending on that
    date. End-date matching is critical — fy/fp tags in XBRL denote the
    filing's period, not the fact's period, so comparative data in a
    current-year 10-Q is tagged with the current filing's fy/fp.
    """
    with get_conn() as conn:
        _reset(conn)
        cid, rid, raw_id = _seed(conn)
        # Q3 FY2026 3M discrete: the reconciler uses this row's
        # period_end to find the 9M YTD XBRL fact for the Q4 derivation.
        _ins(conn, cid, rid, raw_id, "revenue", Decimal("57006000000"),
             date(2025, 10, 26), "quarter", 2026, 3)
        # Q4 FY2026: 68.127B = 215.938 FY − 147.811 9M YTD.
        _ins(conn, cid, rid, raw_id, "revenue", Decimal("68127000000"),
             date(2026, 1, 25), "quarter", 2026, 4)

        cf = _companyfacts({
            "Revenues": [
                # FY fact (matches Q4 / annual end date)
                {"start": "2025-01-27", "end": "2026-01-25", "val": 215938000000,
                 "accn": "Y", "fy": 2026, "fp": "FY", "form": "10-K",
                 "filed": "2026-02-25"},
                # 3M discrete Q3 (for direct Q3 match)
                {"start": "2025-07-28", "end": "2025-10-26", "val": 57006000000,
                 "accn": "X", "fy": 2026, "fp": "Q3", "form": "10-Q",
                 "filed": "2025-11-19"},
                # 9M YTD for FY2026 Q3 (used in Q4 derivation; end=2025-10-26)
                {"start": "2025-01-27", "end": "2025-10-26", "val": 147811000000,
                 "accn": "X", "fy": 2026, "fp": "Q3", "form": "10-Q",
                 "filed": "2025-11-19"},
                # Prior-year 9M YTD comparative with fy=2026, fp=Q3 tags —
                # MUST NOT be picked (end-date doesn't match Q3 period_end).
                {"start": "2024-01-29", "end": "2024-10-27", "val": 91166000000,
                 "accn": "X", "fy": 2026, "fp": "Q3", "form": "10-Q",
                 "filed": "2025-11-19"},
            ],
        })
        result = reconcile_anchors(
            conn, company_id=cid, extraction_version="fmp-is-v1",
            companyfacts=cf,
        )
    # Both Q3 and Q4 anchors check (Q3 direct, Q4 derived).
    assert result.anchors_checked == 2
    assert result.anchors_matched == 2
    assert result.divergences == []


def test_q4_divergence_surfaces_derivation_label() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid, rid, raw_id = _seed(conn)
        # Q3 stored (reconciler needs its period_end for Q4 derivation).
        _ins(conn, cid, rid, raw_id, "revenue", Decimal("57006000000"),
             date(2025, 10, 26), "quarter", 2026, 3)
        # FMP Q4 says 69B — but FY−9M derives 68.127B. Delta 873M beyond tolerance.
        _ins(conn, cid, rid, raw_id, "revenue", Decimal("69000000000"),
             date(2026, 1, 25), "quarter", 2026, 4)

        cf = _companyfacts({
            "Revenues": [
                {"start": "2025-01-27", "end": "2026-01-25", "val": 215938000000,
                 "accn": "Y", "fy": 2026, "fp": "FY", "form": "10-K",
                 "filed": "2026-02-25"},
                {"start": "2025-07-28", "end": "2025-10-26", "val": 57006000000,
                 "accn": "X", "fy": 2026, "fp": "Q3", "form": "10-Q",
                 "filed": "2025-11-19"},
                {"start": "2025-01-27", "end": "2025-10-26", "val": 147811000000,
                 "accn": "X", "fy": 2026, "fp": "Q3", "form": "10-Q",
                 "filed": "2025-11-19"},
            ],
        })
        result = reconcile_anchors(
            conn, company_id=cid, extraction_version="fmp-is-v1",
            companyfacts=cf,
        )
    q4_divs = [d for d in result.divergences if d.fiscal_quarter == 4]
    assert len(q4_divs) == 1
    d = q4_divs[0]
    assert d.concept == "revenue"
    assert d.period_type == "quarter"
    assert d.derivation == "q4_derived_fy_minus_9m"
    assert d.xbrl_value == Decimal("68127000000")
    assert d.fmp_value == Decimal("69000000000")


# ---------------------------------------------------------------------------
# Divergence + debug info
# ---------------------------------------------------------------------------


def test_divergence_beyond_tolerance_records_full_debug_info() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid, rid, raw_id = _seed(conn)
        _ins(conn, cid, rid, raw_id, "revenue", Decimal("57006000000"),
             date(2025, 10, 26), "quarter", 2026, 3)

        cf = _companyfacts({
            "Revenues": [
                {"start": "2025-07-28", "end": "2025-10-26", "val": 57100000000,
                 "accn": "0001045810-25-000230", "fy": 2026, "fp": "Q3",
                 "form": "10-Q", "filed": "2025-11-19"},
            ],
        })
        result = reconcile_anchors(
            conn, company_id=cid, extraction_version="fmp-is-v1",
            companyfacts=cf,
        )
    d = result.divergences[0]
    assert d.fmp_value == Decimal("57006000000")
    assert d.xbrl_value == Decimal("57100000000")
    assert d.delta == Decimal("94000000")
    assert d.xbrl_tag == "Revenues"
    assert d.xbrl_accn == "0001045810-25-000230"
    assert d.xbrl_filed == "2025-11-19"
    assert d.derivation == "direct"
    assert d.fiscal_year == 2026
    assert d.fiscal_quarter == 3


# ---------------------------------------------------------------------------
# Absent from XBRL (noted, not failed)
# ---------------------------------------------------------------------------


def test_anchor_without_xbrl_counterpart_is_noted_not_failed() -> None:
    """A filer that doesn't report an anchor tag produces no divergence;
    the absence is recorded in anchors_not_in_xbrl for operator review."""
    with get_conn() as conn:
        _reset(conn)
        cid, rid, raw_id = _seed(conn)
        _ins(conn, cid, rid, raw_id, "gross_profit", Decimal("40000000000"),
             date(2025, 10, 26), "quarter", 2026, 3)

        cf = _companyfacts({})  # no GrossProfit tag
        result = reconcile_anchors(
            conn, company_id=cid, extraction_version="fmp-is-v1",
            companyfacts=cf,
        )
    assert result.divergences == []
    assert result.anchors_checked == 0
    assert len(result.anchors_not_in_xbrl) == 1
    concept, pe, pt = result.anchors_not_in_xbrl[0]
    assert concept == "gross_profit"
    assert pe == date(2025, 10, 26)
    assert pt == "quarter"


# ---------------------------------------------------------------------------
# Non-anchor concepts are NOT checked
# ---------------------------------------------------------------------------


def test_non_anchor_concepts_are_not_checked() -> None:
    """rd, eps_basic, shares_*, etc. are not in IS_ANCHORS → reconciler ignores."""
    with get_conn() as conn:
        _reset(conn)
        cid, rid, raw_id = _seed(conn)
        _ins(conn, cid, rid, raw_id, "rd", Decimal("5000000000"),
             date(2025, 10, 26), "quarter", 2026, 3)
        _ins(conn, cid, rid, raw_id, "eps_basic", Decimal("1.23"),
             date(2025, 10, 26), "quarter", 2026, 3, unit="USD/share")
        _ins(conn, cid, rid, raw_id, "shares_basic_weighted_avg",
             Decimal("24000000000"),
             date(2025, 10, 26), "quarter", 2026, 3, unit="shares")

        cf = _companyfacts({})  # doesn't matter; concepts aren't anchors
        result = reconcile_anchors(
            conn, company_id=cid, extraction_version="fmp-is-v1",
            companyfacts=cf,
        )
    # Reconciler SELECT filters by concept = ANY(IS_ANCHORS), so non-anchor
    # rows are never even returned to the reconciliation loop.
    assert result.anchors_with_fmp_stored == 0
    assert result.anchors_checked == 0


def test_is_anchors_cover_the_current_expected_subtotals() -> None:
    assert set(IS_ANCHORS) == {
        "revenue",
        "gross_profit",
        "operating_income",
        "ebt_incl_unusual",
        "net_income",
        "net_income_attributable_to_parent",
    }


# ---------------------------------------------------------------------------
# Restatement handling
# ---------------------------------------------------------------------------


def test_latest_filed_wins_on_restatement() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid, rid, raw_id = _seed(conn)
        _ins(conn, cid, rid, raw_id, "revenue", Decimal("1000000000"),
             date(2025, 10, 26), "quarter", 2026, 3)

        cf = _companyfacts({
            "Revenues": [
                {"start": "2025-07-28", "end": "2025-10-26", "val": 999000000,
                 "accn": "X1", "fy": 2026, "fp": "Q3", "form": "10-Q",
                 "filed": "2025-11-19"},
                {"start": "2025-07-28", "end": "2025-10-26", "val": 1000000000,
                 "accn": "X2", "fy": 2026, "fp": "Q3", "form": "10-Q/A",
                 "filed": "2026-01-10"},
            ],
        })
        result = reconcile_anchors(
            conn, company_id=cid, extraction_version="fmp-is-v1",
            companyfacts=cf,
        )
    assert result.anchors_matched == 1
    assert result.divergences == []
