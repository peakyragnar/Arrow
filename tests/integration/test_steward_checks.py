"""Integration tests for the five additional steward checks.

Each check gets a trigger test (state that should produce a finding)
and a no-fire test (state that should not). Sharing one file because
the setup pattern (reset → seed → run check → assert) is identical.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from arrow.db.connection import get_conn
from arrow.db.migrations import apply as apply_migrations
from arrow.steward.registry import REGISTRY, Scope
from arrow.steward.runner import run_steward

# Self-register all checks.
import arrow.steward.checks  # noqa: F401


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _reset(conn: psycopg.Connection) -> None:
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE;")
        cur.execute("CREATE SCHEMA public;")
    apply_migrations(conn)


def _seed_company(conn: psycopg.Connection, *, ticker: str = "TEST", cik: int = 9999) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO companies (cik, ticker, name, fiscal_year_end_md) "
            "VALUES (%s, %s, %s, '12-31') RETURNING id;",
            (cik, ticker, f"{ticker} Inc."),
        )
        return cur.fetchone()[0]


def _seed_run(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ingest_runs (vendor, run_kind, status, started_at, finished_at) "
            "VALUES ('fmp', 'manual', 'succeeded', now(), now()) RETURNING id;"
        )
        return cur.fetchone()[0]


def _seed_artifact(
    conn: psycopg.Connection,
    *,
    run_id: int,
    company_id: int,
    ticker: str,
    artifact_type: str = "10k",
    form_family: str | None = "10-K",
    accession: str = "0000000000-00-000001",
) -> int:
    """Seed a SEC filing artifact. Bytes for raw/canonical hashes are
    derived from accession to keep them unique per call."""
    h = (accession.encode("utf-8") + b"\x00" * 32)[:32]
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO artifacts (
                ingest_run_id, artifact_type, source, ticker, company_id,
                form_family, accession_number, raw_hash, canonical_hash,
                title, published_at
            )
            VALUES (%s, %s, 'sec', %s, %s, %s, %s, %s, %s,
                    'Test filing', now())
            RETURNING id;
            """,
            (run_id, artifact_type, ticker, company_id, form_family,
             accession, h, h),
        )
        return cur.fetchone()[0]


def _seed_section(
    conn: psycopg.Connection,
    *,
    artifact_id: int,
    company_id: int,
    fiscal_period_key: str = "FY2024",
    form_family: str = "10-K",
    section_key: str = "item_7_mda",
    confidence: float = 0.9,
    created_offset_days: int = 0,
) -> int:
    """Seed an artifact_sections row. extraction_method is derived from
    section_key + confidence to satisfy the
    ``artifact_sections_confidence_method_contract`` CHECK:

      - section_key='unparsed_body' → method='unparsed_fallback', confidence=0.0
      - confidence >= 0.85          → method='deterministic'
      - 0 < confidence < 0.85       → method='repair'
    """
    if section_key == "unparsed_body":
        method = "unparsed_fallback"
        confidence = 0.0
    elif confidence >= 0.85:
        method = "deterministic"
    else:
        method = "repair"
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO artifact_sections (
                artifact_id, company_id, fiscal_period_key, form_family,
                section_key, section_title, text,
                start_offset, end_offset,
                extractor_version, confidence,
                extraction_method, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, 'text',
                    0, 4,
                    'v1', %s, %s,
                    now() - (%s::int * interval '1 day'))
            RETURNING id;
            """,
            (artifact_id, company_id, fiscal_period_key, form_family,
             section_key, section_key.replace('_', ' ').title(),
             confidence, method, created_offset_days),
        )
        return cur.fetchone()[0]


def _seed_text_unit(
    conn: psycopg.Connection,
    *,
    artifact_id: int,
    company_id: int,
    fiscal_period_key: str = "FY2024",
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO artifact_text_units (
                artifact_id, company_id, fiscal_period_key,
                unit_ordinal, unit_type, unit_key, unit_title, text,
                start_offset, end_offset,
                extractor_version, confidence, extraction_method
            )
            VALUES (%s, %s, %s, 1, 'press_release', 'pr1', 'Press Release 1', 'text',
                    0, 4,
                    'v1', 0.9, 'deterministic')
            RETURNING id;
            """,
            (artifact_id, company_id, fiscal_period_key),
        )
        return cur.fetchone()[0]


def _seed_flag(
    conn: psycopg.Connection,
    *,
    company_id: int,
    flag_type: str = "layer3_q_sum_vs_fy",
    severity: str = "warning",
    age_days: int = 30,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO data_quality_flags (
                company_id, statement, concept, fiscal_year,
                flag_type, severity, reason, flagged_at
            )
            VALUES (%s, 'income_statement', 'revenue', 2024,
                    %s, %s, 'unit-test seeded',
                    now() - (%s::int * interval '1 day'))
            RETURNING id;
            """,
            (company_id, flag_type, severity, age_days),
        )
        return cur.fetchone()[0]


def _findings(conn: psycopg.Connection, *, source_check: str) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, ticker, severity, summary, evidence "
            "FROM data_quality_findings "
            "WHERE source_check = %s AND status = 'open' ORDER BY id;",
            (source_check,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# unresolved_flags_aging
# ---------------------------------------------------------------------------


def test_unresolved_flags_aging_fires_on_aged_flag() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="TEST")
        _seed_flag(conn, company_id=cid, age_days=30, severity="investigate")

        run_steward(conn, scope=Scope.universe(), actor="system:check_runner")
        rows = _findings(conn, source_check="unresolved_flags_aging")
    assert len(rows) == 1
    r = rows[0]
    assert r["ticker"] == "TEST"
    assert r["severity"] == "investigate"  # inherited from the flag
    # Day-math is integer; 30 days ago via interval lands at 29 or 30
    # depending on clock fraction. Either is fine.
    assert "29 days" in r["summary"] or "30 days" in r["summary"]


def test_unresolved_flags_aging_skips_recent_flags() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="TEST")
        _seed_flag(conn, company_id=cid, age_days=3)

        run_steward(conn, scope=Scope.universe())
        rows = _findings(conn, source_check="unresolved_flags_aging")
    assert rows == []


def test_unresolved_flags_aging_skips_resolved_flags() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="TEST")
        flag_id = _seed_flag(conn, company_id=cid, age_days=30)
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE data_quality_flags "
                "SET resolved_at = now(), resolution = 'accept_as_is' "
                "WHERE id = %s;",
                (flag_id,),
            )

        run_steward(conn, scope=Scope.universe())
        rows = _findings(conn, source_check="unresolved_flags_aging")
    assert rows == []


# ---------------------------------------------------------------------------
# sec_artifact_orphans
# ---------------------------------------------------------------------------


def test_sec_artifact_orphans_fires_on_orphan() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="TEST")
        run_id = _seed_run(conn)
        _seed_artifact(
            conn, run_id=run_id, company_id=cid, ticker="TEST",
            artifact_type="10k", form_family="10-K",
        )

        run_steward(conn, scope=Scope.universe())
        rows = _findings(conn, source_check="sec_artifact_orphans")
    assert len(rows) == 1
    assert rows[0]["ticker"] == "TEST"
    # Summary uses artifact_type label (uppercased): '10K', '10Q',
    # 'PRESS_RELEASE'.
    assert "10K" in rows[0]["summary"]


def test_sec_artifact_orphans_skips_artifact_with_section() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="TEST")
        run_id = _seed_run(conn)
        aid = _seed_artifact(
            conn, run_id=run_id, company_id=cid, ticker="TEST",
            artifact_type="10k", form_family="10-K",
        )
        _seed_section(conn, artifact_id=aid, company_id=cid)

        run_steward(conn, scope=Scope.universe())
        rows = _findings(conn, source_check="sec_artifact_orphans")
    assert rows == []


def test_sec_artifact_orphans_skips_artifact_with_text_unit() -> None:
    """A press_release with text units (and no sections) should not be
    treated as orphan."""
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="TEST")
        run_id = _seed_run(conn)
        aid = _seed_artifact(
            conn, run_id=run_id, company_id=cid, ticker="TEST",
            artifact_type="press_release", form_family=None,
            accession="0000000000-00-000099",
        )
        _seed_text_unit(conn, artifact_id=aid, company_id=cid)

        run_steward(conn, scope=Scope.universe())
        rows = _findings(conn, source_check="sec_artifact_orphans")
    assert rows == []


# ---------------------------------------------------------------------------
# unparsed_body_fallback
# ---------------------------------------------------------------------------


def test_unparsed_body_fallback_fires_one_per_artifact() -> None:
    """Two artifacts each with an unparsed_body section → two findings
    (one per artifact). Schema allows only one section per
    (artifact_id, section_key), so 'multiple fallback per artifact' is
    structurally impossible — what we verify is per-artifact scope."""
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="TEST")
        run_id = _seed_run(conn)
        aid1 = _seed_artifact(
            conn, run_id=run_id, company_id=cid, ticker="TEST",
            artifact_type="10k", form_family="10-K",
            accession="0000000000-00-000001",
        )
        aid2 = _seed_artifact(
            conn, run_id=run_id, company_id=cid, ticker="TEST",
            artifact_type="10k", form_family="10-K",
            accession="0000000000-00-000002",
        )
        _seed_section(
            conn, artifact_id=aid1, company_id=cid,
            section_key="unparsed_body",
        )
        _seed_section(
            conn, artifact_id=aid2, company_id=cid,
            section_key="unparsed_body",
        )

        run_steward(conn, scope=Scope.universe())
        rows = _findings(conn, source_check="unparsed_body_fallback")
    assert len(rows) == 2
    for r in rows:
        assert r["evidence"]["fallback_section_count"] == 1


def test_unparsed_body_fallback_skips_artifact_with_real_sections() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="TEST")
        run_id = _seed_run(conn)
        aid = _seed_artifact(
            conn, run_id=run_id, company_id=cid, ticker="TEST",
            artifact_type="10k", form_family="10-K",
        )
        _seed_section(
            conn, artifact_id=aid, company_id=cid,
            section_key="item_7_mda",
        )

        run_steward(conn, scope=Scope.universe())
        rows = _findings(conn, source_check="unparsed_body_fallback")
    assert rows == []


# ---------------------------------------------------------------------------
# section_confidence_drift
# ---------------------------------------------------------------------------


def test_section_confidence_drift_fires_on_significant_drop() -> None:
    """Baseline window: stable high confidence (~0.97). Recent window:
    still deterministic (≥0.85) but lower (~0.86). Drop should exceed
    2σ and produce a finding.

    Both windows must use extraction_method='deterministic' to satisfy
    the artifact_sections_confidence_method_contract CHECK and to be
    seen by the check (which filters on that method).

    Each section needs its own artifact (UNIQUE on
    (artifact_id, section_key))."""
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="TEST")
        run_id = _seed_run(conn)

        # Baseline: 12 rows tightly clustered around 0.97
        for i in range(12):
            aid = _seed_artifact(
                conn, run_id=run_id, company_id=cid, ticker="TEST",
                form_family="10-K", accession=f"BASE-{i:04d}",
            )
            _seed_section(
                conn, artifact_id=aid, company_id=cid,
                section_key="item_1a_risk_factors",
                confidence=0.97 + (0.001 * (i - 6)),
                created_offset_days=45,
            )
        # Recent: 12 rows around 0.86 — well below baseline_mean - 2*stdev
        for i in range(12):
            aid = _seed_artifact(
                conn, run_id=run_id, company_id=cid, ticker="TEST",
                form_family="10-K", accession=f"RECENT-{i:04d}",
            )
            _seed_section(
                conn, artifact_id=aid, company_id=cid,
                section_key="item_1a_risk_factors",
                confidence=0.86,
                created_offset_days=5,
            )

        run_steward(conn, scope=Scope.universe())
        rows = _findings(conn, source_check="section_confidence_drift")
    assert len(rows) == 1
    r = rows[0]
    assert r["evidence"]["section_key"] == "item_1a_risk_factors"
    assert r["evidence"]["recent_n"] == 12
    assert r["evidence"]["baseline_n"] == 12
    assert r["evidence"]["recent_mean"] < r["evidence"]["baseline_mean"]
    assert r["evidence"]["z_score"] >= 2.0
    assert r["ticker"] is None  # corpus-wide finding


def test_section_confidence_drift_skips_when_window_too_small() -> None:
    """Below MIN_ROWS in either window, the test isn't fired."""
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="TEST")
        run_id = _seed_run(conn)

        # Only 5 in each window — below MIN_ROWS=10
        for i in range(5):
            aid = _seed_artifact(
                conn, run_id=run_id, company_id=cid, ticker="TEST",
                form_family="10-K", accession=f"SMALL-B-{i}",
            )
            _seed_section(
                conn, artifact_id=aid, company_id=cid,
                section_key="item_7_mda", confidence=0.95,
                created_offset_days=45,
            )
        for i in range(5):
            aid = _seed_artifact(
                conn, run_id=run_id, company_id=cid, ticker="TEST",
                form_family="10-K", accession=f"SMALL-R-{i}",
            )
            _seed_section(
                conn, artifact_id=aid, company_id=cid,
                section_key="item_7_mda", confidence=0.86,
                created_offset_days=5,
            )

        run_steward(conn, scope=Scope.universe())
        rows = _findings(conn, source_check="section_confidence_drift")
    assert rows == []


def test_section_confidence_drift_skips_when_no_drop() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="TEST")
        run_id = _seed_run(conn)

        # Both windows: same high confidence, no drift.
        for offset_label, offset in (("B", 45), ("R", 5)):
            for i in range(12):
                aid = _seed_artifact(
                    conn, run_id=run_id, company_id=cid, ticker="TEST",
                    form_family="10-K", accession=f"NODRIFT-{offset_label}-{i}",
                )
                _seed_section(
                    conn, artifact_id=aid, company_id=cid,
                    section_key="item_7_mda", confidence=0.95,
                    created_offset_days=offset,
                )

        run_steward(conn, scope=Scope.universe())
        rows = _findings(conn, source_check="section_confidence_drift")
    assert rows == []
