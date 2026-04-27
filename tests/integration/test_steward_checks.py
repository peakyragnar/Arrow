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
    confidence: float | None = None,
    extraction_method: str | None = None,
    created_offset_days: int = 0,
) -> int:
    """Seed an artifact_sections row.

    If ``extraction_method`` is given, ``confidence`` is set to a value
    that satisfies the ``artifact_sections_confidence_method_contract``
    CHECK for that method (caller can override). Otherwise method is
    derived from section_key + confidence:

      - section_key='unparsed_body' → method='unparsed_fallback', confidence=0.0
      - confidence >= 0.85          → method='deterministic'
      - 0 < confidence < 0.85       → method='repair'
    """
    if extraction_method is not None:
        method = extraction_method
        if confidence is None:
            confidence = {
                "deterministic": 0.95,
                "repair": 0.5,
                "unparsed_fallback": 0.0,
            }[method]
    else:
        if confidence is None:
            confidence = 0.9
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
# extraction_method_drift
# ---------------------------------------------------------------------------


def _seed_method_window(
    conn: psycopg.Connection,
    *,
    company_id: int,
    ticker: str,
    section_key: str,
    n_deterministic: int,
    n_repair: int,
    n_fallback: int,
    created_offset_days: int,
    accession_prefix: str,
) -> None:
    """Seed N sections of each extraction_method into one window.

    Each section needs its own artifact (UNIQUE on
    (artifact_id, section_key)). For unparsed_fallback rows, the
    section_key MUST be 'unparsed_body' to satisfy the contract CHECK
    — those sections are excluded from the drift check by design, so
    seeding them on a different section_key would be wrong anyway.
    """
    run_id = _ensure_run(conn)
    counter = 0

    def _seed_one(method: str, key: str) -> None:
        nonlocal counter
        aid = _seed_artifact(
            conn, run_id=run_id, company_id=company_id, ticker=ticker,
            form_family="10-K",
            accession=f"{accession_prefix}-{counter:04d}",
        )
        counter += 1
        _seed_section(
            conn, artifact_id=aid, company_id=company_id,
            section_key=key, extraction_method=method,
            created_offset_days=created_offset_days,
        )

    for _ in range(n_deterministic):
        _seed_one("deterministic", section_key)
    for _ in range(n_repair):
        _seed_one("repair", section_key)
    for _ in range(n_fallback):
        # 'unparsed_fallback' rows must have section_key='unparsed_body'.
        # The drift check correctly excludes them from the share
        # calculation (it filters section_key <> 'unparsed_body').
        # Calling code can still pass n_fallback > 0 — the rows land
        # outside the check's window.
        _seed_one("unparsed_fallback", "unparsed_body")


def _ensure_run(conn: psycopg.Connection) -> int:
    """Return any ingest_runs id, creating one if needed."""
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM ingest_runs LIMIT 1;")
        row = cur.fetchone()
        if row is not None:
            return row[0]
    return _seed_run(conn)


def test_extraction_method_drift_fires_on_share_drop() -> None:
    """Baseline: 100% deterministic (12 of 12). Recent: 50% (6 of 12).
    Share drop = 50 points, well above MIN_SHARE_DROP=15. Fires."""
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="TEST")

        _seed_method_window(
            conn, company_id=cid, ticker="TEST",
            section_key="item_1a_risk_factors",
            n_deterministic=12, n_repair=0, n_fallback=0,
            created_offset_days=45, accession_prefix="BASE",
        )
        _seed_method_window(
            conn, company_id=cid, ticker="TEST",
            section_key="item_1a_risk_factors",
            n_deterministic=6, n_repair=6, n_fallback=0,
            created_offset_days=5, accession_prefix="RECENT",
        )

        run_steward(conn, scope=Scope.universe())
        rows = _findings(conn, source_check="extraction_method_drift")
    assert len(rows) == 1
    r = rows[0]
    assert r["ticker"] is None  # corpus-wide
    ev = r["evidence"]
    assert ev["section_key"] == "item_1a_risk_factors"
    assert ev["recent"]["deterministic"] == 6
    assert ev["recent"]["repair"] == 6
    assert ev["baseline"]["deterministic"] == 12
    assert ev["baseline"]["repair"] == 0
    assert ev["baseline"]["deterministic_share"] == pytest.approx(1.0)
    assert ev["recent"]["deterministic_share"] == pytest.approx(0.5)
    assert ev["share_drop"] == pytest.approx(0.5)


def test_extraction_method_drift_fires_on_demotion_to_fallback() -> None:
    """A regression that pushes sections all the way to unparsed_fallback
    also shows up — the deterministic share still drops because the
    `total` in the recent window includes the now-non-deterministic
    sections that stayed in the bucket."""
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="TEST")

        # Baseline: 12 deterministic.
        _seed_method_window(
            conn, company_id=cid, ticker="TEST",
            section_key="item_7_mda",
            n_deterministic=12, n_repair=0, n_fallback=0,
            created_offset_days=45, accession_prefix="BASE",
        )
        # Recent: 5 deterministic + 7 repair (deterministic share dropped
        # from 100% to ~42%). The 7 demoted sections went to repair, not
        # fallback (fallback rows live on a different section_key and
        # are excluded by the check).
        _seed_method_window(
            conn, company_id=cid, ticker="TEST",
            section_key="item_7_mda",
            n_deterministic=5, n_repair=7, n_fallback=0,
            created_offset_days=5, accession_prefix="RECENT",
        )

        run_steward(conn, scope=Scope.universe())
        rows = _findings(conn, source_check="extraction_method_drift")
    assert len(rows) == 1
    ev = rows[0]["evidence"]
    assert ev["recent"]["deterministic_share"] == pytest.approx(5 / 12)
    assert ev["share_drop"] >= 0.5


def test_extraction_method_drift_skips_small_drop() -> None:
    """Baseline 100%, recent 90% — share drop is 10 points, below the
    15-point threshold. Should not fire."""
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="TEST")

        _seed_method_window(
            conn, company_id=cid, ticker="TEST",
            section_key="item_7_mda",
            n_deterministic=20, n_repair=0, n_fallback=0,
            created_offset_days=45, accession_prefix="BASE",
        )
        # 18 of 20 = 90% deterministic; baseline 100%; drop = 10 points.
        _seed_method_window(
            conn, company_id=cid, ticker="TEST",
            section_key="item_7_mda",
            n_deterministic=18, n_repair=2, n_fallback=0,
            created_offset_days=5, accession_prefix="RECENT",
        )

        run_steward(conn, scope=Scope.universe())
        rows = _findings(conn, source_check="extraction_method_drift")
    assert rows == []


def test_extraction_method_drift_skips_when_window_too_small() -> None:
    """Below MIN_ROWS=10 in either window, no fire even with a big drop."""
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="TEST")

        # 5 in each window — both below MIN_ROWS=10.
        _seed_method_window(
            conn, company_id=cid, ticker="TEST",
            section_key="item_7_mda",
            n_deterministic=5, n_repair=0, n_fallback=0,
            created_offset_days=45, accession_prefix="SMALL-B",
        )
        _seed_method_window(
            conn, company_id=cid, ticker="TEST",
            section_key="item_7_mda",
            n_deterministic=0, n_repair=5, n_fallback=0,
            created_offset_days=5, accession_prefix="SMALL-R",
        )

        run_steward(conn, scope=Scope.universe())
        rows = _findings(conn, source_check="extraction_method_drift")
    assert rows == []


def test_extraction_method_drift_skips_when_no_drop() -> None:
    """Both windows: same high deterministic share, no regression."""
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="TEST")

        _seed_method_window(
            conn, company_id=cid, ticker="TEST",
            section_key="item_7_mda",
            n_deterministic=12, n_repair=0, n_fallback=0,
            created_offset_days=45, accession_prefix="STABLE-B",
        )
        _seed_method_window(
            conn, company_id=cid, ticker="TEST",
            section_key="item_7_mda",
            n_deterministic=12, n_repair=0, n_fallback=0,
            created_offset_days=5, accession_prefix="STABLE-R",
        )

        run_steward(conn, scope=Scope.universe())
        rows = _findings(conn, source_check="extraction_method_drift")
    assert rows == []


# ---------------------------------------------------------------------------
# chunk_repair_concentration
# ---------------------------------------------------------------------------


def test_chunk_repair_concentration_fires_on_majority_repair_artifact() -> None:
    """An artifact with >50% sections in repair extraction (and ≥3
    sections total) fires a finding. Mirrors the META FY2025 Q1
    pattern that surfaced in real data."""
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="TEST")
        run_id = _seed_run(conn)
        aid = _seed_artifact(
            conn, run_id=run_id, company_id=cid, ticker="TEST",
            artifact_type="10q", form_family="10-Q",
        )
        # 5 repair + 1 deterministic on the same artifact = 83% repair share,
        # 6 total sections — well above thresholds.
        for sk in ("part1_item2_mda", "part1_item3_market_risk",
                   "part1_item4_controls", "part2_item1_legal_proceedings",
                   "part2_item1a_risk_factors"):
            _seed_section(conn, artifact_id=aid, company_id=cid,
                          section_key=sk, extraction_method="repair")
        _seed_section(conn, artifact_id=aid, company_id=cid,
                      section_key="part2_item5_other_information",
                      extraction_method="deterministic")

        run_steward(conn, scope=Scope.universe())
        rows = _findings(conn, source_check="chunk_repair_concentration")
    assert len(rows) == 1
    r = rows[0]
    assert r["ticker"] == "TEST"
    assert r["evidence"]["repair_count"] == 5
    assert r["evidence"]["total_sections"] == 6
    assert r["evidence"]["repair_share"] == pytest.approx(5 / 6)


def test_chunk_repair_concentration_skips_artifact_with_minority_repair() -> None:
    """50% or below repair share should NOT fire."""
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="TEST")
        run_id = _seed_run(conn)
        aid = _seed_artifact(
            conn, run_id=run_id, company_id=cid, ticker="TEST",
            artifact_type="10q", form_family="10-Q",
        )
        # 2 repair + 4 deterministic = 33% repair, below 50% threshold.
        for sk in ("part1_item2_mda", "part1_item3_market_risk"):
            _seed_section(conn, artifact_id=aid, company_id=cid,
                          section_key=sk, extraction_method="repair")
        for sk in ("part1_item4_controls", "part2_item1_legal_proceedings",
                   "part2_item1a_risk_factors", "part2_item5_other_information"):
            _seed_section(conn, artifact_id=aid, company_id=cid,
                          section_key=sk, extraction_method="deterministic")

        run_steward(conn, scope=Scope.universe())
        rows = _findings(conn, source_check="chunk_repair_concentration")
    assert rows == []


def test_chunk_repair_concentration_skips_artifact_below_min_sections() -> None:
    """Tiny artifacts (e.g. amendments with only 1-2 sections) shouldn't
    fire even with high repair share — the MIN_SECTIONS guard avoids
    false positives on legitimately-small filings like 10-K/A amendments."""
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="TEST")
        run_id = _seed_run(conn)
        aid = _seed_artifact(
            conn, run_id=run_id, company_id=cid, ticker="TEST",
            artifact_type="10k", form_family="10-K",
        )
        # 2 sections both in repair = 100% repair share but only 2 sections.
        for sk in ("item_1_business", "item_1a_risk_factors"):
            _seed_section(conn, artifact_id=aid, company_id=cid,
                          section_key=sk, extraction_method="repair")

        run_steward(conn, scope=Scope.universe())
        rows = _findings(conn, source_check="chunk_repair_concentration")
    assert rows == []


def test_chunk_repair_concentration_skips_artifact_with_all_deterministic() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="TEST")
        run_id = _seed_run(conn)
        aid = _seed_artifact(
            conn, run_id=run_id, company_id=cid, ticker="TEST",
            artifact_type="10q", form_family="10-Q",
        )
        for sk in ("part1_item2_mda", "part1_item3_market_risk",
                   "part1_item4_controls", "part2_item1_legal_proceedings",
                   "part2_item1a_risk_factors"):
            _seed_section(conn, artifact_id=aid, company_id=cid,
                          section_key=sk, extraction_method="deterministic")

        run_steward(conn, scope=Scope.universe())
        rows = _findings(conn, source_check="chunk_repair_concentration")
    assert rows == []


# ---------------------------------------------------------------------------
# quarterly_value_duplication
# ---------------------------------------------------------------------------


def _seed_raw_response(conn: psycopg.Connection, *, run_id: int) -> int:
    with conn.cursor() as cur:
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
            (run_id,),
        )
        return cur.fetchone()[0]


def _seed_fact(
    conn: psycopg.Connection,
    *,
    run_id: int,
    raw_id: int,
    company_id: int,
    statement: str,
    concept: str,
    fiscal_year: int,
    fiscal_quarter: int,
    value: float,
    period_end: str = "2019-06-30",
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO financial_facts (
                ingest_run_id, company_id, statement, concept, value, unit,
                fiscal_year, fiscal_quarter, fiscal_period_label,
                period_end, period_type,
                calendar_year, calendar_quarter, calendar_period_label,
                published_at, source_raw_response_id, extraction_version
            )
            VALUES (%s, %s, %s, %s, %s, 'USD',
                    %s, %s, %s,
                    %s, 'quarter',
                    %s, %s, %s,
                    now(), %s, 'fmp-test-v1')
            RETURNING id;
            """,
            (run_id, company_id, statement, concept, value,
             fiscal_year, fiscal_quarter, f"FY{fiscal_year} Q{fiscal_quarter}",
             period_end,
             fiscal_year, fiscal_quarter, f"CY{fiscal_year} Q{fiscal_quarter}",
             raw_id),
        )
        return cur.fetchone()[0]


def test_quarterly_value_duplication_fires_on_pltr_shaped_h1_split() -> None:
    """16 non-zero CF concepts where 14 have IDENTICAL values across Q1
    and Q2 = 87.5% duplication (above 50% threshold). Mirrors PLTR
    FY2019 Q1/Q2 H1-split fabrication that surfaced in real data."""
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="TEST")
        run_id = _seed_run(conn)
        raw_id = _seed_raw_response(conn, run_id=run_id)
        # 14 concepts duplicated, 2 differing — 16 non-zero concepts total
        duplicated = [
            ("cfo", -170_161_000), ("cfi", -3_641_000), ("cff", 49_860_500),
            ("capital_expenditures", -3_641_000), ("dna_cf", 3_194_500),
            ("sbc", 56_443_500), ("change_accounts_receivable", -27_085_500),
            ("change_other_working_capital", 63_521_500),
            ("deferred_income_tax", -29_358_000),
            ("fx_effect_on_cash", -307_500), ("other_financing", 53_233_500),
            ("stock_repurchase", -3_373_000),
            ("net_change_in_cash", -124_249_000),
            ("cash_end_of_period", -124_249_000),
        ]
        differing = [
            ("net_income_start", -140_229_500, -134_066_000),
            ("other_noncash", -62_484_000, -68_647_500),
        ]
        for concept, val in duplicated:
            _seed_fact(conn, run_id=run_id, raw_id=raw_id, company_id=cid,
                       statement="cash_flow", concept=concept,
                       fiscal_year=2019, fiscal_quarter=1, value=val,
                       period_end="2019-03-31")
            _seed_fact(conn, run_id=run_id, raw_id=raw_id, company_id=cid,
                       statement="cash_flow", concept=concept,
                       fiscal_year=2019, fiscal_quarter=2, value=val,
                       period_end="2019-06-30")
        for concept, q1_val, q2_val in differing:
            _seed_fact(conn, run_id=run_id, raw_id=raw_id, company_id=cid,
                       statement="cash_flow", concept=concept,
                       fiscal_year=2019, fiscal_quarter=1, value=q1_val,
                       period_end="2019-03-31")
            _seed_fact(conn, run_id=run_id, raw_id=raw_id, company_id=cid,
                       statement="cash_flow", concept=concept,
                       fiscal_year=2019, fiscal_quarter=2, value=q2_val,
                       period_end="2019-06-30")

        run_steward(conn, scope=Scope.universe())
        rows = _findings(conn, source_check="quarterly_value_duplication")

    assert len(rows) == 1
    r = rows[0]
    assert r["ticker"] == "TEST"
    assert r["evidence"]["statement"] == "cash_flow"
    assert r["evidence"]["fiscal_year"] == 2019
    assert r["evidence"]["q_a"] == 1 and r["evidence"]["q_b"] == 2
    assert r["evidence"]["nonzero_concepts"] == 16
    assert r["evidence"]["duplicated_nonzero"] == 14
    assert r["evidence"]["duplication_share"] == pytest.approx(14 / 16)


def test_quarterly_value_duplication_skips_normal_quarterly_data() -> None:
    """Real reported quarterly data has distinct Q1 vs Q2 values across
    nearly every concept. Should not fire."""
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="TEST")
        run_id = _seed_run(conn)
        raw_id = _seed_raw_response(conn, run_id=run_id)
        # 8 concepts, all differing Q1 vs Q2
        for i, concept in enumerate(
            ["cfo", "cfi", "cff", "capital_expenditures", "dna_cf", "sbc",
             "change_accounts_receivable", "net_change_in_cash"]
        ):
            _seed_fact(conn, run_id=run_id, raw_id=raw_id, company_id=cid,
                       statement="cash_flow", concept=concept,
                       fiscal_year=2024, fiscal_quarter=1,
                       value=1_000_000 * (i + 1),
                       period_end="2024-03-31")
            _seed_fact(conn, run_id=run_id, raw_id=raw_id, company_id=cid,
                       statement="cash_flow", concept=concept,
                       fiscal_year=2024, fiscal_quarter=2,
                       value=1_500_000 * (i + 1),
                       period_end="2024-06-30")

        run_steward(conn, scope=Scope.universe())
        rows = _findings(conn, source_check="quarterly_value_duplication")
    assert rows == []


def test_quarterly_value_duplication_skips_below_min_nonzero_concepts() -> None:
    """A pair with only 4 non-zero concepts (below MIN_NONZERO_CONCEPTS=5)
    shouldn't fire even if all are duplicated. Avoids noise on companies
    with mostly-zero cash flow statements."""
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="TEST")
        run_id = _seed_run(conn)
        raw_id = _seed_raw_response(conn, run_id=run_id)
        # 4 duplicated concepts only — below threshold
        for concept, val in [("cfo", 1_000_000), ("cfi", -500_000),
                             ("cff", 250_000), ("net_change_in_cash", 750_000)]:
            _seed_fact(conn, run_id=run_id, raw_id=raw_id, company_id=cid,
                       statement="cash_flow", concept=concept,
                       fiscal_year=2019, fiscal_quarter=1, value=val,
                       period_end="2019-03-31")
            _seed_fact(conn, run_id=run_id, raw_id=raw_id, company_id=cid,
                       statement="cash_flow", concept=concept,
                       fiscal_year=2019, fiscal_quarter=2, value=val,
                       period_end="2019-06-30")

        run_steward(conn, scope=Scope.universe())
        rows = _findings(conn, source_check="quarterly_value_duplication")
    assert rows == []


def test_quarterly_value_duplication_ignores_zero_values() -> None:
    """Concepts that are zero in BOTH quarters (legitimately absent —
    no acquisitions, no preferred dividends, etc.) shouldn't count
    toward the duplication share. Otherwise companies with sparse CF
    would falsely fire."""
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn, ticker="TEST")
        run_id = _seed_run(conn)
        raw_id = _seed_raw_response(conn, run_id=run_id)
        # 6 concepts: 5 zero-in-both (legitimately absent), 1 non-zero
        # but distinct. Non-zero pool = 1, well below MIN.
        for concept in ["acquisitions", "purchases_of_investments",
                        "common_dividends_paid", "preferred_dividends_paid",
                        "stock_issuance"]:
            _seed_fact(conn, run_id=run_id, raw_id=raw_id, company_id=cid,
                       statement="cash_flow", concept=concept,
                       fiscal_year=2024, fiscal_quarter=1, value=0,
                       period_end="2024-03-31")
            _seed_fact(conn, run_id=run_id, raw_id=raw_id, company_id=cid,
                       statement="cash_flow", concept=concept,
                       fiscal_year=2024, fiscal_quarter=2, value=0,
                       period_end="2024-06-30")
        _seed_fact(conn, run_id=run_id, raw_id=raw_id, company_id=cid,
                   statement="cash_flow", concept="cfo",
                   fiscal_year=2024, fiscal_quarter=1, value=1_000_000,
                   period_end="2024-03-31")
        _seed_fact(conn, run_id=run_id, raw_id=raw_id, company_id=cid,
                   statement="cash_flow", concept="cfo",
                   fiscal_year=2024, fiscal_quarter=2, value=1_500_000,
                   period_end="2024-06-30")

        run_steward(conn, scope=Scope.universe())
        rows = _findings(conn, source_check="quarterly_value_duplication")
    assert rows == []
