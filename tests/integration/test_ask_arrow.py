"""Integration tests for the deterministic analyst runtime slice.

Covers the revenue-driver recipe end-to-end against a fully seeded test
database: parser -> readiness -> retrieve -> ground -> synthesize -> verify.
"""

from __future__ import annotations

import os
from datetime import date, datetime, timezone

import psycopg
import pytest

from arrow.analysis.company_context import (
    IntentError,
    RuntimeTrace,
    build_revenue_driver_packet,
    parse_revenue_driver_intent,
    synthesize_revenue_driver_answer,
)
from arrow.db.connection import get_conn
from arrow.db.migrations import apply

os.environ.setdefault("FMP_API_KEY", "test-key-for-integration")

H32 = b"\x00" * 32


def _reset(conn: psycopg.Connection) -> None:
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE;")
        cur.execute("CREATE SCHEMA public;")
    apply(conn)
    from scripts.apply_views import main as apply_views_main
    apply_views_main()


def _seed_company(
    conn: psycopg.Connection,
    *,
    ticker: str = "TEST",
    name: str = "Test Co",
    cik: int = 9999999,
    fiscal_year_end_md: str = "12-31",
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO companies (cik, ticker, name, fiscal_year_end_md)
            VALUES (%s, %s, %s, %s)
            RETURNING id;
            """,
            (cik, ticker, name, fiscal_year_end_md),
        )
        return cur.fetchone()[0]


def _seed_run_and_raw(conn: psycopg.Connection) -> tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ingest_runs (run_kind, vendor, status, finished_at) "
            "VALUES ('manual', 'fmp', 'succeeded', now()) RETURNING id;"
        )
        run_id = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO raw_responses (
                ingest_run_id, vendor, endpoint, params_hash,
                http_status, content_type, body_jsonb,
                raw_hash, canonical_hash
            )
            VALUES (%s, 'fmp', 'income-statement', %s, 200, 'application/json',
                    '{}'::jsonb, %s, %s)
            RETURNING id;
            """,
            (run_id, H32, H32, H32),
        )
        raw_id = cur.fetchone()[0]
    return run_id, raw_id


def _seed_annual_fact(
    conn: psycopg.Connection,
    *,
    company_id: int,
    concept: str,
    statement: str,
    value: int,
    fiscal_year: int,
    period_end: date,
    run_id: int,
    raw_id: int,
) -> int:
    published_at = datetime(fiscal_year + 1, 2, 25, tzinfo=timezone.utc)
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
                %s, NULL, %s,
                %s, 'annual',
                %s, 4, %s,
                %s, %s, 'fmp-test-v1', %s
            )
            RETURNING id;
            """,
            (
                company_id, statement, concept, value,
                fiscal_year, f"FY{fiscal_year}",
                period_end,
                period_end.year, f"CY{period_end.year} Q4",
                published_at, raw_id, run_id,
            ),
        )
        return cur.fetchone()[0]


def _seed_segment_fact(
    conn: psycopg.Connection,
    *,
    company_id: int,
    fiscal_year: int,
    period_end: date,
    dimension_type: str,
    dimension_key: str,
    dimension_label: str,
    value: int,
    run_id: int,
    raw_id: int,
) -> int:
    published_at = datetime(fiscal_year + 1, 2, 25, tzinfo=timezone.utc)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO financial_facts (
                company_id, statement, concept, value, unit,
                fiscal_year, fiscal_quarter, fiscal_period_label,
                period_end, period_type,
                calendar_year, calendar_quarter, calendar_period_label,
                published_at, source_raw_response_id, extraction_version,
                ingest_run_id,
                dimension_type, dimension_key, dimension_label, dimension_source
            ) VALUES (
                %s, 'segment', 'revenue', %s, 'USD',
                %s, NULL, %s,
                %s, 'annual',
                %s, 4, %s,
                %s, %s, 'fmp-test-v1', %s,
                %s, %s, %s, 'fmp:revenue-product-segmentation'
            )
            RETURNING id;
            """,
            (
                company_id, value,
                fiscal_year, f"FY{fiscal_year}",
                period_end,
                period_end.year, f"CY{period_end.year} Q4",
                published_at, raw_id, run_id,
                dimension_type, dimension_key, dimension_label,
            ),
        )
        return cur.fetchone()[0]


def _seed_10k_with_mda(
    conn: psycopg.Connection,
    *,
    company_id: int,
    ticker: str,
    fiscal_year: int,
    period_end: date,
    run_id: int,
    mda_text: str,
) -> tuple[int, int, int]:
    published_at = datetime(fiscal_year + 1, 2, 25, tzinfo=timezone.utc)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO artifacts (
                ingest_run_id, artifact_type, source, raw_hash, canonical_hash,
                ticker, fiscal_year, fiscal_period_label, period_type, period_end,
                published_at, company_id, fiscal_period_key, form_family,
                cik, accession_number
            ) VALUES (
                %s, '10k', 'sec', %s, %s,
                %s, %s, %s, 'annual', %s,
                %s, %s, %s, '10-K',
                %s, %s
            )
            RETURNING id;
            """,
            (
                run_id, _hash_for(f"10k-{ticker}-{fiscal_year}"),
                _hash_for(f"10k-{ticker}-{fiscal_year}-c"),
                ticker, fiscal_year, f"FY{fiscal_year}", period_end,
                published_at, company_id, f"FY{fiscal_year}",
                str(9999999), f"0000000000-{fiscal_year}-000001",
            ),
        )
        artifact_id = cur.fetchone()[0]

        cur.execute(
            """
            INSERT INTO artifact_sections (
                artifact_id, company_id, fiscal_period_key, form_family,
                section_key, section_title, text,
                start_offset, end_offset, extractor_version, confidence,
                extraction_method
            ) VALUES (
                %s, %s, %s, '10-K',
                'item_7_mda', 'Management Discussion and Analysis', %s,
                0, %s, 'sec-extractor-test-v1', 0.95,
                'deterministic'
            )
            RETURNING id;
            """,
            (artifact_id, company_id, f"FY{fiscal_year}", mda_text, len(mda_text)),
        )
        section_id = cur.fetchone()[0]

        cur.execute(
            """
            INSERT INTO artifact_section_chunks (
                section_id, chunk_ordinal, text, search_text, heading_path,
                start_offset, end_offset, chunker_version
            ) VALUES (
                %s, 1, %s, %s,
                ARRAY['Item 7', 'Results of Operations', 'Revenue']::text[],
                0, %s, 'chunker-test-v1'
            )
            RETURNING id;
            """,
            (section_id, mda_text, mda_text.lower(), len(mda_text)),
        )
        chunk_id = cur.fetchone()[0]
    return artifact_id, section_id, chunk_id


def _seed_press_release(
    conn: psycopg.Connection,
    *,
    company_id: int,
    ticker: str,
    fiscal_year: int,
    period_end: date,
    run_id: int,
    pr_text: str,
) -> tuple[int, int, int]:
    published_at = datetime(fiscal_year + 1, 2, 26, tzinfo=timezone.utc)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO artifacts (
                ingest_run_id, artifact_type, source, raw_hash, canonical_hash,
                ticker, fiscal_year, fiscal_quarter, fiscal_period_label,
                period_type, period_end,
                published_at, company_id, fiscal_period_key,
                cik, accession_number
            ) VALUES (
                %s, 'press_release', 'sec', %s, %s,
                %s, %s, 4, %s,
                'quarter', %s,
                %s, %s, %s,
                %s, %s
            )
            RETURNING id;
            """,
            (
                run_id, _hash_for(f"pr-{ticker}-{fiscal_year}"),
                _hash_for(f"pr-{ticker}-{fiscal_year}-c"),
                ticker, fiscal_year, f"FY{fiscal_year} Q4", period_end,
                published_at, company_id, f"FY{fiscal_year} Q4",
                str(9999999), f"0000000000-{fiscal_year}-000002",
            ),
        )
        artifact_id = cur.fetchone()[0]

        cur.execute(
            """
            INSERT INTO artifact_text_units (
                artifact_id, company_id, fiscal_period_key,
                unit_ordinal, unit_type, unit_key, unit_title, text,
                start_offset, end_offset, extractor_version, confidence,
                extraction_method
            ) VALUES (
                %s, %s, %s,
                1, 'press_release', 'fy_summary', 'Full-Year Summary', %s,
                0, %s, 'pr-extractor-test-v1', 0.95,
                'deterministic'
            )
            RETURNING id;
            """,
            (artifact_id, company_id, f"FY{fiscal_year}", pr_text, len(pr_text)),
        )
        unit_id = cur.fetchone()[0]

        cur.execute(
            """
            INSERT INTO artifact_text_chunks (
                text_unit_id, chunk_ordinal, text, search_text, heading_path,
                start_offset, end_offset, chunker_version
            ) VALUES (
                %s, 1, %s, %s,
                ARRAY['Full-Year Highlights']::text[],
                0, %s, 'chunker-test-v1'
            )
            RETURNING id;
            """,
            (unit_id, pr_text, pr_text.lower(), len(pr_text)),
        )
        chunk_id = cur.fetchone()[0]
    return artifact_id, unit_id, chunk_id


def _seed_transcript(
    conn: psycopg.Connection,
    *,
    company_id: int,
    ticker: str,
    fiscal_year: int,
    fiscal_quarter: int,
    period_end: date,
    run_id: int,
    turns: list[tuple[str, str]],
) -> tuple[int, list[int]]:
    published_at = datetime(fiscal_year + 1, 2, 27, tzinfo=timezone.utc)
    fiscal_period_key = f"FY{fiscal_year} Q{fiscal_quarter}"
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO artifacts (
                ingest_run_id, artifact_type, source, source_document_id,
                raw_hash, canonical_hash, ticker, fiscal_year, fiscal_quarter,
                fiscal_period_label, period_type, period_end,
                published_at, company_id, fiscal_period_key
            ) VALUES (
                %s, 'transcript', 'fmp', %s,
                %s, %s, %s, %s, %s,
                %s, 'quarter', %s,
                %s, %s, %s
            )
            RETURNING id;
            """,
            (
                run_id,
                f"fmp:earning-call-transcript:{ticker}:FY{fiscal_year}-Q{fiscal_quarter}",
                _hash_for(f"transcript-{ticker}-{fiscal_year}-{fiscal_quarter}"),
                _hash_for(f"transcript-{ticker}-{fiscal_year}-{fiscal_quarter}-c"),
                ticker,
                fiscal_year,
                fiscal_quarter,
                fiscal_period_key,
                period_end,
                published_at,
                company_id,
                fiscal_period_key,
            ),
        )
        artifact_id = cur.fetchone()[0]
        chunk_ids: list[int] = []
        offset = 0
        for ordinal, (speaker, text) in enumerate(turns, start=1):
            full_text = f"{speaker}: {text}"
            cur.execute(
                """
                INSERT INTO artifact_text_units (
                    artifact_id, company_id, fiscal_period_key,
                    unit_ordinal, unit_type, unit_key, unit_title, text,
                    start_offset, end_offset, extractor_version, confidence,
                    extraction_method
                ) VALUES (
                    %s, %s, %s,
                    %s, 'transcript', %s, %s, %s,
                    %s, %s, 'test-transcript-v1', 0.9,
                    'deterministic'
                )
                RETURNING id;
                """,
                (
                    artifact_id,
                    company_id,
                    fiscal_period_key,
                    ordinal,
                    f"turn:{ordinal:03d}",
                    speaker,
                    full_text,
                    offset,
                    offset + len(full_text),
                ),
            )
            unit_id = cur.fetchone()[0]
            cur.execute(
                """
                INSERT INTO artifact_text_chunks (
                    text_unit_id, chunk_ordinal, text, search_text, heading_path,
                    start_offset, end_offset, chunker_version
                ) VALUES (
                    %s, 1, %s, %s, ARRAY[%s]::text[],
                    0, %s, 'chunker-test-v1'
                )
                RETURNING id;
                """,
                (unit_id, full_text, full_text.lower(), speaker, len(full_text)),
            )
            chunk_ids.append(cur.fetchone()[0])
            offset += len(full_text) + 1
    return artifact_id, chunk_ids


def _hash_for(seed: str) -> bytes:
    import hashlib
    return hashlib.sha256(seed.encode()).digest()


# Driver-vocabulary text — must clear the analyst ranker's quality threshold
# (revenue + growth/driven by + commercial/government/data center keywords).
_HAPPY_MDA_TEXT = (
    "Revenue for fiscal year 2024 grew 30% year over year, driven by "
    "Commercial revenue growth and continued strength in Government demand. "
    "U.S. revenue increased on customer expansion. Growth was driven by "
    "data center deployments and commercial customer count expansion. "
    "Segment revenue mix shifted toward higher-growth categories."
)
_HAPPY_PR_TEXT = (
    "Full-year revenue of $2.86B grew 30% year over year, driven by "
    "Commercial growth and Government demand. U.S. revenue increased "
    "primarily due to customer expansion. Growth was driven by data center "
    "deployments and commercial customer count expansion."
)


def _seed_happy_path_company(conn: psycopg.Connection) -> dict:
    company_id = _seed_company(conn, ticker="ARRW")
    run_id, raw_id = _seed_run_and_raw(conn)

    fy_facts: dict[int, dict[str, int]] = {}
    for fy, period_end, multiplier in (
        (2023, date(2023, 12, 31), 1.0),
        (2024, date(2024, 12, 31), 1.30),
    ):
        ids: dict[str, int] = {}
        ids["revenue"] = _seed_annual_fact(
            conn, company_id=company_id, concept="revenue", statement="income_statement",
            value=int(2_200_000_000 * multiplier), fiscal_year=fy, period_end=period_end,
            run_id=run_id, raw_id=raw_id,
        )
        ids["cogs"] = _seed_annual_fact(
            conn, company_id=company_id, concept="cogs", statement="income_statement",
            value=int(440_000_000 * multiplier), fiscal_year=fy, period_end=period_end,
            run_id=run_id, raw_id=raw_id,
        )
        ids["gross_profit"] = _seed_annual_fact(
            conn, company_id=company_id, concept="gross_profit", statement="income_statement",
            value=int(1_760_000_000 * multiplier), fiscal_year=fy, period_end=period_end,
            run_id=run_id, raw_id=raw_id,
        )
        ids["operating_income"] = _seed_annual_fact(
            conn, company_id=company_id, concept="operating_income", statement="income_statement",
            value=int(120_000_000 * multiplier), fiscal_year=fy, period_end=period_end,
            run_id=run_id, raw_id=raw_id,
        )
        ids["cfo"] = _seed_annual_fact(
            conn, company_id=company_id, concept="cfo", statement="cash_flow",
            value=int(700_000_000 * multiplier), fiscal_year=fy, period_end=period_end,
            run_id=run_id, raw_id=raw_id,
        )
        ids["capex"] = _seed_annual_fact(
            conn, company_id=company_id, concept="capital_expenditures", statement="cash_flow",
            value=int(-50_000_000 * multiplier), fiscal_year=fy, period_end=period_end,
            run_id=run_id, raw_id=raw_id,
        )
        # Segment facts — Commercial (faster) and Government (slower).
        ids["seg_commercial"] = _seed_segment_fact(
            conn, company_id=company_id, fiscal_year=fy, period_end=period_end,
            dimension_type="operating_segment", dimension_key="commercial",
            dimension_label="Commercial", value=int(1_100_000_000 * multiplier),
            run_id=run_id, raw_id=raw_id,
        )
        ids["seg_government"] = _seed_segment_fact(
            conn, company_id=company_id, fiscal_year=fy, period_end=period_end,
            dimension_type="operating_segment", dimension_key="government",
            dimension_label="Government", value=int(1_100_000_000 * multiplier),
            run_id=run_id, raw_id=raw_id,
        )
        fy_facts[fy] = ids

    artifact_id, section_id, chunk_id = _seed_10k_with_mda(
        conn, company_id=company_id, ticker="ARRW",
        fiscal_year=2024, period_end=date(2024, 12, 31),
        run_id=run_id, mda_text=_HAPPY_MDA_TEXT,
    )
    pr_artifact_id, unit_id, pr_chunk_id = _seed_press_release(
        conn, company_id=company_id, ticker="ARRW",
        fiscal_year=2024, period_end=date(2024, 12, 31),
        run_id=run_id, pr_text=_HAPPY_PR_TEXT,
    )
    transcript_artifact_id, transcript_chunk_ids = _seed_transcript(
        conn,
        company_id=company_id,
        ticker="ARRW",
        fiscal_year=2024,
        fiscal_quarter=4,
        period_end=date(2024, 12, 31),
        run_id=run_id,
        turns=[
            (
                "CEO",
                "Revenue growth was driven by strong commercial demand, "
                "government customers, and data center deployments.",
            ),
            (
                "CFO",
                "Year over year revenue growth reflected customer expansion.",
            ),
        ],
    )
    return {
        "company_id": company_id,
        "fy_facts": fy_facts,
        "mda_artifact_id": artifact_id,
        "mda_chunk_id": chunk_id,
        "pr_artifact_id": pr_artifact_id,
        "pr_chunk_id": pr_chunk_id,
        "transcript_artifact_id": transcript_artifact_id,
        "transcript_chunk_ids": transcript_chunk_ids,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_revenue_driver_recipe_happy_path() -> None:
    """Full slice: parse -> readiness -> retrieve -> ground -> synthesize -> verify."""
    with get_conn() as conn:
        _reset(conn)
        seeded = _seed_happy_path_company(conn)
        trace = RuntimeTrace.start(source_question="dummy")
        intent = parse_revenue_driver_intent(
            conn, "What drove ARRW revenue growth in FY2024?", trace,
        )
        packet = build_revenue_driver_packet(conn, intent, trace, limit_chunks=2)
        answer = synthesize_revenue_driver_answer(packet, trace)

    # Intent
    assert intent.ticker == "ARRW"
    assert intent.fiscal_year == 2024
    assert intent.fiscal_period_key == "FY2024"
    assert intent.topic == "revenue_growth"
    assert intent.mode == "single_company_period"
    assert intent.asof is None  # day-1 seam: field present, MVP value None

    # Readiness
    assert packet.readiness.status in ("PASS", "SOFT_GAP")
    assert any("PASS v_metrics_fy" in c for c in packet.readiness.checks)
    assert any("PASS annual artifact" in c for c in packet.readiness.checks)

    # Retrieval populated
    assert packet.current_metrics is not None
    assert packet.prior_metrics is not None
    assert len(packet.segment_facts) == 2
    assert any(s.dimension_key == "commercial" for s in packet.segment_facts)
    assert any(s.dimension_key == "government" for s in packet.segment_facts)
    assert all(s.prior_value is not None for s in packet.segment_facts)
    assert len(packet.mda_chunks) >= 1
    assert len(packet.earnings_chunks) >= 1
    assert len(packet.transcript_turns) >= 1

    # Provenance — every cited fact/chunk traces back to a seeded ID.
    assert seeded["mda_chunk_id"] in packet.provenance.chunk_ids
    assert seeded["pr_chunk_id"] in packet.provenance.chunk_ids
    assert seeded["transcript_chunk_ids"][0] in packet.provenance.chunk_ids
    assert seeded["fy_facts"][2024]["revenue"] in packet.provenance.fact_ids
    assert seeded["fy_facts"][2023]["revenue"] in packet.provenance.fact_ids

    # Synthesis
    assert answer.verification_status == "verified"
    assert "ARRW" in answer.summary
    assert "FY2024" in answer.summary
    assert "30.0%" in answer.summary  # YoY growth was constructed at 30%
    assert "$2.86B" in answer.summary  # current revenue
    assert "$2.20B" in answer.summary  # prior revenue
    assert any(f"[F:{seeded['fy_facts'][2024]['revenue']}]" in c for c in answer.citations)
    assert any(f"[S:{seeded['mda_chunk_id']}]" in c for c in answer.citations)
    assert any(f"[T:{seeded['transcript_chunk_ids'][0]}]" in c for c in answer.citations)


def test_revenue_driver_recipe_hard_fails_on_missing_period() -> None:
    """Readiness check must hard-fail when v_metrics_fy has no row for the period."""
    with get_conn() as conn:
        _reset(conn)
        # Seed company only — no facts, no artifacts for the requested FY.
        _seed_company(conn, ticker="EMPT")
        trace = RuntimeTrace.start(source_question="dummy")
        intent = parse_revenue_driver_intent(
            conn, "What drove EMPT revenue growth in FY2024?", trace,
        )
        packet = build_revenue_driver_packet(conn, intent, trace, limit_chunks=2)

    assert packet.readiness.status == "HARD_FAIL"
    assert any("FAIL no v_metrics_fy row" in c for c in packet.readiness.checks)
    assert any("FAIL no annual artifact" in c for c in packet.readiness.checks)
    assert packet.current_metrics is None
    assert packet.prior_metrics is None
    assert packet.segment_facts == []
    assert packet.mda_chunks == []
    assert packet.earnings_chunks == []
    assert packet.transcript_turns == []


def test_intent_parser_rejects_questions_without_fiscal_year() -> None:
    with get_conn() as conn:
        _reset(conn)
        _seed_company(conn, ticker="TEST")
        trace = RuntimeTrace.start(source_question="dummy")
        with pytest.raises(IntentError, match="explicit fiscal year"):
            parse_revenue_driver_intent(conn, "What drove TEST revenue growth?", trace)


def test_intent_parser_rejects_questions_without_revenue_topic() -> None:
    with get_conn() as conn:
        _reset(conn)
        _seed_company(conn, ticker="TEST")
        trace = RuntimeTrace.start(source_question="dummy")
        with pytest.raises(IntentError, match="revenue growth driver"):
            parse_revenue_driver_intent(conn, "What was TEST margin in FY2024?", trace)


def test_intent_parser_rejects_unknown_ticker() -> None:
    with get_conn() as conn:
        _reset(conn)
        # No companies seeded.
        trace = RuntimeTrace.start(source_question="dummy")
        with pytest.raises(IntentError, match="No company found"):
            parse_revenue_driver_intent(
                conn, "What drove WXYZ revenue growth in FY2024?", trace,
            )
