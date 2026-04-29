"""Document and chunk retrieval primitives.

Three layers of access:

- ``list_documents``: which artifacts exist for a company-period.
- ``get_section_chunks``: parsed sections of long-form filings (10-K, 10-Q).
- ``get_text_unit_chunks``: text units of release-style artifacts (press
  releases, transcripts) addressed by ``unit_type``.

These are structural reads, not full-text search. FTS-driven retrieval is a
separate primitive layered on top, added when an analyst question demands it.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import psycopg

from arrow.retrieval._query import run_query
from arrow.retrieval.types import ArtifactPeriod, EvidenceChunk


_DEFAULT_ARTIFACT_TYPES: tuple[str, ...] = (
    "10k",
    "10q",
    "8k",
    "press_release",
    "transcript",
)


def list_documents(
    conn: psycopg.Connection,
    *,
    company_id: int,
    period_type: str,
    fiscal_year: int | None = None,
    fiscal_period_key: str | None = None,
    artifact_types: Sequence[str] | None = None,
) -> list[ArtifactPeriod]:
    """List current artifacts for a company-period.

    For ``period_type='annual'``, supply ``fiscal_year`` to match every
    artifact tied to that fiscal year (annual + the year's quarterlies).
    For ``period_type='quarter'``, supply ``fiscal_period_key`` (e.g.
    ``"FY2025 Q3"``) to match artifacts tied to that exact quarter.
    """
    types_param = list(artifact_types or _DEFAULT_ARTIFACT_TYPES)
    rows = run_query(
        conn,
        sql="""
            SELECT
                a.id AS artifact_id,
                a.artifact_type,
                a.fiscal_period_key,
                a.fiscal_period_label,
                a.period_type,
                a.period_end,
                a.published_at,
                a.source_document_id,
                a.accession_number
            FROM artifacts a
            WHERE a.company_id = %s
              AND (
                    (%s = 'annual' AND a.fiscal_year = %s)
                    OR (%s = 'quarter' AND a.fiscal_period_key = %s)
              )
              AND a.artifact_type = ANY(%s)
              AND a.superseded_at IS NULL
            ORDER BY
                CASE WHEN a.period_type = 'annual' THEN 0 ELSE 1 END,
                a.published_at DESC NULLS LAST,
                a.id DESC;
        """,
        params=(
            company_id,
            period_type,
            fiscal_year,
            period_type,
            fiscal_period_key,
            types_param,
        ),
    )
    return [ArtifactPeriod(**row) for row in rows]


def get_section_chunks(
    conn: psycopg.Connection,
    *,
    company_id: int,
    fiscal_period_key: str,
    section_keys: Sequence[str],
    source_kind: str,
    limit: int | None = None,
) -> list[EvidenceChunk]:
    """Pull section-bound chunks (e.g. MD&A) for one company-period.

    No text search — section-key + period filter only. Caller does any
    ranking/filtering that requires the chunk body.
    """
    if not section_keys:
        return []
    sql = """
        SELECT
            a.id AS artifact_id,
            a.accession_number,
            a.source_document_id,
            a.published_at,
            s.fiscal_period_key,
            s.section_key AS unit_key,
            s.section_title AS unit_title,
            c.id AS chunk_id,
            c.chunk_ordinal,
            c.heading_path,
            c.text
        FROM artifact_sections s
        JOIN artifact_section_chunks c ON c.section_id = s.id
        JOIN artifacts a ON a.id = s.artifact_id
        WHERE s.company_id = %s
          AND s.fiscal_period_key = %s
          AND s.section_key = ANY(%s)
          AND a.superseded_at IS NULL
        ORDER BY
            a.published_at DESC NULLS LAST,
            CASE s.section_key WHEN 'item_7_mda' THEN 0 ELSE 1 END,
            c.chunk_ordinal
    """
    params: list[Any] = [company_id, fiscal_period_key, list(section_keys)]
    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)
    sql += ";"
    rows = run_query(conn, sql=sql, params=tuple(params))
    return [EvidenceChunk(source_kind=source_kind, **row) for row in rows]


def get_text_unit_chunks(
    conn: psycopg.Connection,
    *,
    company_id: int,
    fiscal_period_key: str,
    unit_type: str,
    source_kind: str,
    annual_q4_fallback: tuple[int, Any] | None = None,
    limit: int | None = None,
) -> list[EvidenceChunk]:
    """Pull text-unit chunks (press releases, transcripts) for a company-period.

    ``annual_q4_fallback=(fiscal_year, fy_end)`` extends the match to also
    accept Q4 artifacts at that fiscal-year-end date. This is how the annual
    recipe finds the FY-end Q4 earnings release when the unit isn't tagged
    with the annual ``fiscal_period_key``.
    """
    use_fallback = annual_q4_fallback is not None
    fallback_year = annual_q4_fallback[0] if use_fallback else None
    fallback_period_end = annual_q4_fallback[1] if use_fallback else None
    sql = """
        SELECT
            a.id AS artifact_id,
            a.accession_number,
            a.source_document_id,
            a.published_at,
            COALESCE(u.fiscal_period_key, a.fiscal_period_key) AS fiscal_period_key,
            u.unit_key,
            u.unit_title,
            c.id AS chunk_id,
            c.chunk_ordinal,
            c.heading_path,
            c.text
        FROM artifact_text_units u
        JOIN artifact_text_chunks c ON c.text_unit_id = u.id
        JOIN artifacts a ON a.id = u.artifact_id
        WHERE (u.company_id = %s OR a.company_id = %s)
          AND u.unit_type = %s
          AND (
                COALESCE(u.fiscal_period_key, a.fiscal_period_key) = %s
                OR (
                    %s
                    AND a.period_type = 'quarter'
                    AND a.fiscal_year = %s
                    AND a.fiscal_quarter = 4
                    AND a.period_end = %s
                )
          )
          AND a.superseded_at IS NULL
        ORDER BY
            CASE
                WHEN COALESCE(u.fiscal_period_key, a.fiscal_period_key) = %s THEN 0
                ELSE 1
            END,
            a.published_at DESC NULLS LAST,
            u.unit_ordinal,
            c.chunk_ordinal
    """
    params: list[Any] = [
        company_id,
        company_id,
        unit_type,
        fiscal_period_key,
        use_fallback,
        fallback_year,
        fallback_period_end,
        fiscal_period_key,
    ]
    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)
    sql += ";"
    rows = run_query(conn, sql=sql, params=tuple(params))
    return [EvidenceChunk(source_kind=source_kind, **row) for row in rows]
