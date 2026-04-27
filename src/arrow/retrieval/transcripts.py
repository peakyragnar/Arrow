"""Transcript retrieval primitives for analyst recipes and CLI tools."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row


@dataclass(frozen=True)
class TranscriptDocument:
    artifact_id: int
    company_id: int
    ticker: str
    fiscal_period_key: str
    fiscal_period_label: str
    fiscal_year: int
    fiscal_quarter: int
    period_end: Any
    published_at: Any
    title: str | None
    source_document_id: str | None
    turn_count: int
    chunk_count: int


@dataclass(frozen=True)
class TranscriptTurn:
    artifact_id: int
    text_unit_id: int
    chunk_id: int | None
    ticker: str
    fiscal_period_key: str
    fiscal_period_label: str
    fiscal_year: int
    fiscal_quarter: int
    period_end: Any
    published_at: Any
    source_document_id: str | None
    unit_ordinal: int
    unit_key: str
    speaker: str
    chunk_ordinal: int | None
    heading_path: list[str]
    text: str
    rank: float | None = None


@dataclass(frozen=True)
class TranscriptContext:
    document: TranscriptDocument
    turns: list[TranscriptTurn]


@dataclass(frozen=True)
class TranscriptMentionSummary:
    artifact_id: int
    ticker: str
    fiscal_period_key: str
    fiscal_period_label: str
    fiscal_year: int
    fiscal_quarter: int
    period_end: Any
    published_at: Any
    term_counts: dict[str, int]
    total_mentions: int


def get_latest_transcripts(
    conn: psycopg.Connection,
    ticker: str,
    *,
    n: int = 4,
    asof: datetime | None = None,
) -> list[TranscriptDocument]:
    """Return the latest current transcript artifacts for a ticker."""
    if n <= 0:
        return []
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
                a.id AS artifact_id,
                a.company_id,
                a.ticker,
                a.fiscal_period_key,
                a.fiscal_period_label,
                a.fiscal_year,
                a.fiscal_quarter,
                a.period_end,
                a.published_at,
                a.title,
                a.source_document_id,
                COUNT(DISTINCT u.id)::int AS turn_count,
                COUNT(c.id)::int AS chunk_count
            FROM artifacts a
            LEFT JOIN artifact_text_units u
              ON u.artifact_id = a.id
             AND u.unit_type = 'transcript'
            LEFT JOIN artifact_text_chunks c
              ON c.text_unit_id = u.id
            WHERE upper(a.ticker) = %s
              AND a.artifact_type = 'transcript'
              AND a.superseded_at IS NULL
              AND (%s::timestamptz IS NULL OR a.published_at <= %s)
            GROUP BY a.id
            ORDER BY a.period_end DESC NULLS LAST,
                     a.published_at DESC NULLS LAST,
                     a.id DESC
            LIMIT %s;
            """,
            (_ticker(ticker), asof, asof, n),
        )
        return [TranscriptDocument(**row) for row in cur.fetchall()]


def get_transcript_context(
    conn: psycopg.Connection,
    ticker: str,
    fiscal_period_key: str,
    *,
    max_turns: int | None = None,
    asof: datetime | None = None,
) -> TranscriptContext | None:
    """Read ordered speaker turns for one transcript period."""
    document = _get_transcript_document(
        conn,
        ticker=ticker,
        fiscal_period_key=fiscal_period_key,
        asof=asof,
    )
    if document is None:
        return None

    limit_clause = "" if max_turns is None else "LIMIT %s"
    params: list[Any] = [document.artifact_id]
    if max_turns is not None:
        if max_turns <= 0:
            return TranscriptContext(document=document, turns=[])
        params.append(max_turns)

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            SELECT
                a.id AS artifact_id,
                u.id AS text_unit_id,
                NULL::bigint AS chunk_id,
                a.ticker,
                COALESCE(u.fiscal_period_key, a.fiscal_period_key) AS fiscal_period_key,
                a.fiscal_period_label,
                a.fiscal_year,
                a.fiscal_quarter,
                a.period_end,
                a.published_at,
                a.source_document_id,
                u.unit_ordinal,
                u.unit_key,
                u.unit_title AS speaker,
                NULL::integer AS chunk_ordinal,
                ARRAY[u.unit_title]::text[] AS heading_path,
                u.text,
                NULL::double precision AS rank
            FROM artifact_text_units u
            JOIN artifacts a ON a.id = u.artifact_id
            WHERE a.id = %s
              AND u.unit_type = 'transcript'
            ORDER BY u.unit_ordinal
            {limit_clause};
            """,
            tuple(params),
        )
        return TranscriptContext(
            document=document,
            turns=[_turn(row) for row in cur.fetchall()],
        )


def search_transcript_turns(
    conn: psycopg.Connection,
    ticker: str,
    query: str,
    *,
    fiscal_period_key: str | None = None,
    limit: int = 10,
    asof: datetime | None = None,
) -> list[TranscriptTurn]:
    """Search transcript chunks by Postgres FTS and return cited speaker turns."""
    query = query.strip()
    if not query:
        raise ValueError("query must not be empty")
    if limit <= 0:
        return []
    patterns = [f"%{term.lower()}%" for term in _query_terms(query)]

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            WITH q AS (
                SELECT websearch_to_tsquery('english', %s) AS tsq
            )
            SELECT
                a.id AS artifact_id,
                u.id AS text_unit_id,
                c.id AS chunk_id,
                a.ticker,
                COALESCE(u.fiscal_period_key, a.fiscal_period_key) AS fiscal_period_key,
                a.fiscal_period_label,
                a.fiscal_year,
                a.fiscal_quarter,
                a.period_end,
                a.published_at,
                a.source_document_id,
                u.unit_ordinal,
                u.unit_key,
                u.unit_title AS speaker,
                c.chunk_ordinal,
                c.heading_path,
                c.text,
                ts_rank_cd(c.tsv, q.tsq)::double precision AS rank
            FROM q
            JOIN artifact_text_chunks c ON c.tsv @@ q.tsq
            JOIN artifact_text_units u ON u.id = c.text_unit_id
            JOIN artifacts a ON a.id = u.artifact_id
            WHERE upper(a.ticker) = %s
              AND a.artifact_type = 'transcript'
              AND u.unit_type = 'transcript'
              AND a.superseded_at IS NULL
              AND (%s::text IS NULL OR COALESCE(u.fiscal_period_key, a.fiscal_period_key) = %s)
              AND (%s::timestamptz IS NULL OR a.published_at <= %s)
              AND (
                    cardinality(%s::text[]) = 0
                    OR lower(c.search_text) LIKE ANY(%s::text[])
              )
            ORDER BY rank DESC,
                     a.period_end DESC NULLS LAST,
                     u.unit_ordinal,
                     c.chunk_ordinal
            LIMIT %s;
            """,
            (
                query,
                _ticker(ticker),
                fiscal_period_key,
                fiscal_period_key,
                asof,
                asof,
                patterns,
                patterns,
                limit,
            ),
        )
        return [_turn(row) for row in cur.fetchall()]


def compare_transcript_mentions(
    conn: psycopg.Connection,
    ticker: str,
    terms: list[str],
    *,
    periods: int = 8,
    asof: datetime | None = None,
) -> list[TranscriptMentionSummary]:
    """Count plain-text term mentions across the latest transcript periods."""
    cleaned_terms = [term.strip() for term in terms if term.strip()]
    if not cleaned_terms:
        raise ValueError("at least one term is required")
    docs = get_latest_transcripts(conn, ticker, n=periods, asof=asof)
    if not docs:
        return []

    artifact_ids = [doc.artifact_id for doc in docs]
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT u.artifact_id, string_agg(u.text, E'\n' ORDER BY u.unit_ordinal) AS text
            FROM artifact_text_units u
            WHERE u.artifact_id = ANY(%s)
              AND u.unit_type = 'transcript'
            GROUP BY u.artifact_id;
            """,
            (artifact_ids,),
        )
        text_by_artifact = {row["artifact_id"]: row["text"] or "" for row in cur.fetchall()}

    out: list[TranscriptMentionSummary] = []
    for doc in docs:
        text = text_by_artifact.get(doc.artifact_id, "")
        term_counts = {
            term: _count_term(text, term)
            for term in cleaned_terms
        }
        out.append(
            TranscriptMentionSummary(
                artifact_id=doc.artifact_id,
                ticker=doc.ticker,
                fiscal_period_key=doc.fiscal_period_key,
                fiscal_period_label=doc.fiscal_period_label,
                fiscal_year=doc.fiscal_year,
                fiscal_quarter=doc.fiscal_quarter,
                period_end=doc.period_end,
                published_at=doc.published_at,
                term_counts=term_counts,
                total_mentions=sum(term_counts.values()),
            )
        )
    return out


def _get_transcript_document(
    conn: psycopg.Connection,
    *,
    ticker: str,
    fiscal_period_key: str,
    asof: datetime | None,
) -> TranscriptDocument | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
                a.id AS artifact_id,
                a.company_id,
                a.ticker,
                a.fiscal_period_key,
                a.fiscal_period_label,
                a.fiscal_year,
                a.fiscal_quarter,
                a.period_end,
                a.published_at,
                a.title,
                a.source_document_id,
                COUNT(DISTINCT u.id)::int AS turn_count,
                COUNT(c.id)::int AS chunk_count
            FROM artifacts a
            LEFT JOIN artifact_text_units u
              ON u.artifact_id = a.id
             AND u.unit_type = 'transcript'
            LEFT JOIN artifact_text_chunks c
              ON c.text_unit_id = u.id
            WHERE upper(a.ticker) = %s
              AND a.artifact_type = 'transcript'
              AND a.fiscal_period_key = %s
              AND a.superseded_at IS NULL
              AND (%s::timestamptz IS NULL OR a.published_at <= %s)
            GROUP BY a.id
            ORDER BY a.published_at DESC NULLS LAST, a.id DESC
            LIMIT 1;
            """,
            (_ticker(ticker), fiscal_period_key, asof, asof),
        )
        row = cur.fetchone()
    return None if row is None else TranscriptDocument(**row)


def _turn(row: dict[str, Any]) -> TranscriptTurn:
    return TranscriptTurn(
        artifact_id=row["artifact_id"],
        text_unit_id=row["text_unit_id"],
        chunk_id=row["chunk_id"],
        ticker=row["ticker"],
        fiscal_period_key=row["fiscal_period_key"],
        fiscal_period_label=row["fiscal_period_label"],
        fiscal_year=row["fiscal_year"],
        fiscal_quarter=row["fiscal_quarter"],
        period_end=row["period_end"],
        published_at=row["published_at"],
        source_document_id=row["source_document_id"],
        unit_ordinal=row["unit_ordinal"],
        unit_key=row["unit_key"],
        speaker=row["speaker"],
        chunk_ordinal=row["chunk_ordinal"],
        heading_path=list(row["heading_path"] or []),
        text=row["text"],
        rank=row["rank"],
    )


def _ticker(ticker: str) -> str:
    return ticker.strip().upper()


def _count_term(text: str, term: str) -> int:
    pattern = re.compile(rf"(?<!\w){re.escape(term)}(?!\w)", re.I)
    return len(pattern.findall(text))


def _query_terms(query: str) -> list[str]:
    quoted = [
        term
        for term in re.findall(r'"([^"]+)"', query)
        if len(term) >= 3 or " " in term
    ]
    without_quoted = re.sub(r'"[^"]+"', " ", query)
    words = [
        word
        for word in re.findall(r"[A-Za-z][A-Za-z0-9.-]*", without_quoted)
        if word.upper() not in {"AND", "OR", "NOT"} and len(word) >= 3
    ]
    return [*quoted, *words]
