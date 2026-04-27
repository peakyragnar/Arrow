"""FMP earnings-call transcript ingest orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timezone
from typing import Any

import psycopg

from arrow.ingest.common.artifacts import write_artifact
from arrow.ingest.common.runs import close_failed, close_succeeded, open_run
from arrow.ingest.fmp.client import FMPClient
from arrow.ingest.fmp.transcript_parse import (
    canonicalize_transcript_content,
    parse_speaker_turns,
)
from arrow.ingest.fmp.transcripts import (
    Transcript,
    TranscriptDate,
    TranscriptFetch,
    fetch_earning_call_transcript,
    fetch_transcript_dates,
)
from arrow.ingest.sec.qualitative import (
    TEXT_CHUNKER_VERSION,
    TextUnit,
    build_text_unit_chunks,
)
from arrow.normalize.periods.derive import derive_calendar_period

TRANSCRIPT_UNIT_EXTRACTOR_VERSION = "fmp_transcript_units_v1"


@dataclass(frozen=True)
class CompanyRow:
    id: int
    cik: int
    ticker: str
    fiscal_year_end_md: str


@dataclass(frozen=True)
class NormalizeResult:
    artifact_id: int
    created: bool
    superseded_existing: bool
    text_units_inserted: int
    text_chunks_inserted: int


class CompanyNotSeeded(RuntimeError):
    pass


class MissingFiscalAnchor(RuntimeError):
    pass


class AmbiguousFiscalAnchor(RuntimeError):
    pass


def ingest_transcripts(
    conn: psycopg.Connection,
    tickers: list[str],
    *,
    refresh: bool = False,
    actor: str = "operator",
    limit_per_ticker: int | None = None,
    client: FMPClient | None = None,
) -> dict[str, Any]:
    """Fetch and normalize FMP earnings-call transcripts for tickers."""
    normalized_tickers = [ticker.upper() for ticker in tickers]
    run_id = open_run(
        conn,
        run_kind="manual",
        vendor="fmp",
        ticker_scope=normalized_tickers,
    )
    client = client or FMPClient()

    counts: dict[str, Any] = {
        "actor": actor,
        "refresh": refresh,
        "limit_per_ticker": limit_per_ticker,
        "raw_responses": 0,
        "transcript_dates_fetched": 0,
        "transcript_dates_seen": 0,
        "transcripts_requested": 0,
        "transcripts_missing": 0,
        "transcripts_fetched": 0,
        "artifacts_inserted": 0,
        "artifacts_existing": 0,
        "artifacts_superseded": 0,
        "text_units_inserted": 0,
        "text_chunks_inserted": 0,
    }

    current_ticker: str | None = None
    try:
        for ticker in normalized_tickers:
            current_ticker = ticker
            company = _get_company(conn, ticker)
            with conn.transaction():
                dates_fetch = fetch_transcript_dates(
                    conn,
                    ticker=company.ticker,
                    ingest_run_id=run_id,
                    client=client,
                )
                counts["raw_responses"] += 1
                counts["transcript_dates_fetched"] += 1
                counts["transcript_dates_seen"] += len(dates_fetch.dates)

                candidates = _select_candidates(
                    conn,
                    company=company,
                    dates=dates_fetch.dates,
                    refresh=refresh,
                    limit=limit_per_ticker,
                )
                for candidate in candidates:
                    fetched = fetch_earning_call_transcript(
                        conn,
                        ticker=company.ticker,
                        fiscal_year=candidate.fiscal_year,
                        fiscal_quarter=candidate.fiscal_quarter,
                        ingest_run_id=run_id,
                        client=client,
                    )
                    counts["raw_responses"] += 1
                    counts["transcripts_requested"] += 1
                    if fetched.transcript is None:
                        counts["transcripts_missing"] += 1
                        continue

                    counts["transcripts_fetched"] += 1
                    result = _normalize_one(
                        conn,
                        company=company,
                        fetched=fetched,
                        ingest_run_id=run_id,
                    )
                    if result.created:
                        counts["artifacts_inserted"] += 1
                    else:
                        counts["artifacts_existing"] += 1
                    if result.superseded_existing:
                        counts["artifacts_superseded"] += 1
                    counts["text_units_inserted"] += result.text_units_inserted
                    counts["text_chunks_inserted"] += result.text_chunks_inserted

    except Exception as e:
        close_failed(
            conn,
            run_id,
            error_message=str(e),
            error_details={"kind": type(e).__name__, "ticker": current_ticker},
        )
        raise

    close_succeeded(conn, run_id, counts=counts)
    counts["ingest_run_id"] = run_id
    return counts


def _get_company(conn: psycopg.Connection, ticker: str) -> CompanyRow:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, cik, ticker, fiscal_year_end_md FROM companies WHERE ticker = %s;",
            (ticker.upper(),),
        )
        row = cur.fetchone()
    if row is None:
        raise CompanyNotSeeded(
            f"{ticker} not in companies — run seed_companies.py {ticker} first"
        )
    return CompanyRow(id=row[0], cik=row[1], ticker=row[2], fiscal_year_end_md=row[3])


def _source_document_id(ticker: str, fiscal_year: int, fiscal_quarter: int) -> str:
    return f"fmp:earning-call-transcript:{ticker.upper()}:FY{fiscal_year}-Q{fiscal_quarter}"


def _select_candidates(
    conn: psycopg.Connection,
    *,
    company: CompanyRow,
    dates: list[TranscriptDate],
    refresh: bool,
    limit: int | None,
) -> list[TranscriptDate]:
    out: list[TranscriptDate] = []
    for item in dates:
        if not refresh and _current_artifact_exists(
            conn,
            source_document_id=_source_document_id(
                company.ticker,
                item.fiscal_year,
                item.fiscal_quarter,
            ),
        ):
            continue
        out.append(item)
        if limit is not None and len(out) >= limit:
            break
    return out


def _current_artifact_exists(conn: psycopg.Connection, *, source_document_id: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM artifacts
            WHERE source = 'fmp'
              AND source_document_id = %s
              AND superseded_at IS NULL
            LIMIT 1;
            """,
            (source_document_id,),
        )
        return cur.fetchone() is not None


def _normalize_one(
    conn: psycopg.Connection,
    *,
    company: CompanyRow,
    fetched: TranscriptFetch,
    ingest_run_id: int,
) -> NormalizeResult:
    transcript = fetched.transcript
    if transcript is None:
        raise ValueError("_normalize_one requires a non-empty transcript fetch")

    period_end = _resolve_period_end(conn, company=company, transcript=transcript)
    calendar = derive_calendar_period(period_end)
    fiscal_period_key = _fiscal_period_label(
        transcript.fiscal_year,
        transcript.fiscal_quarter,
    )
    published_at = datetime.combine(transcript.call_date, time.min, tzinfo=timezone.utc)

    artifact_id, created = write_artifact(
        conn,
        ingest_run_id=ingest_run_id,
        artifact_type="transcript",
        source="fmp",
        source_document_id=_source_document_id(
            transcript.ticker,
            transcript.fiscal_year,
            transcript.fiscal_quarter,
        ),
        body=fetched.raw_body,
        company_id=company.id,
        ticker=company.ticker,
        fiscal_period_key=fiscal_period_key,
        fiscal_year=transcript.fiscal_year,
        fiscal_quarter=transcript.fiscal_quarter,
        fiscal_period_label=fiscal_period_key,
        period_end=period_end,
        period_type="quarter",
        calendar_year=calendar.calendar_year,
        calendar_quarter=calendar.calendar_quarter,
        calendar_period_label=calendar.calendar_period_label,
        title=f"{company.ticker} earnings call {fiscal_period_key}",
        content_type="application/json",
        language="en",
        published_at=published_at,
        artifact_metadata={},
    )

    superseded_existing = False
    if created:
        with conn.cursor() as cur:
            cur.execute("SELECT supersedes FROM artifacts WHERE id = %s;", (artifact_id,))
            superseded_existing = cur.fetchone()[0] is not None
        units = _transcript_text_units(
            transcript,
            ticker=company.ticker,
            fiscal_period_key=fiscal_period_key,
        )
        text_units_inserted, text_chunks_inserted = _insert_text_units_and_chunks(
            conn,
            artifact_id=artifact_id,
            company_id=company.id,
            fiscal_period_key=fiscal_period_key,
            units=units,
        )
    else:
        text_units_inserted = 0
        text_chunks_inserted = 0

    return NormalizeResult(
        artifact_id=artifact_id,
        created=created,
        superseded_existing=superseded_existing,
        text_units_inserted=text_units_inserted,
        text_chunks_inserted=text_chunks_inserted,
    )


def _resolve_period_end(
    conn: psycopg.Connection,
    *,
    company: CompanyRow,
    transcript: Transcript,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT period_end
            FROM financial_facts
            WHERE company_id = %s
              AND fiscal_year = %s
              AND fiscal_quarter = %s
              AND period_type = 'quarter'
              AND statement IN ('income_statement', 'balance_sheet', 'cash_flow')
              AND superseded_at IS NULL
            ORDER BY period_end;
            """,
            (company.id, transcript.fiscal_year, transcript.fiscal_quarter),
        )
        rows = cur.fetchall()

    label = _fiscal_period_label(transcript.fiscal_year, transcript.fiscal_quarter)
    if not rows:
        raise MissingFiscalAnchor(
            f"Run `uv run scripts/backfill_fmp.py {company.ticker}` before "
            f"ingesting transcripts for {company.ticker} {label}."
        )
    if len(rows) > 1:
        raise AmbiguousFiscalAnchor(
            f"{company.ticker} {label} has multiple current financial_facts "
            f"period_end values: {[r[0].isoformat() for r in rows]}"
        )
    return rows[0][0]


def _fiscal_period_label(fiscal_year: int, fiscal_quarter: int) -> str:
    return f"FY{fiscal_year} Q{fiscal_quarter}"


def _transcript_text_units(
    transcript: Transcript,
    *,
    ticker: str,
    fiscal_period_key: str,
) -> list[TextUnit]:
    canonical = canonicalize_transcript_content(transcript.content)
    turns = parse_speaker_turns(canonical)
    if not turns:
        return [
            TextUnit(
                unit_ordinal=1,
                unit_type="transcript",
                unit_key="unparsed",
                unit_title=f"{ticker} transcript (unparsed)",
                text=canonical,
                start_offset=0,
                end_offset=len(canonical),
                confidence=0.0,
                extraction_method="unparsed_fallback",
            )
        ]

    return [
        TextUnit(
            unit_ordinal=turn.ordinal,
            unit_type="transcript",
            unit_key=f"turn:{turn.ordinal:03d}",
            unit_title=turn.speaker,
            text=turn.text,
            start_offset=turn.start_offset,
            end_offset=turn.end_offset,
            confidence=0.9,
            extraction_method="deterministic",
        )
        for turn in turns
    ]


def _insert_text_units_and_chunks(
    conn: psycopg.Connection,
    *,
    artifact_id: int,
    company_id: int,
    fiscal_period_key: str,
    units: list[TextUnit],
) -> tuple[int, int]:
    text_units_inserted = 0
    text_chunks_inserted = 0
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM artifact_text_chunks
            WHERE text_unit_id IN (
                SELECT id FROM artifact_text_units WHERE artifact_id = %s
            );
            """,
            (artifact_id,),
        )
        cur.execute("DELETE FROM artifact_text_units WHERE artifact_id = %s;", (artifact_id,))

        for unit in units:
            cur.execute(
                """
                INSERT INTO artifact_text_units (
                    artifact_id, company_id, fiscal_period_key,
                    unit_ordinal, unit_type, unit_key, unit_title,
                    text, start_offset, end_offset,
                    extractor_version, confidence, extraction_method
                ) VALUES (
                    %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s
                )
                RETURNING id;
                """,
                (
                    artifact_id,
                    company_id,
                    fiscal_period_key,
                    unit.unit_ordinal,
                    unit.unit_type,
                    unit.unit_key,
                    unit.unit_title,
                    unit.text,
                    unit.start_offset,
                    unit.end_offset,
                    TRANSCRIPT_UNIT_EXTRACTOR_VERSION,
                    unit.confidence,
                    unit.extraction_method,
                ),
            )
            unit_id = cur.fetchone()[0]
            text_units_inserted += 1
            for chunk in build_text_unit_chunks(unit):
                cur.execute(
                    """
                    INSERT INTO artifact_text_chunks (
                        text_unit_id, chunk_ordinal, text, search_text,
                        heading_path, start_offset, end_offset, chunker_version
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        unit_id,
                        chunk.chunk_ordinal,
                        chunk.text,
                        chunk.search_text,
                        chunk.heading_path,
                        chunk.start_offset,
                        chunk.end_offset,
                        TEXT_CHUNKER_VERSION,
                    ),
                )
                text_chunks_inserted += 1
    return text_units_inserted, text_chunks_inserted
