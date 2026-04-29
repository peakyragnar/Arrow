"""Chat-style analyst surface mounted into the dashboard FastAPI app.

Routes:
  GET  /ask                    HTML chat page
  POST /ask/stream             Server-Sent Events: agent loop progress + answer
  GET  /evidence/{kind}/{id}   Lookup raw evidence row for a citation popup

The /ask page is a single-shot UI for now: ask one question, see the agent
loop progress, see the answer with clickable citations. Multi-turn ships
when single-turn is solid.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from fastapi.templating import Jinja2Templates
from psycopg.rows import dict_row

from arrow.analysis.agent import ask_stream
from arrow.db.connection import get_conn

BASE_DIR = Path(__file__).resolve().parents[3]
TEMPLATES = Jinja2Templates(directory=BASE_DIR / "templates")

router = APIRouter()


@router.get("/ask")
def ask_page(request: Request):
    return TEMPLATES.TemplateResponse(
        request=request,
        name="ask.html.j2",
        context={},
    )


@router.post("/ask/stream")
async def ask_stream_endpoint(request: Request):
    body = await request.json()
    question = (body.get("question") or "").strip()
    if not question:
        raise HTTPException(status_code=400, detail="question is required")

    async def event_source():
        try:
            async for event in ask_stream(question):
                yield f"data: {json.dumps(event, default=str)}\n\n"
        except Exception as exc:
            yield f"data: {json.dumps({'event': 'error', 'message': f'{type(exc).__name__}: {exc}'})}\n\n"

    return StreamingResponse(
        event_source(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# --------------------------------------------------------------------------- #
# Citation popup data — fetches the raw evidence row for [F:..]/[T:..]/[S:..].
# --------------------------------------------------------------------------- #


def _fetch_financial_fact(conn, fact_id: int) -> dict[str, Any] | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
                f.id AS fact_id,
                f.statement,
                f.concept,
                f.value,
                f.unit,
                f.fiscal_period_label,
                f.period_end,
                f.dimension_type,
                f.dimension_key,
                f.dimension_label,
                c.ticker,
                c.name AS company_name
            FROM financial_facts f
            LEFT JOIN companies c ON c.id = f.company_id
            WHERE f.id = %s;
            """,
            (fact_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    row["value"] = str(row["value"]) if row["value"] is not None else None
    row["period_end"] = str(row["period_end"]) if row["period_end"] else None
    return row


def _fetch_transcript_chunk(conn, chunk_id: int) -> dict[str, Any] | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
                c.id AS chunk_id,
                c.chunk_ordinal,
                c.text,
                c.heading_path,
                u.unit_title AS speaker,
                u.unit_key AS turn_key,
                u.fiscal_period_key AS unit_period_key,
                a.id AS artifact_id,
                a.ticker,
                a.fiscal_period_label,
                a.fiscal_period_key,
                a.period_end,
                a.published_at,
                a.source_document_id
            FROM artifact_text_chunks c
            JOIN artifact_text_units u ON u.id = c.text_unit_id
            JOIN artifacts a ON a.id = u.artifact_id
            WHERE c.id = %s
              AND a.artifact_type = 'transcript';
            """,
            (chunk_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    row["heading_path"] = list(row["heading_path"] or [])
    row["period_end"] = str(row["period_end"]) if row["period_end"] else None
    row["published_at"] = str(row["published_at"]) if row["published_at"] else None
    return row


def _fetch_section_chunk(conn, chunk_id: int) -> dict[str, Any] | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
                c.id AS chunk_id,
                c.chunk_ordinal,
                c.text,
                c.heading_path,
                s.section_key,
                s.section_title,
                s.fiscal_period_key,
                a.id AS artifact_id,
                a.artifact_type,
                a.ticker,
                a.fiscal_period_label,
                a.period_end,
                a.published_at,
                a.accession_number
            FROM artifact_section_chunks c
            JOIN artifact_sections s ON s.id = c.section_id
            JOIN artifacts a ON a.id = s.artifact_id
            WHERE c.id = %s;
            """,
            (chunk_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    row["heading_path"] = list(row["heading_path"] or [])
    row["period_end"] = str(row["period_end"]) if row["period_end"] else None
    row["published_at"] = str(row["published_at"]) if row["published_at"] else None
    return row


@router.get("/evidence/{kind}/{evidence_id}")
def get_evidence(kind: str, evidence_id: str):
    """Return the raw row for a citation popup.

    ``kind`` is one of: F (financial_fact), T (transcript chunk),
    S (filing section chunk). M (metric view) and A (artifact) are not
    implemented yet — the citation just shows the ID for now.
    """
    kind = kind.upper()
    if kind not in {"F", "T", "S"}:
        raise HTTPException(
            status_code=400,
            detail=f"unsupported evidence kind '{kind}'. Supported: F, T, S.",
        )
    try:
        eid = int(evidence_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="evidence id must be an integer")
    with get_conn() as conn:
        if kind == "F":
            row = _fetch_financial_fact(conn, eid)
        elif kind == "T":
            row = _fetch_transcript_chunk(conn, eid)
        else:
            row = _fetch_section_chunk(conn, eid)
    if row is None:
        raise HTTPException(status_code=404, detail=f"no {kind} evidence row for id {eid}")
    return {"kind": kind, "id": eid, "row": row}
