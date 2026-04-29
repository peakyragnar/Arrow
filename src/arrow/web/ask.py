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
import logging
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from fastapi.templating import Jinja2Templates
from psycopg.rows import dict_row

from arrow.analysis.agent import ask_stream, load_thread
from arrow.db.connection import get_conn

BASE_DIR = Path(__file__).resolve().parents[3]
TEMPLATES = Jinja2Templates(directory=BASE_DIR / "templates")

_LOG = logging.getLogger(__name__)

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
    thread_id = (body.get("thread_id") or "").strip() or None
    if not question:
        raise HTTPException(status_code=400, detail="question is required")

    async def event_source():
        try:
            async for event in ask_stream(question, thread_id=thread_id):
                yield f"data: {json.dumps(event, default=str)}\n\n"
        except Exception:
            # Log the full exception server-side; surface only the type to
            # the browser. Exception messages from psycopg can include the
            # connection string (with credentials) — never echo them out.
            _LOG.exception("ask_stream failed")
            yield f"data: {json.dumps({'event': 'error', 'message': 'internal error — check server logs'})}\n\n"

    return StreamingResponse(
        event_source(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/threads/{thread_id}")
def get_thread(thread_id: str):
    """Return the prior Q+A turns for a thread, in chronological order.

    Used by the chat UI to hydrate prior turns on page load when the
    browser remembers a thread_id from a previous session.
    """
    if not thread_id or "/" in thread_id or ".." in thread_id:
        raise HTTPException(status_code=400, detail="invalid thread_id")
    turns = load_thread(thread_id)
    return {
        "thread_id": thread_id,
        "turn_count": len(turns),
        "turns": [
            {
                "question": t.question,
                "answer": t.answer,
                "started_at": t.started_at,
            }
            for t in turns
        ],
    }


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


_METRIC_VIEW_KEY_FIELDS = {
    "v_metrics_fy": ("fiscal_period_label",),
    "v_metrics_q": ("fiscal_period_label",),
    "v_metrics_roic": ("period_end",),
    "v_metrics_cy": ("calendar_period_label",),
    "v_metrics_ttm": ("period_end",),
    "v_metrics_ttm_yoy": ("period_end",),
}

# Columns to surface in the popup, per view. Keep this tight — full row
# would render dozens of fields.
_METRIC_VIEW_DISPLAY_COLS: dict[str, tuple[str, ...]] = {
    "v_metrics_fy": (
        "ticker", "fiscal_year", "fiscal_period_label", "fy_end",
        "revenue_fy", "gross_margin_fy", "operating_margin_fy", "net_margin_fy",
        "cfo_fy", "capital_expenditures_fy", "rd_fy", "sbc_fy",
        "total_employees_fy",
    ),
    "v_metrics_q": (
        "ticker", "fiscal_period_label", "period_end",
        "revenue", "gross_margin", "operating_margin", "net_margin",
        "cfo", "capital_expenditures",
    ),
    "v_metrics_roic": (
        "ticker", "period_end", "roic", "roiic",
        "adjusted_nopat_ttm", "adjusted_ic_q",
    ),
}


def _row_to_safe(row: dict[str, Any]) -> dict[str, Any]:
    safe: dict[str, Any] = {}
    for k, v in row.items():
        if v is None:
            safe[k] = None
        elif hasattr(v, "isoformat"):
            safe[k] = v.isoformat()
        else:
            safe[k] = str(v) if not isinstance(v, (int, str, float, bool)) else v
    return safe


def _fetch_metric_view_row(conn, body: str) -> dict[str, Any] | None:
    """Look up metric-view evidence for a citation body.

    Body shapes (decoded — colons preserved):
      v_metrics_fy:<company_id>:FY2024                  single annual row
      v_metrics_q:<company_id>:FY2024 Q3                single quarterly row
      v_metrics_roic:<company_id>:<period_end_iso>      single ROIC row
      v_metrics_roic:<company_id>:<start>_to_<end>      window of rows
                                                        (from screen_companies)
    """
    parts = body.split(":", 2)
    if len(parts) != 3:
        return None
    view, company_part, period_part = parts
    if view not in _METRIC_VIEW_KEY_FIELDS:
        return None
    try:
        company_id = int(company_part)
    except ValueError:
        return None
    period_field = _METRIC_VIEW_KEY_FIELDS[view][0]
    cols = _METRIC_VIEW_DISPLAY_COLS.get(view)
    select_clause = ", ".join(cols) if cols else "*"

    if "_to_" in period_part:
        start, end = period_part.split("_to_", 1)
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT {select_clause}
                FROM {view}
                WHERE company_id = %s
                  AND {period_field}::text BETWEEN %s AND %s
                ORDER BY {period_field};
                """,
                (company_id, start, end),
            )
            rows = list(cur.fetchall())
        if not rows:
            return None
        return {
            "_view": view,
            "_period": period_part,
            "_window": True,
            "_period_start": start,
            "_period_end": end,
            "_n_rows": len(rows),
            "rows": [_row_to_safe(r) for r in rows],
        }

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            SELECT {select_clause}
            FROM {view}
            WHERE company_id = %s
              AND {period_field}::text = %s
            LIMIT 1;
            """,
            (company_id, period_part),
        )
        row = cur.fetchone()
    if row is None:
        return None
    safe = _row_to_safe(row)
    safe["_view"] = view
    safe["_period"] = period_part
    safe["_window"] = False
    return safe


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


@router.get("/evidence/{kind}/{evidence_id:path}")
def get_evidence(kind: str, evidence_id: str):
    """Return the raw row for a citation popup.

    ``kind`` is one of:
      F (financial_fact), T (transcript chunk), S (filing section chunk),
      M (metric-view row, body is ``view:company_id:period``).

    A (artifact) is not implemented yet; cite just renders as text.
    """
    kind = kind.upper()
    if kind not in {"F", "T", "S", "M"}:
        raise HTTPException(
            status_code=400,
            detail=f"unsupported evidence kind '{kind}'. Supported: F, T, S, M.",
        )
    with get_conn() as conn:
        if kind == "M":
            row = _fetch_metric_view_row(conn, evidence_id)
            if row is None:
                raise HTTPException(status_code=404, detail=f"no metric row for id '{evidence_id}'")
            return {"kind": "M", "id": evidence_id, "row": row}
        try:
            eid = int(evidence_id)
        except ValueError:
            raise HTTPException(status_code=400, detail="evidence id must be an integer for F/T/S")
        if kind == "F":
            row = _fetch_financial_fact(conn, eid)
        elif kind == "T":
            row = _fetch_transcript_chunk(conn, eid)
        else:
            row = _fetch_section_chunk(conn, eid)
    if row is None:
        raise HTTPException(status_code=404, detail=f"no {kind} evidence row for id {eid}")
    return {"kind": kind, "id": eid, "row": row}
