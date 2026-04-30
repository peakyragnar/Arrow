"""Capture chat-driven triage sessions as structured records.

Every meaningful triage activity in chat (Claude Code, Codex, ...)
leaves one ``triage_session`` row so the autonomous-agent path has
training data: what surfaced, what was investigated, the operator's
reasoning, the actions taken, the outcome.

This is the V1 substrate for the future autonomous data-quality
operator agent (see ``docs/architecture/steward.md`` § LLM Trajectory).
The agent will read these rows via SQL+FTS — the same retrieval pattern
the analyst layer uses — to recognize patterns it has seen the
operator approve before.

Distinct from:
  - ``data_quality_findings.history`` (per-row state-change audit)
  - ``ingest_runs.counts`` (per-script execution metadata)

This module captures the higher-level loop: a chat session that ties
findings, investigations, and actions together. One row per session.

Conventions for ``created_by``:
  - ``human:michael``                — operator drove the analysis;
                                       reasoning is the operator's
  - ``claude:assistant_via_michael`` — AI investigated; operator
                                       approved (the honest label for
                                       most current sessions)
  - ``claude:assistant_triage``      — AI-driven, low operator review
                                       (avoid; flags pollution risk)
  - ``system:auto``                  — fully automated, no human
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import psycopg
from psycopg.types.json import Jsonb


def record_triage_session(
    conn: psycopg.Connection,
    *,
    intent: str,
    created_by: str,
    harness: str = "claude_code",
    finding_ids: list[int] | None = None,
    operator_quotes: list[str] | None = None,
    investigations: list[dict[str, Any]] | None = None,
    actions_taken: list[dict[str, Any]] | None = None,
    outcomes: dict[str, Any] | None = None,
    captured_pattern: str | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
    session_ref: str | None = None,
) -> int:
    """Insert one ``triage_session`` row. Returns the new id.

    Caller is responsible for the transaction (this function does not
    commit). All optional fields default to empty / None — the minimum
    valid session is ``intent`` + ``created_by``.
    """
    if not intent or not intent.strip():
        raise ValueError("intent is required")
    if not created_by or not created_by.strip():
        raise ValueError("created_by is required")
    if harness not in ("claude_code", "codex", "human_only", "other"):
        raise ValueError(
            f"unknown harness {harness!r}; "
            "expected one of claude_code, codex, human_only, other"
        )

    finished_at = finished_at or datetime.now(timezone.utc)

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO triage_session (
                started_at, finished_at, harness, intent,
                finding_ids, operator_quotes, investigations,
                actions_taken, outcomes, captured_pattern,
                session_ref, created_by
            ) VALUES (
                COALESCE(%s, now()), %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s
            )
            RETURNING id
            """,
            (
                started_at,
                finished_at,
                harness,
                intent,
                finding_ids or [],
                Jsonb(operator_quotes or []),
                Jsonb(investigations or []),
                Jsonb(actions_taken or []),
                Jsonb(outcomes or {}),
                captured_pattern,
                session_ref,
                created_by,
            ),
        )
        return cur.fetchone()[0]


def find_similar_sessions(
    conn: psycopg.Connection,
    *,
    intent_query: str | None = None,
    pattern_query: str | None = None,
    finding_ids: list[int] | None = None,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Search past sessions by FTS over intent / captured_pattern,
    or by finding-id overlap. Used by the future agent (and current
    chat sessions) to find precedents.

    Returns rows newest-first, capped at ``limit``.
    """
    where: list[str] = []
    params: list[Any] = []

    if intent_query:
        where.append(
            "to_tsvector('english', intent) @@ plainto_tsquery('english', %s)"
        )
        params.append(intent_query)
    if pattern_query:
        where.append(
            "to_tsvector('english', coalesce(captured_pattern, '')) "
            "@@ plainto_tsquery('english', %s)"
        )
        params.append(pattern_query)
    if finding_ids:
        where.append("finding_ids && %s::bigint[]")
        params.append(finding_ids)

    sql = [
        "SELECT id, started_at, finished_at, harness, intent,",
        "       finding_ids, operator_quotes, investigations,",
        "       actions_taken, outcomes, captured_pattern,",
        "       session_ref, created_by",
        "FROM triage_session",
    ]
    if where:
        sql.append("WHERE " + " AND ".join(where))
    sql.append("ORDER BY started_at DESC LIMIT %s")
    params.append(limit)

    with conn.cursor() as cur:
        cur.execute("\n".join(sql), params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
