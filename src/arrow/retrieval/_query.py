"""Query and trace helpers shared by retrieval primitives and recipes.

Primitives use ``run_query`` for parameterized SELECTs that should optionally
record a TraceAction on a RuntimeTrace. Recipes (and the future agent loop) use
``record_action`` to wrap pure primitive calls with timing/trace metadata when
the primitive itself doesn't see a trace.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import asdict
from datetime import datetime
from decimal import Decimal
from typing import Any

import psycopg
from psycopg.rows import dict_row

from arrow.retrieval.types import RuntimeTrace, TraceAction


def jsonable(value: Any) -> Any:
    """Recursively convert dataclasses/Decimals/datetimes to JSON-safe values."""
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, list):
        return [jsonable(v) for v in value]
    if isinstance(value, tuple):
        return [jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}
    if hasattr(value, "__dict__"):
        return jsonable(asdict(value))
    return value


def param_hash(params: Any) -> str:
    payload = json.dumps(jsonable(params), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def run_query(
    conn: psycopg.Connection,
    *,
    sql: str,
    params: tuple[Any, ...],
    trace: RuntimeTrace | None = None,
    label: str | None = None,
    selected_id_keys: tuple[str, ...] = (),
) -> list[dict[str, Any]]:
    """Execute a SELECT and optionally record a TraceAction on ``trace``."""
    started = time.perf_counter()
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        rows = list(cur.fetchall())
    if trace is None or label is None:
        return rows
    duration_ms = (time.perf_counter() - started) * 1000
    selected_ids: list[str] = []
    for row in rows:
        for key in selected_id_keys:
            if row.get(key) is not None:
                selected_ids.append(f"{key}:{row[key]}")
    trace.actions.append(
        TraceAction(
            label=label,
            params_hash=param_hash(params),
            row_count=len(rows),
            duration_ms=round(duration_ms, 2),
            selected_ids=selected_ids,
        )
    )
    return rows


def record_action(
    trace: RuntimeTrace,
    *,
    label: str,
    params: Any,
    started: float,
    rows: list[Any],
    selected_ids: list[str] | None = None,
) -> None:
    """Append a TraceAction for a primitive call wrapped externally.

    Use this when a recipe calls a pure primitive and wants the trace shape to
    match the inline ``run_query`` calls.
    """
    duration_ms = (time.perf_counter() - started) * 1000
    trace.actions.append(
        TraceAction(
            label=label,
            params_hash=param_hash(params),
            row_count=len(rows),
            duration_ms=round(duration_ms, 2),
            selected_ids=selected_ids or [],
        )
    )
