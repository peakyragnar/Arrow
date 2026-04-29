"""LLM agent loop over the retrieval primitives.

Two-model design:

- **Haiku** runs the tool-use loop: parses the question, decides which
  tools to call, reads results, decides whether more retrieval is needed.
- **Sonnet** synthesizes the final answer once Haiku has gathered enough
  evidence.

Each question writes one JSONL trace under ``outputs/qa_runs/agent/`` with
per-tool latency, token counts, evidence yield, and citation coverage —
the substrate for tuning the planner and the tool registry over time.
"""

from __future__ import annotations

import json
import os
import re
import time
import uuid
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import anthropic
import psycopg
from dotenv import load_dotenv

from arrow.retrieval.companies import resolve_company_by_ticker
from arrow.retrieval.documents import get_section_chunks, list_documents
from arrow.retrieval.facts import get_financial_facts, get_segment_facts
from arrow.retrieval.metrics import get_metrics, metrics_view_name
from arrow.retrieval.transcripts import (
    get_latest_transcripts,
    search_transcript_turns,
)
from arrow.retrieval._query import jsonable


HAIKU_MODEL = "claude-haiku-4-5-20251001"
SONNET_MODEL = "claude-sonnet-4-6"
MAX_TOOL_ITERATIONS = 8
MAX_OUTPUT_TOKENS_HAIKU = 2048
MAX_OUTPUT_TOKENS_SONNET = 1500
CITATION_RE = re.compile(r"\[([A-Z]):([^\]]+)\]")


# --------------------------------------------------------------------------- #
# Tool registry
# --------------------------------------------------------------------------- #


@dataclass
class ToolResult:
    rows: list[dict[str, Any]]
    evidence_ids: list[str]
    summary: str
    error: str | None = None

    def as_content(self) -> str:
        if self.error:
            return json.dumps({"error": self.error}, default=str)
        return json.dumps(
            {
                "summary": self.summary,
                "rows": self.rows,
                "evidence_ids": self.evidence_ids,
            },
            default=str,
        )


@dataclass
class Tool:
    name: str
    description: str
    input_schema: dict[str, Any]
    execute: Callable[[psycopg.Connection, dict[str, Any]], ToolResult]

    def anthropic_spec(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "input_schema": self.input_schema,
        }


def _resolve_company(conn: psycopg.Connection, ticker: str):
    matches = resolve_company_by_ticker(conn, ticker_candidates=[ticker])
    return matches[0] if matches else None


def _period_type(fiscal_quarter: int | None) -> str:
    return "quarter" if fiscal_quarter else "annual"


def _money(value: Any) -> str | None:
    return None if value is None else str(value)


def _tool_resolve_company(conn: psycopg.Connection, params: dict[str, Any]) -> ToolResult:
    ticker = params["ticker"]
    matches = resolve_company_by_ticker(conn, ticker_candidates=[ticker])
    if not matches:
        return ToolResult(rows=[], evidence_ids=[], summary=f"No company found for ticker {ticker}.")
    return ToolResult(
        rows=[
            {
                "ticker": c.ticker,
                "company_id": c.id,
                "name": c.name,
                "cik": c.cik,
                "fiscal_year_end": c.fiscal_year_end_md,
            }
            for c in matches
        ],
        evidence_ids=[],
        summary=f"Resolved {len(matches)} company match(es) for {ticker}.",
    )


def _tool_get_metrics(conn: psycopg.Connection, params: dict[str, Any]) -> ToolResult:
    company = _resolve_company(conn, params["ticker"])
    if company is None:
        return ToolResult(rows=[], evidence_ids=[], summary=f"No company found for {params['ticker']}.")
    fiscal_quarter = params.get("fiscal_quarter")
    period_type = _period_type(fiscal_quarter)
    metric = get_metrics(
        conn,
        company_id=company.id,
        fiscal_year=params["fiscal_year"],
        fiscal_quarter=fiscal_quarter,
        period_type=period_type,
    )
    if metric is None:
        period_label = (
            f"FY{params['fiscal_year']} Q{fiscal_quarter}" if fiscal_quarter else f"FY{params['fiscal_year']}"
        )
        return ToolResult(
            rows=[],
            evidence_ids=[],
            summary=f"No metrics row for {company.ticker} {period_label}.",
        )
    row = {
        "ticker": metric.ticker,
        "fiscal_period": metric.fiscal_period_label,
        "period_end": str(metric.fy_end),
        "revenue": _money(metric.revenue_fy),
        "gross_margin": _money(metric.gross_margin_fy),
        "operating_margin": _money(metric.operating_margin_fy),
        "cfo": _money(metric.cfo_fy),
        "capital_expenditures": _money(metric.capital_expenditures_fy),
        "fcf": _money(metric.fcf_fy),
    }
    cite = f"M:{metrics_view_name(period_type)}:{company.id}:{metric.fiscal_period_label}"
    return ToolResult(
        rows=[row],
        evidence_ids=[cite],
        summary=f"{company.ticker} {metric.fiscal_period_label} metrics from {metrics_view_name(period_type)}.",
    )


def _tool_get_financial_facts(conn: psycopg.Connection, params: dict[str, Any]) -> ToolResult:
    company = _resolve_company(conn, params["ticker"])
    if company is None:
        return ToolResult(rows=[], evidence_ids=[], summary=f"No company found for {params['ticker']}.")
    fiscal_quarter = params.get("fiscal_quarter")
    period_type = _period_type(fiscal_quarter)
    concepts = params.get("concepts")
    facts = get_financial_facts(
        conn,
        company_id=company.id,
        fiscal_year=params["fiscal_year"],
        fiscal_quarter=fiscal_quarter,
        period_type=period_type,
        concepts=concepts,
    )
    if not facts:
        return ToolResult(
            rows=[],
            evidence_ids=[],
            summary=f"No facts for {company.ticker} {params['fiscal_year']}"
            f"{' Q' + str(fiscal_quarter) if fiscal_quarter else ''}.",
        )
    rows = [
        {
            "fact_id": f.fact_id,
            "statement": f.statement,
            "concept": f.concept,
            "value": _money(f.value),
            "unit": f.unit,
            "fiscal_period": f.fiscal_period_label,
            "period_end": str(f.period_end),
        }
        for f in facts
    ]
    return ToolResult(
        rows=rows,
        evidence_ids=[f"F:{f.fact_id}" for f in facts],
        summary=f"{company.ticker}: {len(facts)} fact(s).",
    )


def _tool_get_segment_facts(conn: psycopg.Connection, params: dict[str, Any]) -> ToolResult:
    company = _resolve_company(conn, params["ticker"])
    if company is None:
        return ToolResult(rows=[], evidence_ids=[], summary=f"No company found for {params['ticker']}.")
    fiscal_quarter = params.get("fiscal_quarter")
    period_type = _period_type(fiscal_quarter)
    facts = get_segment_facts(
        conn,
        company_id=company.id,
        fiscal_year=params["fiscal_year"],
        fiscal_quarter=fiscal_quarter,
        period_type=period_type,
    )
    if not facts:
        return ToolResult(rows=[], evidence_ids=[], summary=f"No segment revenue for {company.ticker}.")
    rows = [
        {
            "fact_id": f.fact_id,
            "dimension_type": f.dimension_type,
            "dimension_key": f.dimension_key,
            "dimension_label": f.dimension_label,
            "value": _money(f.value),
            "fiscal_period": f.fiscal_period_label,
            "period_end": str(f.period_end),
        }
        for f in facts
    ]
    return ToolResult(
        rows=rows,
        evidence_ids=[f"F:{f.fact_id}" for f in facts],
        summary=f"{company.ticker}: {len(facts)} segment row(s).",
    )


def _tool_list_documents(conn: psycopg.Connection, params: dict[str, Any]) -> ToolResult:
    company = _resolve_company(conn, params["ticker"])
    if company is None:
        return ToolResult(rows=[], evidence_ids=[], summary=f"No company found for {params['ticker']}.")
    fiscal_quarter = params.get("fiscal_quarter")
    period_type = _period_type(fiscal_quarter)
    fiscal_period_key = (
        f"FY{params['fiscal_year']} Q{fiscal_quarter}" if fiscal_quarter else None
    )
    artifacts = list_documents(
        conn,
        company_id=company.id,
        period_type=period_type,
        fiscal_year=params["fiscal_year"] if not fiscal_quarter else None,
        fiscal_period_key=fiscal_period_key,
    )
    if not artifacts:
        return ToolResult(rows=[], evidence_ids=[], summary=f"No documents for {company.ticker}.")
    rows = [
        {
            "artifact_id": a.artifact_id,
            "artifact_type": a.artifact_type,
            "fiscal_period_key": a.fiscal_period_key,
            "period_end": str(a.period_end),
            "published_at": str(a.published_at) if a.published_at else None,
            "accession_number": a.accession_number,
        }
        for a in artifacts
    ]
    return ToolResult(
        rows=rows,
        evidence_ids=[f"A:{a.artifact_id}" for a in artifacts],
        summary=f"{company.ticker}: {len(artifacts)} document(s).",
    )


def _tool_search_transcripts(conn: psycopg.Connection, params: dict[str, Any]) -> ToolResult:
    ticker = params["ticker"]
    query = params["query"]
    fiscal_period_key = params.get("fiscal_period_key")
    limit = int(params.get("limit", 10))
    turns = search_transcript_turns(
        conn,
        ticker,
        query,
        fiscal_period_key=fiscal_period_key,
        limit=limit,
    )
    if not turns:
        return ToolResult(rows=[], evidence_ids=[], summary=f"No transcript turns for {ticker} matching '{query}'.")
    rows = [
        {
            "chunk_id": t.chunk_id,
            "fiscal_period": t.fiscal_period_label,
            "speaker": t.speaker,
            "text": t.text[:1200],
        }
        for t in turns
    ]
    return ToolResult(
        rows=rows,
        evidence_ids=[f"T:{t.chunk_id}" for t in turns if t.chunk_id is not None],
        summary=f"{ticker}: {len(turns)} transcript turn(s) matching '{query}'.",
    )


def _tool_get_latest_transcripts(conn: psycopg.Connection, params: dict[str, Any]) -> ToolResult:
    ticker = params["ticker"]
    n = int(params.get("n", 4))
    docs = get_latest_transcripts(conn, ticker, n=n)
    if not docs:
        return ToolResult(rows=[], evidence_ids=[], summary=f"No transcripts for {ticker}.")
    rows = [
        {
            "artifact_id": d.artifact_id,
            "fiscal_period": d.fiscal_period_label,
            "period_end": str(d.period_end),
            "turn_count": d.turn_count,
            "chunk_count": d.chunk_count,
        }
        for d in docs
    ]
    return ToolResult(
        rows=rows,
        evidence_ids=[f"A:{d.artifact_id}" for d in docs],
        summary=f"{ticker}: {len(docs)} most-recent transcript(s).",
    )


def _tool_read_filing_sections(conn: psycopg.Connection, params: dict[str, Any]) -> ToolResult:
    company = _resolve_company(conn, params["ticker"])
    if company is None:
        return ToolResult(rows=[], evidence_ids=[], summary=f"No company found for {params['ticker']}.")
    fiscal_quarter = params.get("fiscal_quarter")
    fiscal_period_key = (
        f"FY{params['fiscal_year']} Q{fiscal_quarter}"
        if fiscal_quarter
        else f"FY{params['fiscal_year']}"
    )
    kind = params.get("kind", "mda")
    if kind == "mda":
        section_keys = ("item_7_mda", "part1_item2_mda")
    else:
        return ToolResult(
            rows=[],
            evidence_ids=[],
            summary=f"Unknown section kind '{kind}'. Supported: 'mda'.",
            error=f"Unsupported kind '{kind}'.",
        )
    chunks = get_section_chunks(
        conn,
        company_id=company.id,
        fiscal_period_key=fiscal_period_key,
        section_keys=section_keys,
        source_kind=kind,
        limit=int(params.get("limit", 10)),
    )
    if not chunks:
        return ToolResult(rows=[], evidence_ids=[], summary=f"No {kind} sections for {company.ticker} {fiscal_period_key}.")
    rows = [
        {
            "chunk_id": c.chunk_id,
            "section_key": c.unit_key,
            "section_title": c.unit_title,
            "fiscal_period_key": c.fiscal_period_key,
            "heading_path": list(c.heading_path or []),
            "text": c.text[:1500],
        }
        for c in chunks
    ]
    return ToolResult(
        rows=rows,
        evidence_ids=[f"S:{c.chunk_id}" for c in chunks],
        summary=f"{company.ticker} {fiscal_period_key} {kind}: {len(chunks)} chunk(s).",
    )


REGISTRY: list[Tool] = [
    Tool(
        name="resolve_company",
        description=(
            "Look up a company by ticker. Returns ticker, company_id, name, "
            "CIK, and fiscal year end. Call this first when a question names "
            "a ticker you have not yet resolved."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "ticker": {"type": "string", "description": "Stock ticker, e.g. NVDA, DELL."},
            },
            "required": ["ticker"],
        },
        execute=_tool_resolve_company,
    ),
    Tool(
        name="get_metrics",
        description=(
            "Canonical period metrics from the v_metrics_fy / v_metrics_q views: "
            "revenue, gross_margin, operating_margin, cfo, capital_expenditures, fcf. "
            "Use this for headline numbers like 'DELL FY2024 revenue'. Omit "
            "fiscal_quarter for annual metrics."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "fiscal_year": {"type": "integer"},
                "fiscal_quarter": {"type": "integer", "description": "1-4. Omit for full-year."},
            },
            "required": ["ticker", "fiscal_year"],
        },
        execute=_tool_get_metrics,
    ),
    Tool(
        name="get_financial_facts",
        description=(
            "Raw stored financial facts for one (company, period). Filter by "
            "concept name(s) like ['revenue', 'gross_profit', 'cfo']. Use this "
            "when you need fact_ids to cite or when the metric view doesn't "
            "carry the concept you need."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "fiscal_year": {"type": "integer"},
                "fiscal_quarter": {"type": "integer"},
                "concepts": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Concept names to filter to. Omit to return all concepts.",
                },
            },
            "required": ["ticker", "fiscal_year"],
        },
        execute=_tool_get_financial_facts,
    ),
    Tool(
        name="get_segment_facts",
        description=(
            "Segment revenue breakdown for one (company, period). Returns one "
            "row per (dimension_type, dimension_key) — e.g. operating segments, "
            "geographies, products. Use this when a question asks about mix or "
            "segment growth."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "fiscal_year": {"type": "integer"},
                "fiscal_quarter": {"type": "integer"},
            },
            "required": ["ticker", "fiscal_year"],
        },
        execute=_tool_get_segment_facts,
    ),
    Tool(
        name="list_documents",
        description=(
            "List artifacts (10-K, 10-Q, 8-K, press release, transcript) for "
            "one company-period. Use this to discover what documents exist "
            "before reading text."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "fiscal_year": {"type": "integer"},
                "fiscal_quarter": {"type": "integer"},
            },
            "required": ["ticker", "fiscal_year"],
        },
        execute=_tool_list_documents,
    ),
    Tool(
        name="search_transcripts",
        description=(
            "Full-text search transcript turns for a ticker. Optionally scope "
            "to one fiscal_period_key like 'FY2025 Q3'. Returns ranked speaker "
            "turns with chunk_ids you can cite as [T:chunk_id]."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "query": {"type": "string", "description": "FTS query, e.g. 'sovereign AI' or 'guidance OR outlook'."},
                "fiscal_period_key": {
                    "type": "string",
                    "description": "Optional scope, e.g. 'FY2025 Q3'.",
                },
                "limit": {"type": "integer", "description": "Max turns (default 10)."},
            },
            "required": ["ticker", "query"],
        },
        execute=_tool_search_transcripts,
    ),
    Tool(
        name="get_latest_transcripts",
        description=(
            "List the N most recent transcript artifacts for a ticker. Useful "
            "for orienting a multi-period transcript question before searching."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "n": {"type": "integer", "description": "How many recent transcripts (default 4)."},
            },
            "required": ["ticker"],
        },
        execute=_tool_get_latest_transcripts,
    ),
    Tool(
        name="read_filing_sections",
        description=(
            "Read parsed sections from a 10-K or 10-Q for one company-period. "
            "kind='mda' returns Item 7 / Part I Item 2 management discussion. "
            "Returns chunks you can cite as [S:chunk_id]."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "fiscal_year": {"type": "integer"},
                "fiscal_quarter": {"type": "integer"},
                "kind": {"type": "string", "enum": ["mda"], "description": "Section kind. Currently 'mda'."},
                "limit": {"type": "integer", "description": "Max chunks (default 10)."},
            },
            "required": ["ticker", "fiscal_year"],
        },
        execute=_tool_read_filing_sections,
    ),
]

REGISTRY_BY_NAME = {t.name: t for t in REGISTRY}


# --------------------------------------------------------------------------- #
# Trace
# --------------------------------------------------------------------------- #


@dataclass
class ToolExecution:
    name: str
    params: dict[str, Any]
    started_at: str
    duration_ms: float
    row_count: int
    evidence_ids: list[str]
    error: str | None
    cited_in_answer: bool = False


@dataclass
class ModelCall:
    model: str
    role: str  # "router" | "synthesizer"
    duration_ms: float
    input_tokens: int
    output_tokens: int
    stop_reason: str | None


@dataclass
class AgentTrace:
    trace_id: str
    question: str
    started_at: str
    finished_at: str | None = None
    duration_ms: float = 0.0
    tool_executions: list[ToolExecution] = field(default_factory=list)
    model_calls: list[ModelCall] = field(default_factory=list)
    answer: str | None = None
    citations: list[str] = field(default_factory=list)
    verifier_status: str | None = None
    verifier_issues: list[str] = field(default_factory=list)
    error: str | None = None

    def write(self, out_dir: Path | None = None) -> Path:
        out_dir = out_dir or Path("outputs/qa_runs/agent")
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"{datetime.now(UTC).date().isoformat()}.jsonl"
        payload = jsonable(self)
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, sort_keys=True, default=str) + "\n")
        return path


# --------------------------------------------------------------------------- #
# Agent loop
# --------------------------------------------------------------------------- #


_PLANNER_SYSTEM = """You are the routing model for an analyst data system over US public-company financials, transcripts, and SEC filings.

Your job: read the user's question, call the available tools to gather grounded evidence, then signal you are done. Do NOT write the final answer — a separate model handles synthesis.

Rules:
- Resolve tickers via resolve_company before deeper queries when in doubt.
- Prefer get_metrics for headline numbers; use get_financial_facts when you need fact_ids or non-canonical concepts.
- For 'what changed' or 'what did management say' questions, use search_transcripts with focused FTS queries.
- Stop calling tools when you have enough evidence. When done, produce a brief plain-text note like 'Evidence gathered.' — synthesis happens elsewhere.
- Do NOT fabricate evidence; if a tool returns no rows, accept that and either try a different query or stop.
- Hard cap: 8 tool iterations.
"""


_SYNTH_SYSTEM = """You are a financial analyst writing a grounded answer.

Rules:
- Every numeric or substantive claim must include a citation in the form [F:123], [T:456], [S:789], [M:view:co:period], or [A:artifact_id].
- ONE id per bracket — write [F:1] [F:2] not [F:1, F:2]. Never combine multiple ids inside one pair of brackets.
- Cite ONLY from the allowed citation IDs provided. Never invent IDs.
- If evidence is missing or thin, say so explicitly. Do not extrapolate.
- Answer only what was asked — don't volunteer adjacent metrics the question didn't request.
- 1-3 short paragraphs. Plain prose. No headers or bullets unless the answer truly needs them.
"""


def _model_call_metrics(response, model: str, role: str, duration_ms: float) -> ModelCall:
    usage = getattr(response, "usage", None)
    return ModelCall(
        model=model,
        role=role,
        duration_ms=round(duration_ms, 2),
        input_tokens=getattr(usage, "input_tokens", 0) if usage else 0,
        output_tokens=getattr(usage, "output_tokens", 0) if usage else 0,
        stop_reason=getattr(response, "stop_reason", None),
    )


def _run_planner_loop(
    client: anthropic.Anthropic,
    conn: psycopg.Connection,
    question: str,
    trace: AgentTrace,
) -> tuple[list[ToolExecution], list[dict[str, Any]]]:
    """Run the Haiku tool-use loop. Returns the executions and the final
    Anthropic message history (so the synthesizer can replay the evidence)."""
    tool_specs = [t.anthropic_spec() for t in REGISTRY]
    messages: list[dict[str, Any]] = [{"role": "user", "content": question}]
    executions: list[ToolExecution] = []

    for _ in range(MAX_TOOL_ITERATIONS):
        started = time.perf_counter()
        response = client.messages.create(
            model=HAIKU_MODEL,
            max_tokens=MAX_OUTPUT_TOKENS_HAIKU,
            system=_PLANNER_SYSTEM,
            tools=tool_specs,
            messages=messages,
        )
        trace.model_calls.append(
            _model_call_metrics(response, HAIKU_MODEL, "router", (time.perf_counter() - started) * 1000)
        )

        if response.stop_reason != "tool_use":
            messages.append({"role": "assistant", "content": response.content})
            break

        messages.append({"role": "assistant", "content": response.content})
        tool_use_blocks = [b for b in response.content if b.type == "tool_use"]
        tool_results_content: list[dict[str, Any]] = []
        for block in tool_use_blocks:
            tool = REGISTRY_BY_NAME.get(block.name)
            tool_started = time.perf_counter()
            if tool is None:
                exec_record = ToolExecution(
                    name=block.name,
                    params=dict(block.input),
                    started_at=datetime.now(UTC).isoformat(),
                    duration_ms=0.0,
                    row_count=0,
                    evidence_ids=[],
                    error=f"unknown tool: {block.name}",
                )
                executions.append(exec_record)
                tool_results_content.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": json.dumps({"error": exec_record.error}),
                        "is_error": True,
                    }
                )
                continue
            try:
                result = tool.execute(conn, dict(block.input))
                duration_ms = (time.perf_counter() - tool_started) * 1000
                exec_record = ToolExecution(
                    name=tool.name,
                    params=dict(block.input),
                    started_at=datetime.now(UTC).isoformat(),
                    duration_ms=round(duration_ms, 2),
                    row_count=len(result.rows),
                    evidence_ids=result.evidence_ids,
                    error=result.error,
                )
                executions.append(exec_record)
                tool_results_content.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result.as_content(),
                        "is_error": result.error is not None,
                    }
                )
            except Exception as exc:
                duration_ms = (time.perf_counter() - tool_started) * 1000
                exec_record = ToolExecution(
                    name=tool.name,
                    params=dict(block.input),
                    started_at=datetime.now(UTC).isoformat(),
                    duration_ms=round(duration_ms, 2),
                    row_count=0,
                    evidence_ids=[],
                    error=f"{type(exc).__name__}: {exc}",
                )
                executions.append(exec_record)
                tool_results_content.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": json.dumps({"error": exec_record.error}),
                        "is_error": True,
                    }
                )

        messages.append({"role": "user", "content": tool_results_content})

    return executions, messages


def _format_evidence_for_synthesis(executions: list[ToolExecution], conn: psycopg.Connection) -> str:
    """Re-execute None — we already have the rows in the planner messages."""
    raise NotImplementedError  # not used; kept as a reminder


def _build_synthesis_prompt(
    question: str,
    executions: list[ToolExecution],
    planner_messages: list[dict[str, Any]],
) -> tuple[str, list[str]]:
    """Build the synthesis user prompt from the gathered tool results.

    Returns (prompt_text, allowed_citation_ids).
    """
    allowed_ids: list[str] = []
    evidence_blocks: list[str] = []

    for msg in planner_messages:
        if msg["role"] != "user":
            continue
        content = msg["content"]
        if not isinstance(content, list):
            continue
        for item in content:
            if isinstance(item, dict) and item.get("type") == "tool_result":
                evidence_blocks.append(item.get("content", ""))

    for execution in executions:
        for cid in execution.evidence_ids:
            if cid not in allowed_ids:
                allowed_ids.append(cid)

    evidence_section = "\n\n".join(f"<tool_result>\n{block}\n</tool_result>" for block in evidence_blocks)
    if not evidence_section:
        evidence_section = "(no tool results)"

    allowed = ", ".join(allowed_ids) if allowed_ids else "(none)"
    prompt = (
        f"Question:\n{question}\n\n"
        f"Evidence gathered by the routing model:\n{evidence_section}\n\n"
        f"Allowed citation IDs (cite ONLY from these):\n{allowed}\n\n"
        "Write a grounded answer."
    )
    return prompt, allowed_ids


def _split_bracket_body(kind: str, body: str) -> list[str]:
    """Handle accidental multi-id brackets like [M:a, M:b]."""
    parts = [p.strip() for p in body.split(",")]
    out: list[str] = []
    for part in parts:
        if not part:
            continue
        if ":" in part and part[0].isupper():
            out.append(part)
        else:
            out.append(f"{kind}:{part}")
    return out


def _verify_citations(answer: str, allowed_ids: list[str]) -> tuple[str, list[str]]:
    issues: list[str] = []
    allowed_set = set(allowed_ids)
    for kind, body in CITATION_RE.findall(answer):
        for cite in _split_bracket_body(kind, body):
            if cite not in allowed_set:
                issues.append(f"[{cite}] is not in the allowed evidence set.")
    status = "verified" if not issues else "unverified"
    return status, issues


def _extract_citations(answer: str) -> list[str]:
    found: list[str] = []
    for kind, body in CITATION_RE.findall(answer):
        for cite in _split_bracket_body(kind, body):
            if cite not in found:
                found.append(cite)
    return found


def _mark_cited_executions(executions: list[ToolExecution], citations: list[str]) -> None:
    cite_set = set(citations)
    for execution in executions:
        if any(eid in cite_set for eid in execution.evidence_ids):
            execution.cited_in_answer = True


def ask(
    question: str,
    *,
    conn: psycopg.Connection | None = None,
    client: anthropic.Anthropic | None = None,
    out_dir: Path | None = None,
) -> AgentTrace:
    """Answer one question and return the persisted trace."""
    load_dotenv(override=True)
    if not os.environ.get("ANTHROPIC_API_KEY"):
        raise RuntimeError("ANTHROPIC_API_KEY is not set in the environment.")

    from arrow.db.connection import get_conn

    trace = AgentTrace(
        trace_id=str(uuid.uuid4()),
        question=question,
        started_at=datetime.now(UTC).isoformat(),
    )
    started_total = time.perf_counter()

    owns_conn = conn is None
    owns_client = client is None
    conn_cm = get_conn() if owns_conn else None
    if owns_conn:
        conn = conn_cm.__enter__()
    if owns_client:
        client = anthropic.Anthropic()

    try:
        executions, planner_messages = _run_planner_loop(client, conn, question, trace)
        trace.tool_executions = executions

        synth_prompt, allowed_ids = _build_synthesis_prompt(question, executions, planner_messages)
        synth_started = time.perf_counter()
        synth_response = client.messages.create(
            model=SONNET_MODEL,
            max_tokens=MAX_OUTPUT_TOKENS_SONNET,
            system=_SYNTH_SYSTEM,
            messages=[{"role": "user", "content": synth_prompt}],
        )
        trace.model_calls.append(
            _model_call_metrics(
                synth_response, SONNET_MODEL, "synthesizer", (time.perf_counter() - synth_started) * 1000
            )
        )

        answer_parts = [b.text for b in synth_response.content if getattr(b, "type", None) == "text"]
        answer = "\n".join(answer_parts).strip()
        trace.answer = answer
        trace.citations = _extract_citations(answer)
        trace.verifier_status, trace.verifier_issues = _verify_citations(answer, allowed_ids)
        _mark_cited_executions(executions, trace.citations)

    except Exception as exc:
        trace.error = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        trace.finished_at = datetime.now(UTC).isoformat()
        trace.duration_ms = round((time.perf_counter() - started_total) * 1000, 2)
        try:
            trace.write(out_dir)
        except Exception:
            pass
        if owns_conn and conn_cm is not None:
            conn_cm.__exit__(None, None, None)

    return trace


# --------------------------------------------------------------------------- #
# Streaming variant — emits per-step events for an async UI consumer.
# --------------------------------------------------------------------------- #


import asyncio
from collections.abc import AsyncIterator


def _execute_tool_block(
    conn: psycopg.Connection,
    block: Any,
) -> tuple[ToolExecution, dict[str, Any]]:
    """Run one tool_use block. Returns (execution_record, tool_result_payload)."""
    tool = REGISTRY_BY_NAME.get(block.name)
    tool_started = time.perf_counter()
    if tool is None:
        execution = ToolExecution(
            name=block.name,
            params=dict(block.input),
            started_at=datetime.now(UTC).isoformat(),
            duration_ms=0.0,
            row_count=0,
            evidence_ids=[],
            error=f"unknown tool: {block.name}",
        )
        payload = {
            "type": "tool_result",
            "tool_use_id": block.id,
            "content": json.dumps({"error": execution.error}),
            "is_error": True,
        }
        return execution, payload
    try:
        result = tool.execute(conn, dict(block.input))
        duration_ms = (time.perf_counter() - tool_started) * 1000
        execution = ToolExecution(
            name=tool.name,
            params=dict(block.input),
            started_at=datetime.now(UTC).isoformat(),
            duration_ms=round(duration_ms, 2),
            row_count=len(result.rows),
            evidence_ids=result.evidence_ids,
            error=result.error,
        )
        payload = {
            "type": "tool_result",
            "tool_use_id": block.id,
            "content": result.as_content(),
            "is_error": result.error is not None,
        }
        return execution, payload
    except Exception as exc:
        duration_ms = (time.perf_counter() - tool_started) * 1000
        execution = ToolExecution(
            name=tool.name,
            params=dict(block.input),
            started_at=datetime.now(UTC).isoformat(),
            duration_ms=round(duration_ms, 2),
            row_count=0,
            evidence_ids=[],
            error=f"{type(exc).__name__}: {exc}",
        )
        payload = {
            "type": "tool_result",
            "tool_use_id": block.id,
            "content": json.dumps({"error": execution.error}),
            "is_error": True,
        }
        return execution, payload


async def ask_stream(
    question: str,
    *,
    out_dir: Path | None = None,
) -> AsyncIterator[dict[str, Any]]:
    """Async generator: run the agent and yield per-step events.

    Events:
      {"event": "started", "trace_id": ..., "question": ...}
      {"event": "router_turn", "iteration": N, "tokens_in": ..., "tokens_out": ..., "duration_ms": ...}
      {"event": "tool_call", "name": ..., "params": {...}}
      {"event": "tool_result", "name": ..., "row_count": ..., "evidence_ids": [...], "error": ... | None}
      {"event": "synthesizing"}
      {"event": "answer", "text": ..., "citations": [...]}
      {"event": "verifier", "status": ..., "issues": [...]}
      {"event": "done", "trace_id": ..., "duration_ms": ..., "input_tokens_total": ..., "output_tokens_total": ...}
      {"event": "error", "message": ...}
    """
    load_dotenv(override=True)
    if not os.environ.get("ANTHROPIC_API_KEY"):
        yield {"event": "error", "message": "ANTHROPIC_API_KEY is not set in the environment."}
        return

    from arrow.db.connection import get_conn

    trace = AgentTrace(
        trace_id=str(uuid.uuid4()),
        question=question,
        started_at=datetime.now(UTC).isoformat(),
    )
    started_total = time.perf_counter()
    yield {"event": "started", "trace_id": trace.trace_id, "question": question}

    client = anthropic.Anthropic()
    conn_cm = get_conn()
    conn = await asyncio.to_thread(conn_cm.__enter__)
    tool_specs = [t.anthropic_spec() for t in REGISTRY]
    messages: list[dict[str, Any]] = [{"role": "user", "content": question}]
    executions: list[ToolExecution] = []
    allowed_ids: list[str] = []

    try:
        for iteration in range(MAX_TOOL_ITERATIONS):
            turn_started = time.perf_counter()
            response = await asyncio.to_thread(
                client.messages.create,
                model=HAIKU_MODEL,
                max_tokens=MAX_OUTPUT_TOKENS_HAIKU,
                system=_PLANNER_SYSTEM,
                tools=tool_specs,
                messages=messages,
            )
            turn_ms = (time.perf_counter() - turn_started) * 1000
            trace.model_calls.append(
                _model_call_metrics(response, HAIKU_MODEL, "router", turn_ms)
            )
            usage = getattr(response, "usage", None)
            yield {
                "event": "router_turn",
                "iteration": iteration + 1,
                "tokens_in": getattr(usage, "input_tokens", 0) if usage else 0,
                "tokens_out": getattr(usage, "output_tokens", 0) if usage else 0,
                "duration_ms": round(turn_ms, 2),
                "stop_reason": response.stop_reason,
            }

            if response.stop_reason != "tool_use":
                messages.append({"role": "assistant", "content": response.content})
                break

            messages.append({"role": "assistant", "content": response.content})
            tool_use_blocks = [b for b in response.content if b.type == "tool_use"]
            tool_results_content: list[dict[str, Any]] = []

            for block in tool_use_blocks:
                yield {
                    "event": "tool_call",
                    "name": block.name,
                    "params": dict(block.input),
                }
                execution, payload = await asyncio.to_thread(_execute_tool_block, conn, block)
                executions.append(execution)
                tool_results_content.append(payload)
                for cid in execution.evidence_ids:
                    if cid not in allowed_ids:
                        allowed_ids.append(cid)
                yield {
                    "event": "tool_result",
                    "name": execution.name,
                    "row_count": execution.row_count,
                    "evidence_ids": execution.evidence_ids,
                    "duration_ms": execution.duration_ms,
                    "error": execution.error,
                }

            messages.append({"role": "user", "content": tool_results_content})

        trace.tool_executions = executions
        synth_prompt, allowed_ids = _build_synthesis_prompt(question, executions, messages)
        yield {"event": "synthesizing"}

        synth_started = time.perf_counter()
        synth_response = await asyncio.to_thread(
            client.messages.create,
            model=SONNET_MODEL,
            max_tokens=MAX_OUTPUT_TOKENS_SONNET,
            system=_SYNTH_SYSTEM,
            messages=[{"role": "user", "content": synth_prompt}],
        )
        synth_ms = (time.perf_counter() - synth_started) * 1000
        trace.model_calls.append(
            _model_call_metrics(synth_response, SONNET_MODEL, "synthesizer", synth_ms)
        )

        answer_parts = [b.text for b in synth_response.content if getattr(b, "type", None) == "text"]
        answer = "\n".join(answer_parts).strip()
        trace.answer = answer
        trace.citations = _extract_citations(answer)
        trace.verifier_status, trace.verifier_issues = _verify_citations(answer, allowed_ids)
        _mark_cited_executions(executions, trace.citations)

        yield {"event": "answer", "text": answer, "citations": trace.citations}
        yield {
            "event": "verifier",
            "status": trace.verifier_status,
            "issues": trace.verifier_issues,
        }

    except Exception as exc:
        trace.error = f"{type(exc).__name__}: {exc}"
        yield {"event": "error", "message": trace.error}
    finally:
        trace.finished_at = datetime.now(UTC).isoformat()
        trace.duration_ms = round((time.perf_counter() - started_total) * 1000, 2)
        try:
            await asyncio.to_thread(trace.write, out_dir)
        except Exception:
            pass
        try:
            await asyncio.to_thread(conn_cm.__exit__, None, None, None)
        except Exception:
            pass

    total_in = sum(mc.input_tokens for mc in trace.model_calls)
    total_out = sum(mc.output_tokens for mc in trace.model_calls)
    yield {
        "event": "done",
        "trace_id": trace.trace_id,
        "duration_ms": trace.duration_ms,
        "input_tokens_total": total_in,
        "output_tokens_total": total_out,
        "verifier_status": trace.verifier_status,
    }
