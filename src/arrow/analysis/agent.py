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
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import anthropic
import psycopg
from dotenv import load_dotenv

from arrow.retrieval.companies import resolve_company_by_ticker
from arrow.retrieval.documents import get_section_chunks, list_documents
from arrow.retrieval.facts import get_financial_facts, get_segment_facts
from arrow.retrieval.metrics import (
    get_metrics,
    get_quarterly_metrics_series,
    metrics_view_name,
)
from arrow.retrieval.screens import (
    count_universe_for_metric,
    get_latest_roic,
    list_companies,
    metric_value_kind,
    screen_companies_by_metric,
    screen_companies_by_trajectory,
    supported_metrics,
)
from arrow.retrieval.transcripts import (
    compare_transcript_mentions,
    get_latest_transcripts,
    read_transcript_turns,
    search_transcript_turns,
)
from arrow.retrieval.prices import (
    latest_price_date,
    read_prices,
    read_valuation,
    read_valuation_series,
    valuation_percentile,
)
from arrow.retrieval.estimates import (
    EstimateWarning,
    read_consensus,
    read_estimate_warnings,
    read_surprise_history,
    read_target_gap,
    recent_analyst_actions,
)
from arrow.retrieval._query import jsonable


HAIKU_MODEL = "claude-haiku-4-5-20251001"
SONNET_MODEL = "claude-sonnet-4-6"
OPUS_MODEL = "claude-opus-4-7"
#: Iteration cap on planner ↔ tool round-trips per question. Each iteration
#: is one router model call + one tool execution. Cost driver is the router
#: (haiku, ~2k input tokens cached + ~150 output); tool exec is free (DB).
#: Bumped from 8 → 16 (2026-04-30) so multi-dimensional cross-ticker
#: questions ("compare valuation + 5yr percentile + quarterly trend +
#: transcript narrative across 4 names") have room to compose. Smaller
#: questions still finish in 1-3 iterations as before.
MAX_TOOL_ITERATIONS = 16
MAX_OUTPUT_TOKENS_HAIKU = 2048
MAX_OUTPUT_TOKENS_SONNET = 1500
MAX_OUTPUT_TOKENS_OPUS = 4096

#: Synthesizer-model registry. Keys are the short names the UI / API
#: accepts; values are (model_id, max_tokens). Sonnet is the default —
#: fast and cheap for testing. Opus is the considered-answer model.
SYNTHESIZER_MODELS: dict[str, tuple[str, int]] = {
    "sonnet": (SONNET_MODEL, MAX_OUTPUT_TOKENS_SONNET),
    "opus": (OPUS_MODEL, MAX_OUTPUT_TOKENS_OPUS),
}
DEFAULT_SYNTHESIZER = "sonnet"


def _resolve_synthesizer(name: str | None) -> tuple[str, int]:
    """Return (model_id, max_tokens) for the requested synthesizer name.
    Falls back to the default if name is None or unknown."""
    if not name:
        return SYNTHESIZER_MODELS[DEFAULT_SYNTHESIZER]
    return SYNTHESIZER_MODELS.get(name.lower(), SYNTHESIZER_MODELS[DEFAULT_SYNTHESIZER])

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
    evidence_ids = [cite]

    # Attach the most-recent-at-or-before ROIC for annual metrics. Quarterly
    # metrics already have their own period_end; the ROIC view is quarterly,
    # so pulling latest at or before fy_end gives the year-end snapshot.
    if metric.fy_end is not None:
        roic, roic_period = get_latest_roic(
            conn, company_id=company.id, on_or_before=metric.fy_end
        )
        if roic is not None:
            row["roic"] = _money(roic)
            row["roic_period_end"] = roic_period
            evidence_ids.append(f"M:v_metrics_roic:{company.id}:{roic_period}")

    return ToolResult(
        rows=[row],
        evidence_ids=evidence_ids,
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


def _tool_read_transcript(conn: psycopg.Connection, params: dict[str, Any]) -> ToolResult:
    ticker = params["ticker"]
    fiscal_year = params["fiscal_year"]
    fiscal_quarter = params.get("fiscal_quarter")
    if fiscal_quarter:
        fiscal_period_key = f"FY{fiscal_year} Q{fiscal_quarter}"
    else:
        fiscal_period_key = f"FY{fiscal_year} Q4"  # annual calls map to Q4
    turns = read_transcript_turns(conn, ticker, fiscal_period_key)
    if not turns:
        return ToolResult(
            rows=[],
            evidence_ids=[],
            summary=f"No transcript found for {ticker} {fiscal_period_key}.",
        )
    rows = [
        {
            "chunk_id": t.chunk_id,
            "unit_ordinal": t.unit_ordinal,
            "speaker": t.speaker,
            "fiscal_period": t.fiscal_period_label,
            "text": t.text,
        }
        for t in turns
    ]
    return ToolResult(
        rows=rows,
        evidence_ids=[f"T:{t.chunk_id}" for t in turns if t.chunk_id is not None],
        summary=f"{ticker} {fiscal_period_key} transcript: {len(turns)} turn(s) in reading order.",
    )


def _parse_iso_date(s: str | None) -> "date | None":
    if not s:
        return None
    from datetime import date as _date
    y, m, d = s.split("-")
    return _date(int(y), int(m), int(d))


def _tool_read_prices(conn: psycopg.Connection, params: dict[str, Any]) -> ToolResult:
    ticker = params["ticker"].upper()
    from_date = _parse_iso_date(params.get("from_date"))
    to_date = _parse_iso_date(params.get("to_date"))
    if from_date is None or to_date is None:
        return ToolResult(
            rows=[], evidence_ids=[],
            summary="from_date and to_date are required (YYYY-MM-DD).",
            error="from_date and to_date are required",
        )

    bars = read_prices(conn, ticker=ticker, from_date=from_date, to_date=to_date)
    if not bars:
        return ToolResult(
            rows=[], evidence_ids=[],
            summary=f"No prices for {ticker} in [{from_date}, {to_date}].",
        )

    rows = []
    evidence_ids: list[str] = []
    for b in bars:
        cite = f"P:{b.security_id}:{b.date.isoformat()}"
        rows.append({
            "date": b.date.isoformat(),
            "close": _money(b.close),
            "adj_close": _money(b.adj_close),
            "open": _money(b.open),
            "high": _money(b.high),
            "low": _money(b.low),
            "volume": b.volume,
            "evidence_id": cite,
        })
        evidence_ids.append(cite)
    first_close = bars[0].adj_close
    last_close = bars[-1].adj_close
    pct_change = None
    if first_close:
        pct_change = float((last_close - first_close) / first_close) * 100.0
    summary = (
        f"{ticker}: {len(bars)} trading day(s) {bars[0].date}..{bars[-1].date}. "
        f"adj_close {bars[0].adj_close} → {bars[-1].adj_close}"
        + (f" ({pct_change:+.2f}% total return)." if pct_change is not None else ".")
        + f" Cite individual bars as P:{bars[0].security_id}:YYYY-MM-DD."
    )
    return ToolResult(rows=rows, evidence_ids=evidence_ids, summary=summary)


def _tool_read_valuation(conn: psycopg.Connection, params: dict[str, Any]) -> ToolResult:
    ticker = params["ticker"].upper()
    as_of = _parse_iso_date(params.get("as_of"))

    val = read_valuation(conn, ticker=ticker, as_of=as_of)
    if val is None:
        if as_of is None:
            return ToolResult(
                rows=[], evidence_ids=[],
                summary=f"No valuation data for {ticker} (no prices loaded).",
            )
        return ToolResult(
            rows=[], evidence_ids=[],
            summary=(
                f"No valuation row for {ticker} on {as_of} — likely a "
                f"non-trading day. Try the nearest weekday."
            ),
        )

    row = {
        "ticker": val.ticker,
        "date": val.date.isoformat(),
        "close": _money(val.close),
        "adj_close": _money(val.adj_close),
        "market_cap": _money(val.market_cap),
        "ev": _money(val.ev),
        "fiscal_period_label_at_asof": val.fiscal_period_label_at_asof,
        "components_known_since": (
            val.components_known_since.isoformat() if val.components_known_since else None
        ),
        "quarters_in_window": val.quarters_in_window,
        "pe_ttm": _money(val.pe_ttm),
        "ps_ttm": _money(val.ps_ttm),
        "ev_ebitda_ttm": _money(val.ev_ebitda_ttm),
        "fcf_yield_ttm": _money(val.fcf_yield_ttm),
        "components": {
            "ttm_net_income": _money(val.ttm_net_income),
            "ttm_revenue": _money(val.ttm_revenue),
            "ttm_operating_income": _money(val.ttm_operating_income),
            "ttm_dna": _money(val.ttm_dna),
            "ttm_ebitda": _money(val.ttm_ebitda),
            "ttm_cfo": _money(val.ttm_cfo),
            "ttm_capex": _money(val.ttm_capex),
            "ttm_fcf": _money(val.ttm_fcf),
            "cash_and_equivalents": _money(val.cash_and_equivalents),
            "short_term_investments": _money(val.short_term_investments),
            "long_term_debt": _money(val.long_term_debt),
            "current_portion_lt_debt": _money(val.current_portion_lt_debt),
            "noncontrolling_interest": _money(val.noncontrolling_interest),
        },
    }

    cite = (
        f"M:v_valuation_ratios_ttm:{val.security_id}:{val.date.isoformat()}"
    )
    summary = (
        f"{ticker} valuation on {val.date} (TTM as of {val.fiscal_period_label_at_asof}): "
        f"P/E {val.pe_ttm}, P/S {val.ps_ttm}, EV/EBITDA {val.ev_ebitda_ttm}, "
        f"FCF yield {val.fcf_yield_ttm}. PIT — uses financials known on {val.date}, "
        f"NOT recomputed with hindsight."
    )
    return ToolResult(rows=[row], evidence_ids=[cite], summary=summary)


def _tool_read_valuation_series(conn: psycopg.Connection, params: dict[str, Any]) -> ToolResult:
    ticker = params["ticker"].upper()
    from_date = _parse_iso_date(params.get("from_date"))
    to_date = _parse_iso_date(params.get("to_date"))
    sample = params.get("sample", "monthly")
    if from_date is None or to_date is None:
        return ToolResult(
            rows=[], evidence_ids=[],
            summary="from_date and to_date are required (YYYY-MM-DD).",
            error="from_date and to_date are required",
        )
    try:
        points = read_valuation_series(
            conn, ticker=ticker,
            from_date=from_date, to_date=to_date, sample=sample,
        )
    except ValueError as e:
        return ToolResult(rows=[], evidence_ids=[], summary=str(e), error=str(e))
    if not points:
        return ToolResult(
            rows=[], evidence_ids=[],
            summary=f"No valuation series for {ticker} in [{from_date}, {to_date}].",
        )

    rows = []
    evidence_ids: list[str] = []
    for p in points:
        cite = f"M:v_valuation_ratios_ttm:{p.security_id}:{p.date.isoformat()}"
        rows.append({
            "date": p.date.isoformat(),
            "market_cap": _money(p.market_cap),
            "pe_ttm": _money(p.pe_ttm),
            "ps_ttm": _money(p.ps_ttm),
            "ev_ebitda_ttm": _money(p.ev_ebitda_ttm),
            "fcf_yield_ttm": _money(p.fcf_yield_ttm),
            "fiscal_period_at_asof": p.fiscal_period_label_at_asof,
            "evidence_id": cite,
        })
        evidence_ids.append(cite)
    summary = (
        f"{ticker}: {len(points)} valuation samples ({sample}) "
        f"{points[0].date}..{points[-1].date}. "
        f"P/E first {points[0].pe_ttm} → last {points[-1].pe_ttm}."
    )
    return ToolResult(rows=rows, evidence_ids=evidence_ids, summary=summary)


def _tool_valuation_percentile(conn: psycopg.Connection, params: dict[str, Any]) -> ToolResult:
    ticker = params["ticker"].upper()
    as_of = _parse_iso_date(params.get("as_of"))
    window_years = int(params.get("window_years") or 5)

    vp = valuation_percentile(
        conn, ticker=ticker, as_of=as_of, window_years=window_years
    )
    if vp is None:
        return ToolResult(
            rows=[], evidence_ids=[],
            summary=f"No valuation history for {ticker} (or as_of is not a trading day).",
        )

    def _pct(p):
        return None if p is None else round(float(p) * 100.0, 1)

    row = {
        "ticker": vp.ticker,
        "as_of": vp.as_of.isoformat(),
        "window_from": vp.window_from.isoformat(),
        "window_to": vp.window_to.isoformat(),
        "n_samples": vp.n_samples,
        "current": {
            "pe_ttm": _money(vp.pe_ttm),
            "ps_ttm": _money(vp.ps_ttm),
            "ev_ebitda_ttm": _money(vp.ev_ebitda_ttm),
            "fcf_yield_ttm": _money(vp.fcf_yield_ttm),
        },
        "percentile_in_window_pct": {
            "pe_ttm": _pct(vp.pe_percentile),
            "ps_ttm": _pct(vp.ps_percentile),
            "ev_ebitda_ttm": _pct(vp.ev_ebitda_percentile),
            "fcf_yield_ttm": _pct(vp.fcf_yield_percentile),
        },
        "pe_window_stats": {
            "min": _money(vp.pe_min),
            "median": _money(vp.pe_median),
            "max": _money(vp.pe_max),
        },
    }
    cite = f"M:v_valuation_ratios_ttm:{vp.security_id}:{vp.as_of.isoformat()}"
    pe_pct = _pct(vp.pe_percentile)
    summary = (
        f"{ticker} on {vp.as_of}: P/E {vp.pe_ttm} is "
        + (f"at the {pe_pct}th percentile " if pe_pct is not None else "(percentile n/a) ")
        + f"of its {window_years}-year history "
        f"(min {vp.pe_min}, median {vp.pe_median}, max {vp.pe_max}; "
        f"n={vp.n_samples} historical days)."
    )
    return ToolResult(rows=[row], evidence_ids=[cite], summary=summary)


def _tool_read_quarterly_metrics_series(conn: psycopg.Connection, params: dict[str, Any]) -> ToolResult:
    ticker = params["ticker"]
    n = int(params.get("n") or 8)
    if n < 1 or n > 40:
        return ToolResult(rows=[], evidence_ids=[], summary="n must be 1..40", error="n out of range")
    company = _resolve_company(conn, ticker)
    if company is None:
        return ToolResult(rows=[], evidence_ids=[], summary=f"No company found for {ticker}.")

    rows_raw = get_quarterly_metrics_series(conn, company_id=company.id, n=n)
    if not rows_raw:
        return ToolResult(
            rows=[], evidence_ids=[],
            summary=f"No quarterly metrics for {ticker}.",
        )

    rows = []
    evidence_ids: list[str] = []
    for r in rows_raw:
        cite = f"M:v_metrics_q:{company.id}:{r['fiscal_period_label']}"
        rows.append({
            "fiscal_period": r["fiscal_period_label"],
            "period_end": str(r["period_end"]),
            "revenue": _money(r["revenue"]),
            "gross_margin": _money(r["gross_margin"]),
            "operating_margin": _money(r["operating_margin"]),
            "net_margin": _money(r["net_margin"]),
            "cfo": _money(r["cfo"]),
            "capital_expenditures": _money(r["capital_expenditures"]),
            "fcf": _money(r["fcf"]),
            "dna_cf": _money(r.get("dna_cf")),
            "capex_to_dna_ratio": _money(r.get("capex_to_dna_ratio")),
            "evidence_id": cite,
        })
        evidence_ids.append(cite)
    return ToolResult(
        rows=rows,
        evidence_ids=evidence_ids,
        summary=(
            f"{company.ticker}: last {len(rows)} quarter(s) of metrics, "
            f"{rows[-1]['fiscal_period']}..{rows[0]['fiscal_period']}. "
            f"capex_to_dna_ratio surfaces capex-cycle vs structural-compression: "
            f">1.5x sustained = structural; ~1.0x = replacement-only."
        ),
    )


def _tool_compare_transcript_mentions(conn: psycopg.Connection, params: dict[str, Any]) -> ToolResult:
    ticker = params["ticker"]
    terms = params.get("terms") or []
    periods = int(params.get("periods", 8))
    if not terms:
        return ToolResult(
            rows=[],
            evidence_ids=[],
            summary="terms list is required",
            error="terms must be a non-empty list",
        )
    summaries = compare_transcript_mentions(
        conn,
        ticker,
        terms=list(terms),
        periods=periods,
    )
    if not summaries:
        return ToolResult(
            rows=[],
            evidence_ids=[],
            summary=f"No transcripts found for {ticker} when comparing terms.",
        )
    rows = [
        {
            "fiscal_period": s.fiscal_period_label,
            "period_end": str(s.period_end),
            "term_counts": s.term_counts,
            "total_mentions": s.total_mentions,
        }
        for s in summaries
    ]
    return ToolResult(
        rows=rows,
        evidence_ids=[f"A:{s.artifact_id}" for s in summaries],
        summary=(
            f"{ticker}: {len(summaries)} period(s) compared for terms "
            f"{terms}. Use the time-series shape to identify when "
            f"discussion of these topics rose or fell."
        ),
    )


def _tool_list_companies(conn: psycopg.Connection, params: dict[str, Any]) -> ToolResult:
    companies = list_companies(conn)
    rows = [
        {
            "ticker": c.ticker,
            "company_id": c.id,
            "name": c.name,
            "cik": c.cik,
            "fiscal_year_end": c.fiscal_year_end_md,
        }
        for c in companies
    ]
    return ToolResult(
        rows=rows,
        evidence_ids=[],
        summary=f"{len(companies)} company(ies) in the universe.",
    )


def _format_screen_citation(view_name: str, company_id: int, period_start: str, period_end: str) -> str:
    """Format an M-citation, collapsing single-period windows to a clean form.

    Matches get_metrics conventions when the screen window is one row, so the
    popup lands in the existing single-row code path. Window form is reserved
    for actual multi-row aggregates.
    """
    if view_name == "v_metrics_fy" and period_start == period_end:
        return f"M:v_metrics_fy:{company_id}:FY{period_start}"
    if view_name == "v_metrics_fy":
        return f"M:v_metrics_fy:{company_id}:FY{period_start}_to_FY{period_end}"
    if period_start == period_end:
        return f"M:{view_name}:{company_id}:{period_start}"
    return f"M:{view_name}:{company_id}:{period_start}_to_{period_end}"


def _tool_screen_companies(conn: psycopg.Connection, params: dict[str, Any]) -> ToolResult:
    metric = params["metric"]
    if metric not in supported_metrics():
        return ToolResult(
            rows=[],
            evidence_ids=[],
            summary=f"unsupported metric '{metric}'. Supported: {', '.join(supported_metrics())}.",
            error=f"unsupported metric '{metric}'",
        )
    n_years = int(params.get("n_years", 1))
    limit = int(params.get("limit", 10))
    sort_desc = bool(params.get("sort_desc", True))
    universe_size = count_universe_for_metric(conn, metric=metric)
    screen_rows = screen_companies_by_metric(
        conn,
        metric=metric,
        n_years=n_years,
        limit=limit,
        sort_desc=sort_desc,
    )
    if not screen_rows:
        return ToolResult(
            rows=[],
            evidence_ids=[],
            summary=f"No companies matched the {metric} screen (universe={universe_size}, insufficient coverage?).",
        )
    kind = metric_value_kind(metric)
    rows: list[dict[str, Any]] = []
    evidence_ids: list[str] = []
    for r in screen_rows:
        rows.append(
            {
                "rank": len(rows) + 1,
                "ticker": r.ticker,
                "company_id": r.company_id,
                "value": _money(r.value),
                "value_kind": kind,
                "metric": metric,
                "n_periods": r.n_periods,
                "period_start": r.period_start,
                "period_end": r.period_end,
            }
        )
        evidence_ids.append(
            _format_screen_citation(r.view_name, r.company_id, r.period_start, r.period_end)
        )
    direction = "highest first" if sort_desc else "lowest first"
    label = (
        f"{metric} (single year)"
        if n_years <= 1
        else f"{metric} avg over last {n_years} years"
    )
    summary = (
        f"Top {len(rows)} of {universe_size} companies by {label} ({direction}). "
        f"All {universe_size} ranked; only top {len(rows)} returned."
    )
    return ToolResult(
        rows=rows,
        evidence_ids=evidence_ids,
        summary=summary,
    )


def _tool_read_consensus(conn: psycopg.Connection, params: dict[str, Any]) -> ToolResult:
    ticker = params["ticker"].upper()
    period_kind = params.get("period_kind", "quarter")
    n_forward = int(params.get("n_forward", 4))
    n_past = int(params.get("n_past", 1))
    try:
        rows_data = read_consensus(
            conn, ticker=ticker, period_kind=period_kind,
            n_forward=n_forward, n_past=n_past,
        )
    except ValueError as e:
        return ToolResult(rows=[], evidence_ids=[], summary=str(e), error=str(e))
    if not rows_data:
        return ToolResult(
            rows=[], evidence_ids=[],
            summary=f"No analyst consensus for {ticker} ({period_kind}).",
        )

    warnings = read_estimate_warnings(conn, ticker=ticker)
    # Index by (period_kind, period_end) for per-row attachment.
    warnings_by_period: dict[tuple[str, date], list[EstimateWarning]] = {}
    unscoped_warnings: list[EstimateWarning] = []
    for w in warnings:
        if w.period_kind and w.period_end:
            warnings_by_period.setdefault((w.period_kind, w.period_end), []).append(w)
        else:
            unscoped_warnings.append(w)

    rows: list[dict[str, Any]] = []
    evidence_ids: list[str] = []
    for r in rows_data:
        cite = f"E:{r.security_id}:{r.period_kind}:{r.period_end.isoformat()}"
        row_warnings = warnings_by_period.get((r.period_kind, r.period_end), [])
        rows.append({
            "period_kind": r.period_kind,
            "period_end": r.period_end.isoformat(),
            "is_forward": r.is_forward,
            "eps_avg": _money(r.eps_avg),
            "eps_low": _money(r.eps_low),
            "eps_high": _money(r.eps_high),
            "revenue_avg": _money(r.revenue_avg),
            "revenue_low": _money(r.revenue_low),
            "revenue_high": _money(r.revenue_high),
            "ebitda_avg": _money(r.ebitda_avg),
            "ebit_avg": _money(r.ebit_avg),
            "ebit_low": _money(r.ebit_low),
            "ebit_high": _money(r.ebit_high),
            "net_income_avg": _money(r.net_income_avg),
            "num_analysts_eps": r.num_analysts_eps,
            "num_analysts_revenue": r.num_analysts_revenue,
            "fetched_at": r.fetched_at.isoformat(),
            "evidence_id": cite,
            "warnings": [
                {
                    "finding_id": w.finding_id,
                    "source_check": w.source_check,
                    "severity": w.severity,
                    "summary": w.summary,
                }
                for w in row_warnings
            ],
        })
        evidence_ids.append(cite)
    forward = [r for r in rows_data if r.is_forward]
    warn_count = sum(len(r["warnings"]) for r in rows) + len(unscoped_warnings)
    warn_note = (
        f" {warn_count} steward warning(s) attached — review before relying on EBIT/EBITDA."
        if warn_count else ""
    )
    summary = (
        f"{ticker} {period_kind} consensus: {len(rows_data)} period(s) returned "
        f"({len(forward)} forward). Latest fetched_at "
        f"{rows_data[-1].fetched_at.date()}. Cite individual periods as "
        f"E:{rows_data[0].security_id}:{period_kind}:YYYY-MM-DD.{warn_note}"
    )
    return ToolResult(rows=rows, evidence_ids=evidence_ids, summary=summary)


def _tool_read_target_gap(conn: psycopg.Connection, params: dict[str, Any]) -> ToolResult:
    ticker = params["ticker"].upper()
    as_of = _parse_iso_date(params.get("as_of"))
    gap = read_target_gap(conn, ticker=ticker, as_of=as_of)
    if gap is None:
        return ToolResult(
            rows=[], evidence_ids=[],
            summary=f"No price-target consensus for {ticker}.",
        )
    cite = f"T:{gap.security_id}:{gap.fetched_at.date().isoformat()}"
    row = {
        "ticker": gap.ticker,
        "target_high": _money(gap.target_high),
        "target_low": _money(gap.target_low),
        "target_median": _money(gap.target_median),
        "target_consensus": _money(gap.target_consensus),
        "current_close": _money(gap.current_close),
        "current_close_date": (
            gap.current_close_date.isoformat() if gap.current_close_date else None
        ),
        "upside_to_consensus_pct": (
            None if gap.upside_to_consensus_pct is None
            else round(gap.upside_to_consensus_pct, 2)
        ),
        "fetched_at": gap.fetched_at.isoformat(),
        "evidence_id": cite,
    }
    upside_str = (
        "n/a" if gap.upside_to_consensus_pct is None
        else f"{gap.upside_to_consensus_pct:+.1f}%"
    )
    summary = (
        f"{ticker}: consensus target {gap.target_consensus} "
        f"(low {gap.target_low}, median {gap.target_median}, high {gap.target_high}); "
        f"close {gap.current_close} on {gap.current_close_date} → upside {upside_str}. "
        f"Cite as {cite}."
    )
    return ToolResult(rows=[row], evidence_ids=[cite], summary=summary)


def _tool_read_surprise_history(conn: psycopg.Connection, params: dict[str, Any]) -> ToolResult:
    ticker = params["ticker"].upper()
    n = int(params.get("n", 8))
    rows_data = read_surprise_history(conn, ticker=ticker, n=n)
    if not rows_data:
        return ToolResult(
            rows=[], evidence_ids=[],
            summary=f"No earnings surprise history for {ticker}.",
        )
    rows: list[dict[str, Any]] = []
    evidence_ids: list[str] = []
    for s in rows_data:
        cite = f"S:{s.security_id}:{s.announcement_date.isoformat()}"
        rows.append({
            "announcement_date": s.announcement_date.isoformat(),
            "eps_actual": _money(s.eps_actual),
            "eps_estimated": _money(s.eps_estimated),
            "eps_surprise_pct": (
                None if s.eps_surprise_pct is None else round(s.eps_surprise_pct, 2)
            ),
            "revenue_actual": _money(s.revenue_actual),
            "revenue_estimated": _money(s.revenue_estimated),
            "revenue_surprise_pct": (
                None if s.revenue_surprise_pct is None
                else round(s.revenue_surprise_pct, 2)
            ),
            "evidence_id": cite,
        })
        evidence_ids.append(cite)
    beats = [s for s in rows_data if s.eps_surprise_pct is not None and s.eps_surprise_pct > 0]
    summary = (
        f"{ticker}: {len(rows_data)} announcement(s), "
        f"{rows_data[-1].announcement_date}..{rows_data[0].announcement_date}. "
        f"EPS beats: {len(beats)} of "
        f"{sum(1 for s in rows_data if s.eps_surprise_pct is not None)}. "
        f"Cite individual quarters as S:{rows_data[0].security_id}:YYYY-MM-DD."
    )
    return ToolResult(rows=rows, evidence_ids=evidence_ids, summary=summary)


def _tool_recent_analyst_actions(conn: psycopg.Connection, params: dict[str, Any]) -> ToolResult:
    ticker = params["ticker"].upper()
    days = int(params.get("days", 90))
    limit = int(params.get("limit", 50))
    actions = recent_analyst_actions(conn, ticker=ticker, days=days, limit=limit)
    if not actions:
        return ToolResult(
            rows=[], evidence_ids=[],
            summary=f"No analyst actions for {ticker} in the last {days} days.",
        )
    rows: list[dict[str, Any]] = []
    evidence_ids: list[str] = []
    for a in actions:
        rows.append({
            "kind": a.kind,
            "when": a.when.date().isoformat(),
            "firm": a.firm,
            "analyst_name": a.analyst_name,
            "previous_grade": a.previous_grade,
            "new_grade": a.new_grade,
            "action": a.action,
            "price_target": _money(a.price_target),
            "adj_price_target": _money(a.adj_price_target),
            "price_when_posted": _money(a.price_when_posted),
            "news_title": a.news_title,
            "news_url": a.news_url,
            "evidence_id": a.citation,
        })
        evidence_ids.append(a.citation)
    grades = [a for a in actions if a.kind == "grade"]
    targets = [a for a in actions if a.kind == "price_target"]
    summary = (
        f"{ticker}: {len(actions)} action(s) in last {days}d "
        f"({len(grades)} grade, {len(targets)} price-target). "
        f"Latest: {actions[0].when.date()} ({actions[0].kind}, {actions[0].firm}). "
        f"Cite grade rows as G:<id>, price-target rows as A:<id>."
    )
    return ToolResult(rows=rows, evidence_ids=evidence_ids, summary=summary)


def _tool_screen_companies_by_trajectory(
    conn: psycopg.Connection, params: dict[str, Any]
) -> ToolResult:
    metric = params["metric"]
    if metric not in supported_metrics():
        return ToolResult(
            rows=[], evidence_ids=[],
            summary=f"unsupported metric '{metric}'. Supported: {', '.join(supported_metrics())}.",
            error=f"unsupported metric '{metric}'",
        )
    window_periods = int(params.get("window_periods", 12))
    limit = int(params.get("limit", 10))
    sort_desc = bool(params.get("sort_desc", True))
    basis = params.get("basis", "auto")

    try:
        traj_rows = screen_companies_by_trajectory(
            conn,
            metric=metric,
            window_periods=window_periods,
            limit=limit,
            sort_desc=sort_desc,
            basis=basis,
        )
    except ValueError as e:
        return ToolResult(rows=[], evidence_ids=[], summary=str(e), error=str(e))
    if not traj_rows:
        return ToolResult(
            rows=[], evidence_ids=[],
            summary=f"No companies matched the {metric} trajectory screen (insufficient history?).",
        )
    kind = metric_value_kind(metric)
    rows: list[dict[str, Any]] = []
    evidence_ids: list[str] = []
    for r in traj_rows:
        rows.append({
            "rank": len(rows) + 1,
            "ticker": r.ticker,
            "company_id": r.company_id,
            "metric": metric,
            "value_kind": kind,
            "earliest_value": _money(r.earliest_value),
            "latest_value": _money(r.latest_value),
            "delta": _money(r.delta),
            "relative_change": _money(r.relative_change),
            "earliest_period": r.earliest_period,
            "latest_period": r.latest_period,
            "n_periods": r.n_periods,
        })
        evidence_ids.append(
            _format_screen_citation(r.view_name, r.company_id, r.earliest_period, r.latest_period)
        )
    direction = "fastest improving first" if sort_desc else "fastest declining first"
    summary = (
        f"Top {len(rows)} companies by {metric} trajectory over the last "
        f"{window_periods} periods ({direction}, basis={basis}). "
        f"earliest_value = avg of first 1/3 of window; latest_value = avg of "
        f"last 1/3. delta = latest - earliest. relative_change = delta / |earliest|."
    )
    return ToolResult(rows=rows, evidence_ids=evidence_ids, summary=summary)


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
    Tool(
        name="read_transcript",
        description=(
            "Read the FULL transcript for one (ticker, fiscal_period) call in "
            "speaker-turn order. Use this when the question wants synthesis "
            "across an entire call (commentary tone, narrative arc, what was "
            "emphasized, how guidance was framed). Returns every turn with "
            "[T:chunk_id] citations. For annual questions (no fiscal_quarter), "
            "this returns the FY-end (Q4) call where annual guidance is set."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "fiscal_year": {"type": "integer"},
                "fiscal_quarter": {"type": "integer", "description": "Optional 1-4. Omit to get the Q4 / annual call."},
            },
            "required": ["ticker", "fiscal_year"],
        },
        execute=_tool_read_transcript,
    ),
    Tool(
        name="compare_transcript_mentions",
        description=(
            "Count how often a list of terms appears across the most recent N "
            "transcripts for one ticker, returning a per-period mention "
            "time-series. Use this for 'how did discussion of X evolve over "
            "time' questions. Pick FEW (1-4) high-signal terms — terms are "
            "OR'd, not AND'd."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "terms": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Short keywords to count, e.g. ['efficiency', 'headcount', 'AI'].",
                },
                "periods": {"type": "integer", "description": "How many recent calls to compare (default 8)."},
            },
            "required": ["ticker", "terms"],
        },
        execute=_tool_compare_transcript_mentions,
    ),
    Tool(
        name="list_companies",
        description=(
            "List every company in the database (ticker, name, fiscal year end). "
            "Use when a question asks 'which companies', 'what tickers', or "
            "needs the universe to scope a follow-up tool call. Cheap."
        ),
        input_schema={"type": "object", "properties": {}},
        execute=_tool_list_companies,
    ),
    Tool(
        name="read_prices",
        description=(
            "Daily OHLCV bars for a security across [from_date, to_date]. "
            "Returns close (raw as-traded) and adj_close (split + dividend "
            "adjusted, total-return basis). Use adj_close for return math; "
            "use close when describing 'what the ticker said that day'. "
            "Caps at 400 rows — narrow the window if you need finer detail. "
            "Works for both common stock (NVDA, AMD, ...) and benchmark ETFs "
            "(SPY, QQQ)."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "from_date": {"type": "string", "description": "YYYY-MM-DD inclusive."},
                "to_date":   {"type": "string", "description": "YYYY-MM-DD inclusive."},
            },
            "required": ["ticker", "from_date", "to_date"],
        },
        execute=_tool_read_prices,
    ),
    Tool(
        name="read_valuation_series",
        description=(
            "Sampled valuation series (P/E, P/S, EV/EBITDA, FCF yield, market cap) "
            "for one ticker across a date range. Use this for trend questions: "
            "'is NVDA's P/E unusually high vs its own history?', 'how has AMZN's "
            "FCF yield trended?'. Default sample is monthly (manageable row "
            "count); use quarterly for multi-decade windows. Returns one M:cite "
            "per sample point."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "from_date": {"type": "string", "description": "YYYY-MM-DD inclusive."},
                "to_date":   {"type": "string", "description": "YYYY-MM-DD inclusive."},
                "sample": {
                    "type": "string",
                    "enum": ["daily", "monthly", "quarterly", "yearly"],
                    "description": "Sampling grain. Default 'monthly'. Use 'daily' only for short windows (caps at 400 rows).",
                },
            },
            "required": ["ticker", "from_date", "to_date"],
        },
        execute=_tool_read_valuation_series,
    ),
    Tool(
        name="valuation_percentile",
        description=(
            "Where one ticker's CURRENT valuation sits in its own historical "
            "distribution. Returns current P/E/P/S/EV-EBITDA/FCF-yield + each "
            "ratio's percentile rank within the trailing N-year window (default 5y). "
            "Pe_percentile=85 means today's P/E is higher than 85% of historical "
            "daily values — i.e. expensive vs its own history. **This is the "
            "right tool for 'is X overvalued vs its own history' questions** — "
            "one call beats dozens of read_valuation calls."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "as_of": {"type": "string", "description": "YYYY-MM-DD trading day. Omit for latest available."},
                "window_years": {"type": "integer", "description": "Trailing window. Default 5."},
            },
            "required": ["ticker"],
        },
        execute=_tool_valuation_percentile,
    ),
    Tool(
        name="read_quarterly_metrics_series",
        description=(
            "Last N quarters of metrics for one ticker: revenue, margins, "
            "CFO, CapEx, FCF, **D&A** (dna_cf), and the **capex_to_dna_ratio**. "
            "Use for trajectory questions ('is margin accelerating?', "
            "'8-quarter FCF trend') AND for capex-cycle vs structural-cash-"
            "compression analysis: capex_to_dna_ratio sustained >1.5x = "
            "structural compression; ~1.0x = replacement-only. One call "
            "replaces 8+ get_metrics / get_financial_facts calls. Default N=8."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "n": {"type": "integer", "description": "How many recent quarters. Default 8, max 40."},
            },
            "required": ["ticker"],
        },
        execute=_tool_read_quarterly_metrics_series,
    ),
    Tool(
        name="read_valuation",
        description=(
            "Valuation ratios (P/E, P/S, EV/EBITDA, FCF yield) + EV + "
            "underlying TTM components for one (ticker, date). Uses "
            "POINT-IN-TIME TTM — financials KNOWN on the date, not "
            "recomputed with hindsight. Public sources (stockanalysis.com, "
            "etc.) typically apply hindsight TTM; expect divergence in the "
            "~30 days between fiscal period end and filing publication. "
            "Omit as_of for the latest available trading day. Common stock "
            "only (no ETF valuation). Cite as M:v_valuation_ratios_ttm:..."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "as_of": {
                    "type": "string",
                    "description": "YYYY-MM-DD trading day. Omit for latest available.",
                },
            },
            "required": ["ticker"],
        },
        execute=_tool_read_valuation,
    ),
    Tool(
        name="screen_companies",
        description=(
            "Rank companies across the universe by a metric, optionally averaged "
            "over the most recent N years. Use this for ANY question of the form "
            "'highest/lowest X', 'top N by X', 'rank by X'. Do not iterate "
            "get_metrics across tickers — call screen_companies once instead. "
            "Returns ranked rows with [M:view:co:window] citation IDs."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "metric": {
                    "type": "string",
                    "enum": ["revenue", "gross_margin", "operating_margin", "net_margin", "fcf", "roic"],
                    "description": "Metric to rank by. roic is averaged over the recent N*4 quarterly rows.",
                },
                "n_years": {
                    "type": "integer",
                    "description": "Lookback window in years. 1 = single most recent year; 3 or 5 = rolling average. Default 1.",
                },
                "limit": {"type": "integer", "description": "Top N to return. Default 10, max 50."},
                "sort_desc": {"type": "boolean", "description": "True for highest-first (default), false for lowest-first."},
            },
            "required": ["metric"],
        },
        execute=_tool_screen_companies,
    ),
    Tool(
        name="screen_companies_by_trajectory",
        description=(
            "Rank companies by the *change* in a metric over a multi-period "
            "window — answers 'fastest-improving X' / 'fastest-declining X' / "
            "'who's accelerating?' / 'who's decelerating?'. Distinct from "
            "screen_companies which ranks by AVERAGE LEVEL. Computes "
            "earliest_value (avg of first 1/3 of window) vs latest_value "
            "(avg of last 1/3); delta and relative_change are returned. "
            "Default window_periods=12 (3 years for quarterly metrics like "
            "ROIC). For ratios (ROIC, margins) ranking defaults to absolute "
            "delta (percentage points); for money metrics (revenue, fcf) it "
            "defaults to relative change. **Use this tool, not screen_companies, "
            "for any 'fastest growing/improving/declining' question.**"
        ),
        input_schema={
            "type": "object",
            "properties": {
                "metric": {
                    "type": "string",
                    "enum": ["revenue", "gross_margin", "operating_margin", "net_margin", "fcf", "roic"],
                },
                "window_periods": {
                    "type": "integer",
                    "description": "Periods (quarters for ROIC, years for FY metrics) in the window. Default 12.",
                },
                "limit": {"type": "integer", "description": "Top N. Default 10, max 50."},
                "sort_desc": {"type": "boolean", "description": "True = fastest-improving first (default). False = fastest-declining first."},
                "basis": {
                    "type": "string",
                    "enum": ["auto", "absolute", "relative"],
                    "description": "Rank order. 'auto' (default) = absolute pp for ratios, relative % for money. 'absolute' = always rank by delta. 'relative' = always rank by relative_change.",
                },
            },
            "required": ["metric"],
        },
        execute=_tool_screen_companies_by_trajectory,
    ),
    Tool(
        name="read_consensus",
        description=(
            "Forward + most-recent past analyst consensus per fiscal period for "
            "one ticker. Returns revenue / EPS / EBITDA / EBIT (operating income) "
            "/ net-income low / avg / high + analyst counts per (period_kind, "
            "period_end). Each row also carries a `warnings` array with any open "
            "steward findings for that period (`forward_estimate_consistency`, "
            "`earnings_surprise_sanity`) — when present, treat the flagged metric "
            "(typically EBIT/EBITDA) as unreliable and surface the warning in the "
            "answer rather than reasoning around it silently. Use for 'what does "
            "the street expect for X next quarter / next FY?' questions, or as "
            "substrate for forward valuation. Default: 4 forward + 1 past "
            "QUARTERLY periods. Set period_kind='annual' for FY-grain. Cite "
            "individual periods as E:<security_id>:<period_kind>:<period_end>."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "period_kind": {
                    "type": "string", "enum": ["annual", "quarter"],
                    "description": "Default 'quarter'.",
                },
                "n_forward": {"type": "integer", "description": "Forward periods to return. Default 4."},
                "n_past": {"type": "integer", "description": "Most-recent past periods to return. Default 1."},
            },
            "required": ["ticker"],
        },
        execute=_tool_read_consensus,
    ),
    Tool(
        name="read_target_gap",
        description=(
            "Current price vs analyst consensus price target for one ticker. "
            "Returns target high / low / median / consensus, latest close, "
            "current_close_date, and upside_to_consensus_pct (= (consensus - "
            "close) / close × 100). Positive upside = analysts expect the "
            "stock to rise. Single snapshot — no analyst count exposed by FMP "
            "on this endpoint. Cite as T:<security_id>:<fetched_at_date>."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "as_of": {
                    "type": "string",
                    "description": "YYYY-MM-DD trading day for the close. Omit for latest available.",
                },
            },
            "required": ["ticker"],
        },
        execute=_tool_read_target_gap,
    ),
    Tool(
        name="read_surprise_history",
        description=(
            "Last N quarterly earnings announcements for one ticker: actual vs "
            "estimated EPS and revenue, with surprise percentages. Newest first. "
            "Use for beat / miss questions ('how often does META beat?', 'did "
            "Q3 surprise?'). Default N=8. Filters out upcoming announcements "
            "(actuals null). Cite individual quarters as "
            "S:<security_id>:<announcement_date>."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "n": {"type": "integer", "description": "How many recent quarters. Default 8."},
            },
            "required": ["ticker"],
        },
        execute=_tool_read_surprise_history,
    ),
    Tool(
        name="recent_analyst_actions",
        description=(
            "Combined event log of analyst grade changes (upgrade / downgrade / "
            "maintain) and price-target updates for one ticker, newest first. "
            "Use for sentiment / news-flow questions ('has anyone moved on TSLA "
            "this week?', 'who's been upgrading NVDA?'). Default window 90 days. "
            "Each row carries firm, analyst name (where known), and news source "
            "URL. Cite grade rows as G:<id>, price-target rows as A:<id>."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "days": {"type": "integer", "description": "Lookback window in days. Default 90."},
                "limit": {"type": "integer", "description": "Max rows. Default 50."},
            },
            "required": ["ticker"],
        },
        execute=_tool_recent_analyst_actions,
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
    rows: list[dict[str, Any]] = field(default_factory=list)


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
    verifier_warnings: list[str] = field(default_factory=list)
    error: str | None = None
    thread_id: str | None = None
    turn_index: int | None = None

    def write(self, out_dir: Path | None = None) -> Path:
        out_dir = out_dir or Path("outputs/qa_runs/agent")
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"{datetime.now(UTC).date().isoformat()}.jsonl"
        payload = jsonable(self)
        line = json.dumps(payload, sort_keys=True, default=str) + "\n"
        with path.open("a", encoding="utf-8") as f:
            f.write(line)
        if self.thread_id:
            thread_dir = out_dir / "threads"
            thread_dir.mkdir(parents=True, exist_ok=True)
            thread_path = thread_dir / f"{self.thread_id}.jsonl"
            with thread_path.open("a", encoding="utf-8") as f:
                f.write(line)
        return path


# --------------------------------------------------------------------------- #
# Multi-turn / thread storage
# --------------------------------------------------------------------------- #

# Cap how many prior turns we replay. Each turn is ~500-2000 tokens of Q+A
# prose; 5 keeps the context manageable while supporting natural follow-ups
# ("what about Q3", "compare to last year", "explain that more").
MAX_PRIOR_TURNS = 5


@dataclass
class PriorTurn:
    question: str
    answer: str
    started_at: str


def _thread_path(thread_id: str, out_dir: Path | None = None) -> Path:
    base = out_dir or Path("outputs/qa_runs/agent")
    return base / "threads" / f"{thread_id}.jsonl"


def load_thread(thread_id: str, out_dir: Path | None = None) -> list[PriorTurn]:
    """Read a thread's prior turns in chronological order.

    Returns only completed, answered turns (skips error rows and unanswered
    runs). Caller is responsible for capping length before replaying.
    """
    path = _thread_path(thread_id, out_dir)
    if not path.exists():
        return []
    turns: list[PriorTurn] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            answer = row.get("answer")
            question = row.get("question")
            if not answer or not question:
                continue
            turns.append(
                PriorTurn(
                    question=question,
                    answer=answer,
                    started_at=row.get("started_at") or "",
                )
            )
    turns.sort(key=lambda t: t.started_at)
    return turns


def _prior_messages(prior_turns: list[PriorTurn]) -> list[dict[str, Any]]:
    """Render prior turns as Anthropic-format chat messages."""
    out: list[dict[str, Any]] = []
    for t in prior_turns[-MAX_PRIOR_TURNS:]:
        out.append({"role": "user", "content": t.question})
        out.append({"role": "assistant", "content": t.answer})
    return out


# --------------------------------------------------------------------------- #
# Agent loop
# --------------------------------------------------------------------------- #


_PLANNER_SYSTEM_TEMPLATE = """You are the routing model for an analyst data system over US public-company financials, transcripts, and SEC filings.

Your job: read the user's question, call the available tools to gather grounded evidence, then signal you are done. Do NOT write the final answer — a separate model handles synthesis.

Today's date: {today}

Time references — resolve relative phrases against today's date:
- 'last N years' / 'recent' / 'latest' / 'current' / 'this year' (no explicit year): use the N most recent COMPLETED fiscal years for that ticker.
- Companies have different fiscal calendars. Most US filers' FY ends in December. Notable exceptions:
    NVDA fiscal year ends late January (FY2026 ended Jan 2026)
    DELL late January / early February
    AAPL late September
    MSFT late June
    ORCL late May
- If unsure what the latest available FY is for a ticker, fetch get_metrics for the year that matches today's date AND the prior year — both succeeding tells you the latest available; the more-recent failing tells you to step back.
- For "last 3 years" with today = 2026: NVDA -> FY2024/FY2025/FY2026; AAPL -> FY2023/FY2024/FY2025 (FY2026 not yet reported until late October); a December filer -> FY2023/FY2024/FY2025.

Tool-selection playbook:
- Headline numbers (revenue, margins, FCF, ROIC for ONE company-period): get_metrics.
- Multiple periods of one company: call get_metrics once per year. For 5-year windows, fetch all 5.
- Cross-company ranking ('highest/lowest X', 'top N by X', 'rank by X'): call screen_companies ONCE — do NOT iterate get_metrics, do NOT call list_companies first.
- Universe discovery ('what tickers do we have'): list_companies. Otherwise skip it.
- "What did management say about X in period P": search_transcripts with a SHORT FTS query.
- "How did the discussion of X evolve over time" — counting/term-frequency framing only: compare_transcript_mentions with 1-4 high-signal terms.
- Synthesis-style reading ("read the call and tell me", "what was the tone", "narrative arc", "what was emphasized"): read_transcript for the full call. Prefer this over chained search_transcripts when the question wants holistic reading.
- Multi-quarter NARRATIVE comparison ("compare commentary across the last N quarters", "what's consistent / different / strengthened / weakened", "how has management's view changed", "characterize the shift"): call read_transcript for EACH of the N quarters. compare_transcript_mentions alone gives only term-count tables, which are too thin for a narrative-comparison answer — a synthesizer cannot characterize tone, framing, conviction, or emphasis from frequency counts. The 8-iteration cap accommodates 5-6 read_transcript calls plus 1-2 supporting calls (resolve_company, get_metrics for context).
- 10-K / 10-Q section text: read_filing_sections (currently kind='mda').
- Forward consensus / analyst estimates ('what does the street expect', 'forward revenue / EBITDA / EBIT / operating income / net income / EPS', 'next quarter', 'next FY', 'why are forward estimates X', 'are the forward numbers reliable', 'consensus forecast'): read_consensus. Defaults to QUARTERLY; pass period_kind='annual' for FY-grain. Each row carries a `warnings` array — when populated, the period's EBIT/EBITDA estimate is flagged as unreliable by the steward; surface that in the answer rather than reasoning around it.
- Price target vs current price ('upside to consensus target', 'how much room', 'analyst target', 'PT upside'): read_target_gap.
- Earnings beat / miss history ('does X usually beat?', 'how big a surprise last quarter', 'EPS / revenue surprise pattern'): read_surprise_history.
- Recent analyst actions ('who upgraded / downgraded', 'recent PT changes', 'analyst sentiment shift this week / month'): recent_analyst_actions.
- Always resolve tickers via resolve_company FIRST when the ticker is named.
- Tickers may appear lowercase or mixed-case in user input ('lite', 'nvda', 'Aapl') — recognize them as tickers and uppercase them when calling tools. Common short tickers that look like English words ('lite', 'meta', 'nice', 'alle', 'rare') are still tickers — resolve them rather than dismissing the question.

FTS query hints (for search_transcripts):
- Use 1-3 keywords. Postgres FTS treats them as required terms — long phrases like "guidance outlook expectations revenue growth margin forecast" return ZERO rows.
- Prefer concrete nouns over hedging adjectives: "data center demand" beats "strong sustained accelerating demand".
- If a search returns 0 rows, RETRY with fewer or different terms. Do not give up after one failed search.
- For meta questions about commentary evolution, use compare_transcript_mentions instead of repeated search_transcripts calls.

General:
- Stop calling tools when you have enough evidence. When done, produce a brief plain-text note like 'Evidence gathered.' — synthesis happens elsewhere.
- Do NOT fabricate evidence; if a tool returns no rows, try a different query or accept the gap.
- Hard cap: 8 tool iterations.
"""


def _planner_system() -> str:
    return _PLANNER_SYSTEM_TEMPLATE.format(today=datetime.now(UTC).date().isoformat())


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
    *,
    prior_turns: list[PriorTurn] | None = None,
) -> tuple[list[ToolExecution], list[dict[str, Any]]]:
    """Run the Haiku tool-use loop. Returns the executions and the final
    Anthropic message history (so the synthesizer can replay the evidence)."""
    tool_specs = [t.anthropic_spec() for t in REGISTRY]
    messages: list[dict[str, Any]] = list(_prior_messages(prior_turns or []))
    messages.append({"role": "user", "content": question})
    executions: list[ToolExecution] = []

    for _ in range(MAX_TOOL_ITERATIONS):
        started = time.perf_counter()
        response = client.messages.create(
            model=HAIKU_MODEL,
            max_tokens=MAX_OUTPUT_TOKENS_HAIKU,
            system=_planner_system(),
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
                    rows=result.rows,
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


# --------------------------------------------------------------------------- #
# Soft verification — numeric and quoted-text checks beyond citation-existence.
# These produce warnings that surface in the trace but do NOT flip the
# verified/unverified status. Use them to spot misread numbers or invented
# quotes; the citation verifier remains the load-bearing invariant.
# --------------------------------------------------------------------------- #


_DOLLAR_RE = re.compile(
    r"\$\s*(-?\d{1,3}(?:,\d{3})*(?:\.\d+)?|\-?\d+(?:\.\d+)?)\s*([BMK]|billion|million|thousand)?",
    re.IGNORECASE,
)
_PERCENT_RE = re.compile(r"(-?\d+(?:\.\d+)?)\s*%")
_QUOTE_RE = re.compile(r'"([^"]{4,400})"')


def _to_canonical_dollars(text_match: re.Match) -> float | None:
    raw = text_match.group(1).replace(",", "")
    try:
        n = float(raw)
    except ValueError:
        return None
    suffix = (text_match.group(2) or "").lower()
    if suffix in ("b", "billion"):
        n *= 1_000_000_000
    elif suffix in ("m", "million"):
        n *= 1_000_000
    elif suffix in ("k", "thousand"):
        n *= 1_000
    return n


def _candidate_evidence_numbers(execution: ToolExecution) -> list[float]:
    """Pull every numeric value from a tool's returned rows, canonicalized."""
    out: list[float] = []
    for row in execution.rows or []:
        for v in row.values():
            if v is None or isinstance(v, bool):
                continue
            if isinstance(v, (int, float)):
                out.append(float(v))
            elif isinstance(v, str):
                try:
                    out.append(float(v.replace(",", "")))
                except ValueError:
                    pass
    return out


def _execution_for_evidence_id(executions: list[ToolExecution], evidence_id: str) -> ToolExecution | None:
    for execution in executions:
        if evidence_id in execution.evidence_ids:
            return execution
    return None


def _close_enough(a: float, b: float, *, rel_tol: float = 0.05, abs_tol: float = 0.5) -> bool:
    """Numeric match with rounding tolerance.

    rel_tol of 5% absorbs B/M rounding (47525 -> 47.5 is 0.05% off; 13.6% vs
    13.5% growth is well inside). abs_tol catches near-zero values like
    margins where relative tolerance is meaningless.
    """
    if a == b:
        return True
    if abs(a - b) <= abs_tol:
        return True
    bigger = max(abs(a), abs(b))
    if bigger == 0:
        return False
    return abs(a - b) / bigger <= rel_tol


def _check_numeric_claims(answer: str, executions: list[ToolExecution]) -> list[str]:
    """For each F:/M: citation, find the nearest dollar amount before it in
    prose and check it matches at least one number from the cited evidence
    (within rounding tolerance). Returns warning strings; does not flip status.
    """
    warnings: list[str] = []
    seen_pairs: set[tuple[str, str]] = set()

    for cite_match in CITATION_RE.finditer(answer):
        kind = cite_match.group(1)
        body = cite_match.group(2)
        if kind not in ("F", "M"):
            continue
        for cite in _split_bracket_body(kind, body):
            execution = _execution_for_evidence_id(executions, cite)
            if execution is None or not execution.rows:
                continue
            evidence_numbers = _candidate_evidence_numbers(execution)
            if not evidence_numbers:
                continue

            window_start = max(0, cite_match.start() - 160)
            window = answer[window_start:cite_match.start()]
            dollar_matches = list(_DOLLAR_RE.finditer(window))
            percent_matches = list(_PERCENT_RE.finditer(window))

            if dollar_matches:
                last_dollar = dollar_matches[-1]
                value = _to_canonical_dollars(last_dollar)
                if value is not None:
                    raw_token = last_dollar.group(0).strip()
                    pair_key = (raw_token, cite)
                    if pair_key not in seen_pairs:
                        seen_pairs.add(pair_key)
                        if not any(_close_enough(value, ev) for ev in evidence_numbers):
                            warnings.append(
                                f"Number {raw_token} near [{cite}] is not within tolerance of any "
                                f"value from the cited evidence."
                            )

            # Percent checks only fire for M: citations because F: rows store
            # raw money values, not the YoY percentages Sonnet derives from
            # them. M: rows often include margin fields directly.
            if kind == "M" and percent_matches:
                last_pct = percent_matches[-1]
                try:
                    pct_value = float(last_pct.group(1)) / 100.0
                except ValueError:
                    continue
                raw_token = last_pct.group(0).strip()
                pair_key = (raw_token, cite)
                if pair_key in seen_pairs:
                    continue
                seen_pairs.add(pair_key)
                if not any(_close_enough(pct_value, ev, rel_tol=0.05, abs_tol=0.005) for ev in evidence_numbers):
                    warnings.append(
                        f"Percent {raw_token} near [{cite}] does not match any margin/ratio "
                        f"in the cited evidence row."
                    )
    return warnings


def _check_quoted_text(answer: str, executions: list[ToolExecution]) -> list[str]:
    """For each quoted phrase in the answer, check the nearest T:/S: citation's
    chunk text actually contains it (case-insensitive, whitespace-tolerant).
    """
    warnings: list[str] = []

    citations = list(CITATION_RE.finditer(answer))
    if not citations:
        return warnings

    for quote_match in _QUOTE_RE.finditer(answer):
        quote = quote_match.group(1).strip()
        if len(quote) < 6:
            continue
        # Find the nearest citation whose start is at or after the quote's end.
        nearest: re.Match | None = None
        for cite in citations:
            if cite.start() >= quote_match.end():
                nearest = cite
                break
        if nearest is None:
            continue
        kind = nearest.group(1)
        body = nearest.group(2)
        if kind not in ("T", "S"):
            continue
        for cite_id in _split_bracket_body(kind, body):
            execution = _execution_for_evidence_id(executions, cite_id)
            if execution is None or not execution.rows:
                continue
            normalized_quote = _normalize(quote)
            found = False
            for row in execution.rows:
                text = row.get("text") or ""
                if normalized_quote in _normalize(text):
                    found = True
                    break
            if not found:
                snippet = quote if len(quote) <= 80 else quote[:77] + "..."
                warnings.append(
                    f'Quoted phrase "{snippet}" attributed to [{cite_id}] '
                    f"was not found in that chunk's text."
                )
    return warnings


def _normalize(text: str) -> str:
    return " ".join(text.lower().split())


def ask(
    question: str,
    *,
    conn: psycopg.Connection | None = None,
    client: anthropic.Anthropic | None = None,
    out_dir: Path | None = None,
    thread_id: str | None = None,
    synthesizer: str | None = None,
) -> AgentTrace:
    """Answer one question and return the persisted trace.

    ``synthesizer`` selects the answer-writing model: ``"sonnet"`` (default,
    fast/cheap) or ``"opus"`` (considered answers). Router stays Haiku.
    """
    synth_model, synth_max_tokens = _resolve_synthesizer(synthesizer)
    load_dotenv(override=True)
    if not os.environ.get("ANTHROPIC_API_KEY"):
        raise RuntimeError("ANTHROPIC_API_KEY is not set in the environment.")

    from arrow.db.connection import get_conn

    prior_turns = load_thread(thread_id, out_dir) if thread_id else []
    trace = AgentTrace(
        trace_id=str(uuid.uuid4()),
        question=question,
        started_at=datetime.now(UTC).isoformat(),
        thread_id=thread_id,
        turn_index=len(prior_turns) if thread_id else None,
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
        executions, planner_messages = _run_planner_loop(
            client, conn, question, trace, prior_turns=prior_turns
        )
        trace.tool_executions = executions

        synth_prompt, allowed_ids = _build_synthesis_prompt(question, executions, planner_messages)
        synth_messages = list(_prior_messages(prior_turns))
        synth_messages.append({"role": "user", "content": synth_prompt})
        synth_started = time.perf_counter()
        synth_response = client.messages.create(
            model=synth_model,
            max_tokens=synth_max_tokens,
            system=_SYNTH_SYSTEM,
            messages=synth_messages,
        )
        trace.model_calls.append(
            _model_call_metrics(
                synth_response, synth_model, "synthesizer", (time.perf_counter() - synth_started) * 1000
            )
        )

        answer_parts = [b.text for b in synth_response.content if getattr(b, "type", None) == "text"]
        answer = "\n".join(answer_parts).strip()
        trace.answer = answer
        trace.citations = _extract_citations(answer)
        trace.verifier_status, trace.verifier_issues = _verify_citations(answer, allowed_ids)
        trace.verifier_warnings = _check_numeric_claims(answer, executions) + _check_quoted_text(answer, executions)
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
    thread_id: str | None = None,
    synthesizer: str | None = None,
) -> AsyncIterator[dict[str, Any]]:
    """Async generator: run the agent and yield per-step events.

    ``synthesizer`` selects the answer-writing model: ``"sonnet"`` (default,
    fast/cheap) or ``"opus"`` (considered answers). Router stays Haiku.
    """
    synth_model, synth_max_tokens = _resolve_synthesizer(synthesizer)
    load_dotenv(override=True)
    if not os.environ.get("ANTHROPIC_API_KEY"):
        yield {"event": "error", "message": "ANTHROPIC_API_KEY is not set in the environment."}
        return

    from arrow.db.connection import get_conn

    prior_turns = load_thread(thread_id, out_dir) if thread_id else []
    trace = AgentTrace(
        trace_id=str(uuid.uuid4()),
        question=question,
        started_at=datetime.now(UTC).isoformat(),
        thread_id=thread_id,
        turn_index=len(prior_turns) if thread_id else None,
    )
    started_total = time.perf_counter()
    yield {
        "event": "started",
        "trace_id": trace.trace_id,
        "question": question,
        "thread_id": thread_id,
        "turn_index": trace.turn_index,
        "prior_turns": len(prior_turns),
    }

    client = anthropic.Anthropic()
    conn_cm = get_conn()
    conn = await asyncio.to_thread(conn_cm.__enter__)
    tool_specs = [t.anthropic_spec() for t in REGISTRY]
    messages: list[dict[str, Any]] = list(_prior_messages(prior_turns))
    messages.append({"role": "user", "content": question})
    executions: list[ToolExecution] = []
    allowed_ids: list[str] = []

    try:
        for iteration in range(MAX_TOOL_ITERATIONS):
            turn_started = time.perf_counter()
            response = await asyncio.to_thread(
                client.messages.create,
                model=HAIKU_MODEL,
                max_tokens=MAX_OUTPUT_TOKENS_HAIKU,
                system=_planner_system(),
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
        synth_messages = list(_prior_messages(prior_turns))
        synth_messages.append({"role": "user", "content": synth_prompt})
        yield {"event": "synthesizing"}

        synth_started = time.perf_counter()
        synth_response = await asyncio.to_thread(
            client.messages.create,
            model=synth_model,
            max_tokens=synth_max_tokens,
            system=_SYNTH_SYSTEM,
            messages=synth_messages,
        )
        synth_ms = (time.perf_counter() - synth_started) * 1000
        trace.model_calls.append(
            _model_call_metrics(synth_response, synth_model, "synthesizer", synth_ms)
        )

        answer_parts = [b.text for b in synth_response.content if getattr(b, "type", None) == "text"]
        answer = "\n".join(answer_parts).strip()
        trace.answer = answer
        trace.citations = _extract_citations(answer)
        trace.verifier_status, trace.verifier_issues = _verify_citations(answer, allowed_ids)
        trace.verifier_warnings = _check_numeric_claims(answer, executions) + _check_quoted_text(answer, executions)
        _mark_cited_executions(executions, trace.citations)

        yield {"event": "answer", "text": answer, "citations": trace.citations}
        yield {
            "event": "verifier",
            "status": trace.verifier_status,
            "issues": trace.verifier_issues,
            "warnings": trace.verifier_warnings,
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
