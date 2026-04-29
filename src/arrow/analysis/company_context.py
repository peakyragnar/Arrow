"""Deterministic analyst runtime slice for company-period questions."""

from __future__ import annotations

import json
import re
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg

from arrow.retrieval._query import jsonable as _jsonable
from arrow.retrieval._query import record_action
from arrow.retrieval.companies import get_company, resolve_company_by_ticker
from arrow.retrieval.documents import (
    get_section_chunks,
    get_text_unit_chunks,
    list_documents,
)
from arrow.retrieval.facts import (
    get_financial_facts,
    get_segment_facts,
    get_segment_value_index,
)
from arrow.retrieval.metrics import get_metrics, metrics_view_name
from arrow.retrieval.transcripts import TranscriptTurn, search_transcript_turns
from arrow.retrieval.types import (
    ArtifactPeriod,
    Company,
    EvidenceChunk,
    FinancialFact,
    FiscalMetric,
    Intent,
    RuntimeTrace,
    SegmentFact,
)


MDA_SECTION_KEYS = ("item_7_mda", "part1_item2_mda")
TRANSCRIPT_REVENUE_QUERY = (
    '"revenue growth" OR "year over year" OR "driven by" OR demand '
    'OR customers OR commercial OR government OR "data center"'
)
REVENUE_SIGNAL_WEIGHTS = (
    (r"\bgrowth was driven\b|\bgrowth .* driven by\b|\bdriven by\b", 12),
    (r"\bprimarily due to\b|\battributable to\b", 7),
    (r"\bincreased \d+(?:\.\d+)?% year over year\b", 8),
    (r"\brevenue\b", 4),
    (r"\bgrowth\b|\bgrew\b|\bincrease[sd]?\b", 3),
    (r"\bcustomer[s]?\b", 3),
    (r"\bcommercial\b", 4),
    (r"\bgovernment\b", 4),
    (r"\bu\.s\.\b|\bunited states\b", 3),
    (r"\bdata center\b", 4),
    (r"\bdemand\b", 3),
    (r"\bsegment\b", 2),
    (r"\bfull[- ]year\b|\bfiscal year\b", 2),
)
BOILERPLATE_PENALTIES = (
    (r"\bforward-looking statements?\b", 8),
    (r"\brisks and uncertainties\b", 5),
    (r"\bannual report on form 10-k\b", 4),
    (r"\bquarterly report on form 10-q\b", 4),
    (r"\bshould be read in conjunction\b", 4),
    (r"\bintended to help the reader\b|\bprovided as a supplement\b", 10),
    (r"\btable of contents\b", 4),
    (r"\bsafe harbor\b", 5),
    (r"\bnon-gaap\b|\babout non-gaap\b|\bconstant currency\b", 10),
    (r"\breconciliation table\b|\breconciliation of\b", 5),
    (r"\bfinancial measure\b", 3),
    (r"\bcosts? of revenues?\b|\boperating expenses?\b", 14),
    (r"\bprovision for income taxes\b|\beffective tax rate\b", 9),
    (r"\bchanges in assets and liabilities\b|\baccounts receivable\b|\bincome taxes\b", 9),
    (r"\bliquidity and capital resources\b|\bcash flow information\b", 12),
    (r"\bcapital expenditures\b|\bcritical accounting (?:estimates|policies)\b", 25),
    (r"\bcontract liabilities\b|\bgeopolitical tensions\b", 18),
    (r"\bresearch and development expenses\b|\bexpenses include\b", 9),
    (r"\beconomic conditions, challenges, and risks\b|\badversely affect\b", 25),
    (r"\bstock-based compensation\b", 7),
    (r"\bunfavorably affected\b", 10),
)
WEAK_EVIDENCE_SCORE = 12


@dataclass(frozen=True)
class ReadinessResult:
    status: str
    checks: list[str]
    gaps: list[str]


@dataclass(frozen=True)
class Provenance:
    fact_ids: list[int]
    chunk_ids: list[int]
    artifact_ids: list[int]
    view_names: list[str]
    planned_actions: list[str]


@dataclass(frozen=True)
class RevenueDriverPacket:
    intent: Intent
    company: Company | None
    current_metrics: FiscalMetric | None
    prior_metrics: FiscalMetric | None
    current_facts: list[FinancialFact]
    prior_facts: list[FinancialFact]
    segment_facts: list[SegmentFact]
    artifact_periods: list[ArtifactPeriod]
    mda_chunks: list[EvidenceChunk]
    earnings_chunks: list[EvidenceChunk]
    transcript_turns: list[TranscriptTurn]
    readiness: ReadinessResult
    gaps: list[str]
    provenance: Provenance
    trace_summary: dict[str, Any]


@dataclass(frozen=True)
class Answer:
    intent: Intent
    summary: str
    details: list[str]
    citations: list[str]
    gaps: list[str]
    verification_status: str
    trace_id: str


@dataclass(frozen=True)
class VerificationResult:
    status: str
    issues: list[str]


class IntentError(ValueError):
    """Raised when v1 cannot resolve the question deterministically."""


def _period_label(fiscal_year: int, fiscal_quarter: int | None) -> str:
    if fiscal_quarter is None:
        return f"FY{fiscal_year}"
    return f"FY{fiscal_year} Q{fiscal_quarter}"


def _prior_period_label(intent: Intent) -> str:
    return _period_label(intent.fiscal_year - 1, intent.fiscal_quarter)


def _metrics_view_name(intent: Intent) -> str:
    return metrics_view_name(intent.period_type)


def _transcript_evidence_period_key(intent: Intent) -> str:
    if intent.period_type == "quarter":
        return intent.fiscal_period_key
    return f"FY{intent.fiscal_year} Q4"


def _expects_mda_evidence(intent: Intent) -> bool:
    return not (intent.period_type == "quarter" and intent.fiscal_quarter == 4)


def _money(value: Decimal | None) -> str:
    if value is None:
        return "NA"
    dec = Decimal(value)
    sign = "-" if dec < 0 else ""
    dec = abs(dec)
    if dec >= Decimal("1000000000"):
        return f"{sign}${dec / Decimal('1000000000'):.2f}B"
    if dec >= Decimal("1000000"):
        return f"{sign}${dec / Decimal('1000000'):.2f}M"
    return f"{sign}${dec:,.0f}"


def _pct(value: Decimal | None) -> str:
    if value is None:
        return "NA"
    return f"{Decimal(value) * Decimal('100'):.1f}%"


def _clean_text(value: str, *, max_chars: int = 360) -> str:
    text = re.sub(r"\s+", " ", value).strip()
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3].rstrip() + "..."


def _turn_body(turn: TranscriptTurn) -> str:
    return re.sub(rf"^{re.escape(turn.speaker)}:\s*", "", turn.text).strip()


def _evidence_score(chunk: EvidenceChunk) -> int:
    lead_text = chunk.text[:2400]
    haystack = " ".join(
        [
            chunk.unit_key or "",
            chunk.unit_title or "",
            " ".join(chunk.heading_path or []),
            lead_text,
        ]
    ).lower()
    score = 0
    for pattern, weight in REVENUE_SIGNAL_WEIGHTS:
        score += len(re.findall(pattern, haystack, re.I)) * weight
    for pattern, penalty in BOILERPLATE_PENALTIES:
        score -= len(re.findall(pattern, haystack, re.I)) * penalty
    if chunk.source_kind == "mda" and len(chunk.heading_path or []) <= 1:
        score -= 60
    return score


def _rank_evidence_chunks(
    chunks: list[EvidenceChunk], *, limit: int
) -> list[EvidenceChunk]:
    ranked = sorted(
        chunks,
        key=lambda chunk: (
            -_evidence_score(chunk),
            chunk.chunk_ordinal,
            chunk.chunk_id,
        ),
    )
    strong = [
        chunk for chunk in ranked
        if _evidence_score(chunk) >= WEAK_EVIDENCE_SCORE
    ]
    return strong[:limit]


def _best_evidence_score(chunks: list[EvidenceChunk]) -> int | None:
    if not chunks:
        return None
    return max(_evidence_score(chunk) for chunk in chunks)


def _growth(current: Decimal | None, prior: Decimal | None) -> Decimal | None:
    if current is None or prior is None or prior == 0:
        return None
    return (current - prior) / prior


def parse_revenue_driver_intent(
    conn: psycopg.Connection, question: str, trace: RuntimeTrace
) -> Intent:
    fiscal_match = re.search(r"\bFY\s*(20\d{2})\b|\bfiscal\s+(20\d{2})\b", question, re.I)
    if fiscal_match is None:
        raise IntentError("v1 needs an explicit fiscal year like FY2024.")
    fiscal_year = int(next(group for group in fiscal_match.groups() if group))

    lowered = question.lower()
    if "revenue" not in lowered or not any(
        word in lowered for word in ("drive", "drove", "driver", "drivers", "growth", "grew")
    ):
        raise IntentError("v1 only supports revenue growth driver questions.")

    quarter_match = re.search(r"\bQ\s*([1-4])\b", question, re.I)
    fiscal_quarter = int(quarter_match.group(1)) if quarter_match else None
    period_type = "quarter" if fiscal_quarter is not None else "annual"
    fiscal_period_key = _period_label(fiscal_year, fiscal_quarter)

    ticker_candidates = [
        token
        for token in re.findall(r"\b[A-Z]{1,6}\b", question)
        if token not in {"FY", "Q", "YTD", "TTM"}
    ]
    if not ticker_candidates:
        raise IntentError("v1 needs an explicit uppercase ticker, e.g. PLTR.")

    started = time.perf_counter()
    matches = resolve_company_by_ticker(
        conn, ticker_candidates=ticker_candidates,
    )
    record_action(
        trace,
        label="resolve_company",
        params={"ticker_candidates": [t.upper() for t in ticker_candidates]},
        started=started,
        rows=matches,
        selected_ids=[f"id:{c.id}" for c in matches],
    )
    if not matches:
        raise IntentError(f"No company found for ticker candidates: {', '.join(ticker_candidates)}.")
    if len(matches) > 1:
        tickers = ", ".join(c.ticker for c in matches)
        raise IntentError(f"v1 needs one ticker; resolved multiple: {tickers}.")
    company = matches[0]
    intent = Intent(
        ticker=company.ticker,
        company_id=company.id,
        fiscal_year=fiscal_year,
        fiscal_quarter=fiscal_quarter,
        fiscal_period_key=fiscal_period_key,
        period_type=period_type,
        topic="revenue_growth",
        mode="single_company_period",
        source_question=question,
        asof=None,
    )
    trace.intent = asdict(intent)
    trace.recipe_name = (
        "quarterly_revenue_driver"
        if intent.period_type == "quarter"
        else "single_period_driver"
    )
    return intent


def _traced(
    trace: RuntimeTrace,
    *,
    label: str,
    params: dict[str, Any],
    selected_id_fn=lambda result: [],
):
    """Decorator-light helper: time a primitive call and append a TraceAction."""
    started = time.perf_counter()

    def finish(result):
        rows: list[Any]
        if result is None:
            rows = []
        elif isinstance(result, list):
            rows = result
        else:
            rows = [result]
        record_action(
            trace,
            label=label,
            params=params,
            started=started,
            rows=rows,
            selected_ids=selected_id_fn(result),
        )
        return result

    return finish


def build_revenue_driver_packet(
    conn: psycopg.Connection,
    intent: Intent,
    trace: RuntimeTrace,
    *,
    limit_chunks: int = 3,
) -> RevenueDriverPacket:
    company = _traced(
        trace,
        label="get_company",
        params={"company_id": intent.company_id},
        selected_id_fn=lambda c: [f"id:{c.id}"] if c else [],
    )(get_company(conn, company_id=intent.company_id))

    metric_params_current = {
        "company_id": intent.company_id,
        "fiscal_year": intent.fiscal_year,
        "fiscal_quarter": intent.fiscal_quarter,
        "period_type": intent.period_type,
    }
    current_metrics: FiscalMetric | None = _traced(
        trace,
        label=f"get_current_{intent.period_type}_metrics",
        params=metric_params_current,
    )(
        get_metrics(
            conn,
            company_id=intent.company_id,
            fiscal_year=intent.fiscal_year,
            fiscal_quarter=intent.fiscal_quarter,
            period_type=intent.period_type,
        )
    )
    metric_params_prior = {**metric_params_current, "fiscal_year": intent.fiscal_year - 1}
    prior_metrics: FiscalMetric | None = _traced(
        trace,
        label=f"get_prior_{intent.period_type}_metrics",
        params=metric_params_prior,
    )(
        get_metrics(
            conn,
            company_id=intent.company_id,
            fiscal_year=intent.fiscal_year - 1,
            fiscal_quarter=intent.fiscal_quarter,
            period_type=intent.period_type,
        )
    )

    fact_concepts = ("revenue", "gross_profit", "operating_income", "cfo", "capital_expenditures")
    current_facts: list[FinancialFact] = _traced(
        trace,
        label="get_current_fact_ids",
        params={**metric_params_current, "concepts": list(fact_concepts)},
        selected_id_fn=lambda facts: [f"fact_id:{f.fact_id}" for f in facts],
    )(
        get_financial_facts(
            conn,
            company_id=intent.company_id,
            fiscal_year=intent.fiscal_year,
            fiscal_quarter=intent.fiscal_quarter,
            period_type=intent.period_type,
            concepts=fact_concepts,
        )
    )
    prior_facts: list[FinancialFact] = _traced(
        trace,
        label="get_prior_fact_ids",
        params={**metric_params_prior, "concepts": list(fact_concepts)},
        selected_id_fn=lambda facts: [f"fact_id:{f.fact_id}" for f in facts],
    )(
        get_financial_facts(
            conn,
            company_id=intent.company_id,
            fiscal_year=intent.fiscal_year - 1,
            fiscal_quarter=intent.fiscal_quarter,
            period_type=intent.period_type,
            concepts=fact_concepts,
        )
    )

    artifact_periods: list[ArtifactPeriod] = _traced(
        trace,
        label="list_period_artifacts",
        params={
            "company_id": intent.company_id,
            "period_type": intent.period_type,
            "fiscal_year": intent.fiscal_year,
            "fiscal_period_key": intent.fiscal_period_key,
        },
        selected_id_fn=lambda artifacts: [f"artifact_id:{a.artifact_id}" for a in artifacts],
    )(
        list_documents(
            conn,
            company_id=intent.company_id,
            period_type=intent.period_type,
            fiscal_year=intent.fiscal_year,
            fiscal_period_key=intent.fiscal_period_key,
        )
    )

    current_segment_facts = _traced(
        trace,
        label="get_segment_revenue",
        params=metric_params_current,
        selected_id_fn=lambda facts: [f"fact_id:{f.fact_id}" for f in facts],
    )(
        get_segment_facts(
            conn,
            company_id=intent.company_id,
            fiscal_year=intent.fiscal_year,
            fiscal_quarter=intent.fiscal_quarter,
            period_type=intent.period_type,
        )
    )
    prior_segment_index = _traced(
        trace,
        label="get_prior_segment_revenue",
        params=metric_params_prior,
    )(
        get_segment_value_index(
            conn,
            company_id=intent.company_id,
            fiscal_year=intent.fiscal_year - 1,
            fiscal_quarter=intent.fiscal_quarter,
            period_type=intent.period_type,
        )
    )
    segment_facts: list[SegmentFact] = []
    for fact in current_segment_facts:
        prior_value = prior_segment_index.get((fact.dimension_type, fact.dimension_key))
        segment_facts.append(
            SegmentFact(
                fact_id=fact.fact_id,
                dimension_type=fact.dimension_type,
                dimension_key=fact.dimension_key,
                dimension_label=fact.dimension_label,
                value=fact.value,
                prior_value=prior_value,
                yoy_growth=_growth(fact.value, prior_value),
                fiscal_period_label=fact.fiscal_period_label,
                period_end=fact.period_end,
            )
        )

    candidate_limit = max(limit_chunks * 8, 12)
    mda_candidates: list[EvidenceChunk] = _traced(
        trace,
        label="get_mda_chunk_candidates",
        params={
            "company_id": intent.company_id,
            "fiscal_period_key": intent.fiscal_period_key,
            "section_keys": list(MDA_SECTION_KEYS),
            "limit": candidate_limit,
        },
        selected_id_fn=lambda chunks: [
            t for c in chunks for t in (f"artifact_id:{c.artifact_id}", f"chunk_id:{c.chunk_id}")
        ],
    )(
        get_section_chunks(
            conn,
            company_id=intent.company_id,
            fiscal_period_key=intent.fiscal_period_key,
            section_keys=MDA_SECTION_KEYS,
            source_kind="mda",
            limit=candidate_limit,
        )
    )
    mda_chunks = _rank_evidence_chunks(mda_candidates, limit=limit_chunks)

    fy_end = None if current_metrics is None else current_metrics.fy_end
    annual_q4_fallback = (
        (intent.fiscal_year, fy_end) if intent.period_type == "annual" else None
    )
    earnings_candidates: list[EvidenceChunk] = _traced(
        trace,
        label="get_earnings_release_chunk_candidates",
        params={
            "company_id": intent.company_id,
            "fiscal_period_key": intent.fiscal_period_key,
            "unit_type": "press_release",
            "annual_q4_fallback": annual_q4_fallback,
            "limit": candidate_limit,
        },
        selected_id_fn=lambda chunks: [
            t for c in chunks for t in (f"artifact_id:{c.artifact_id}", f"chunk_id:{c.chunk_id}")
        ],
    )(
        get_text_unit_chunks(
            conn,
            company_id=intent.company_id,
            fiscal_period_key=intent.fiscal_period_key,
            unit_type="press_release",
            source_kind="earnings_release",
            annual_q4_fallback=annual_q4_fallback,
            limit=candidate_limit,
        )
    )
    earnings_chunks = _rank_evidence_chunks(earnings_candidates, limit=limit_chunks)

    transcript_period_key = _transcript_evidence_period_key(intent)
    transcript_turns: list[TranscriptTurn] = _traced(
        trace,
        label="search_transcript_turns",
        params={
            "ticker": intent.ticker,
            "query": TRANSCRIPT_REVENUE_QUERY,
            "fiscal_period_key": transcript_period_key,
            "limit": limit_chunks,
        },
        selected_id_fn=lambda turns: [
            f"chunk_id:{t.chunk_id}" for t in turns if t.chunk_id is not None
        ],
    )(
        search_transcript_turns(
            conn,
            intent.ticker,
            TRANSCRIPT_REVENUE_QUERY,
            fiscal_period_key=transcript_period_key,
            limit=limit_chunks,
        )
    )
    readiness, gaps = _ground_gaps(
        intent=intent,
        current_metrics=current_metrics,
        prior_metrics=prior_metrics,
        artifact_periods=artifact_periods,
        segment_facts=segment_facts,
        mda_chunks=mda_chunks,
        earnings_chunks=earnings_chunks,
        transcript_turns=transcript_turns,
        mda_candidate_count=len(mda_candidates),
        earnings_candidate_count=len(earnings_candidates),
        mda_best_candidate_score=_best_evidence_score(mda_candidates),
        earnings_best_candidate_score=_best_evidence_score(earnings_candidates),
    )
    trace.readiness = asdict(readiness)
    trace.gaps = gaps
    fact_ids = [fact.fact_id for fact in current_facts + prior_facts] + [
        segment.fact_id for segment in segment_facts
    ]
    chunk_ids = [chunk.chunk_id for chunk in mda_chunks + earnings_chunks] + [
        turn.chunk_id for turn in transcript_turns if turn.chunk_id is not None
    ]
    artifact_ids = sorted(
        {
            row.artifact_id
            for row in artifact_periods
        }
        | {chunk.artifact_id for chunk in mda_chunks + earnings_chunks}
        | {turn.artifact_id for turn in transcript_turns}
    )
    provenance = Provenance(
        fact_ids=sorted(fact_ids),
        chunk_ids=sorted(chunk_ids),
        artifact_ids=artifact_ids,
        view_names=[_metrics_view_name(intent)],
        planned_actions=[action.label for action in trace.actions],
    )
    return RevenueDriverPacket(
        intent=intent,
        company=company,
        current_metrics=current_metrics,
        prior_metrics=prior_metrics,
        current_facts=current_facts,
        prior_facts=prior_facts,
        segment_facts=segment_facts,
        artifact_periods=artifact_periods,
        mda_chunks=mda_chunks,
        earnings_chunks=earnings_chunks,
        transcript_turns=transcript_turns,
        readiness=readiness,
        gaps=gaps,
        provenance=provenance,
        trace_summary={
            "trace_id": trace.trace_id,
            "action_count": len(trace.actions),
            "row_count": sum(action.row_count for action in trace.actions),
        },
    )


def _ground_gaps(
    *,
    intent: Intent,
    current_metrics: FiscalMetric | None,
    prior_metrics: FiscalMetric | None,
    artifact_periods: list[ArtifactPeriod],
    segment_facts: list[SegmentFact],
    mda_chunks: list[EvidenceChunk],
    earnings_chunks: list[EvidenceChunk],
    transcript_turns: list[TranscriptTurn],
    mda_candidate_count: int,
    earnings_candidate_count: int,
    mda_best_candidate_score: int | None,
    earnings_best_candidate_score: int | None,
) -> tuple[ReadinessResult, list[str]]:
    checks: list[str] = []
    gaps: list[str] = []
    hard_fail = False
    if current_metrics is None:
        checks.append(
            f"FAIL no {_metrics_view_name(intent)} row for "
            f"{intent.ticker} {intent.fiscal_period_key}"
        )
        hard_fail = True
    elif current_metrics.fiscal_period_label != intent.fiscal_period_key:
        checks.append(
            "FAIL metric period label "
            f"{current_metrics.fiscal_period_label} != {intent.fiscal_period_key}"
        )
        hard_fail = True
    else:
        checks.append(f"PASS {_metrics_view_name(intent)} row exists for {intent.fiscal_period_key}")

    period_artifacts = [
        row for row in artifact_periods
        if (
            row.period_type == "annual" and row.fiscal_period_key == intent.fiscal_period_key
        ) or (
            intent.period_type == "quarter"
            and row.fiscal_period_key == intent.fiscal_period_key
        )
    ]
    if period_artifacts:
        checks.append(f"PASS period artifact exists for {intent.fiscal_period_key}")
    else:
        checks.append(f"FAIL no period artifact for {intent.fiscal_period_key}")
        hard_fail = True

    if hard_fail:
        return ReadinessResult("HARD_FAIL", checks, gaps), gaps

    if prior_metrics is None:
        gaps.append(
            f"Prior-year period metrics missing for {_prior_period_label(intent)}; "
            "YoY growth suppressed."
        )
    if not segment_facts:
        gaps.append("Plan requested segment revenue facts, but none were found.")
    elif not any(segment.prior_value is not None for segment in segment_facts):
        gaps.append("Segment facts found, but no prior-year segment matches were found.")
    if not mda_chunks and _expects_mda_evidence(intent):
        if mda_candidate_count:
            gaps.append(
                "Plan requested MD&A revenue-driver evidence, but no chunks cleared "
                f"the quality threshold (best_score={mda_best_candidate_score}, "
                f"candidates={mda_candidate_count})."
            )
        else:
            gaps.append("Plan requested MD&A evidence, but no period-aligned MD&A chunks were found.")
    elif mda_chunks and (score := _best_evidence_score(mda_chunks)) is not None and score < WEAK_EVIDENCE_SCORE:
        gaps.append(
            "Plan requested MD&A revenue-driver evidence, but top ranked chunks were weak "
            f"(best_score={score}, candidates={mda_candidate_count})."
        )
    if not earnings_chunks:
        if earnings_candidate_count:
            gaps.append(
                "Plan requested earnings-release revenue-driver evidence, but no chunks cleared "
                f"the quality threshold (best_score={earnings_best_candidate_score}, "
                f"candidates={earnings_candidate_count})."
            )
        else:
            gaps.append(
                "Plan requested earnings-release evidence, but no exact annual or FY-end Q4 chunks were found."
            )
    elif (score := _best_evidence_score(earnings_chunks)) is not None and score < WEAK_EVIDENCE_SCORE:
        gaps.append(
            "Plan requested earnings-release revenue-driver evidence, but top ranked chunks were weak "
            f"(best_score={score}, candidates={earnings_candidate_count})."
        )
    if not transcript_turns:
        gaps.append(
            "Plan requested transcript revenue-driver evidence for "
            f"{_transcript_evidence_period_key(intent)}, but no matching speaker turns were found."
        )
    status = "PASS" if not gaps else "SOFT_GAP"
    return ReadinessResult(status, checks, gaps), gaps


def synthesize_revenue_driver_answer(packet: RevenueDriverPacket, trace: RuntimeTrace) -> Answer:
    trace.synthesizer = "deterministic"
    intent = packet.intent
    current = packet.current_metrics
    prior = packet.prior_metrics
    revenue_growth = (
        None if current is None or prior is None
        else _growth(current.revenue_fy, prior.revenue_fy)
    )
    revenue_fact_ids = {
        fact.fiscal_period_label: fact.fact_id
        for fact in packet.current_facts + packet.prior_facts
        if fact.concept == "revenue"
    }
    current_revenue_cite = (
        f"[F:{revenue_fact_ids[intent.fiscal_period_key]}]"
        if intent.fiscal_period_key in revenue_fact_ids
        else f"[M:{_metrics_view_name(intent)}]"
    )
    prior_key = _prior_period_label(intent)
    prior_revenue_cite = (
        f"[F:{revenue_fact_ids[prior_key]}]"
        if prior_key in revenue_fact_ids
        else f"[M:{_metrics_view_name(intent)}]"
    )
    if current is None:
        summary = (
            f"{intent.ticker} {intent.fiscal_period_key} cannot be answered "
            f"because period metrics are missing."
        )
    elif prior is None or revenue_growth is None:
        summary = (
            f"{intent.ticker} {intent.fiscal_period_key} revenue was "
            f"{_money(current.revenue_fy)} {current_revenue_cite}, but prior-year revenue is missing."
        )
    else:
        summary = (
            f"{intent.ticker} {intent.fiscal_period_key} revenue grew "
            f"{_pct(revenue_growth)} YoY, from {_money(prior.revenue_fy)} "
            f"{prior_revenue_cite} to {_money(current.revenue_fy)} {current_revenue_cite}."
        )

    details: list[str] = []
    if packet.segment_facts:
        details.append("Structured revenue drivers from segment facts:")
        for segment in packet.segment_facts[:8]:
            growth = "NA" if segment.yoy_growth is None else _pct(segment.yoy_growth)
            prior_value = "NA" if segment.prior_value is None else _money(segment.prior_value)
            details.append(
                f"- {segment.dimension_type} / {segment.dimension_label}: "
                f"{_money(segment.value)} vs {prior_value} prior year "
                f"({growth} YoY) [F:{segment.fact_id}]"
            )
    else:
        details.append("Structured revenue drivers from segment facts: none found.")

    if packet.mda_chunks:
        details.append("MD&A evidence:")
        for chunk in packet.mda_chunks[:2]:
            details.append(
                f"- {_clean_text(chunk.text)} [S:{chunk.chunk_id}]"
            )
    else:
        details.append("MD&A evidence: none found.")

    if packet.earnings_chunks:
        details.append("Earnings-release evidence:")
        for chunk in packet.earnings_chunks[:2]:
            details.append(
                f"- {_clean_text(chunk.text)} [S:{chunk.chunk_id}]"
            )
    else:
        details.append("Earnings-release evidence: none found.")

    if packet.transcript_turns:
        details.append("Transcript evidence:")
        for turn in packet.transcript_turns[:2]:
            citation = "T:unknown" if turn.chunk_id is None else f"T:{turn.chunk_id}"
            details.append(
                f"- {turn.fiscal_period_label} {turn.speaker}: "
                f"{_clean_text(_turn_body(turn))} [{citation}]"
            )
    else:
        details.append("Transcript evidence: none found.")

    citations = sorted(set(re.findall(r"\[[FST]:[^\]]+\]|\[M:[^\]]+\]", "\n".join([summary, *details]))))
    answer = Answer(
        intent=intent,
        summary=summary,
        details=details,
        citations=citations,
        gaps=packet.gaps,
        verification_status="unverified",
        trace_id=trace.trace_id,
    )
    verification = verify_answer(answer, packet)
    trace.verifier_status = verification.status
    trace.final_citations = citations
    return Answer(
        intent=answer.intent,
        summary=answer.summary,
        details=answer.details,
        citations=answer.citations,
        gaps=answer.gaps + [f"Verification issue: {issue}" for issue in verification.issues],
        verification_status=verification.status,
        trace_id=answer.trace_id,
    )


def verify_answer(answer: Answer, packet: RevenueDriverPacket) -> VerificationResult:
    issues: list[str] = []
    allowed_fact_ids = set(packet.provenance.fact_ids)
    allowed_chunk_ids = set(packet.provenance.chunk_ids)
    text = "\n".join([answer.summary, *answer.details])
    for fact_id in re.findall(r"\[F:(\d+)\]", text):
        if int(fact_id) not in allowed_fact_ids:
            issues.append(f"Fact citation {fact_id} was not in packet provenance.")
    for chunk_id in re.findall(r"\[[ST]:(\d+)\]", text):
        if int(chunk_id) not in allowed_chunk_ids:
            issues.append(f"Chunk citation {chunk_id} was not in packet provenance.")
    for gap in packet.gaps:
        if not gap:
            issues.append("Empty gap string found.")
    if packet.current_metrics and _money(packet.current_metrics.revenue_fy) not in text:
        issues.append("Current revenue value is missing from deterministic answer.")
    if packet.prior_metrics and _money(packet.prior_metrics.revenue_fy) not in text:
        issues.append("Prior revenue value is missing from deterministic answer.")
    return VerificationResult("verified" if not issues else "unverified", issues)


def render_answer(answer: Answer, packet: RevenueDriverPacket) -> str:
    intent = answer.intent
    lines = [
        "Question",
        intent.source_question,
        "",
        "Resolved Intent",
        f"ticker={intent.ticker}",
        f"company_id={intent.company_id}",
        f"period={intent.fiscal_period_key}",
        f"topic={intent.topic}",
        f"mode={intent.mode}",
        f"asof={intent.asof}",
        "",
        "Readiness Checks",
    ]
    lines.extend(f"- {check}" for check in packet.readiness.checks)
    lines.extend(
        [
            "",
            "Answer",
            answer.summary,
            "",
            "Structured Facts",
        ]
    )
    current = packet.current_metrics
    prior = packet.prior_metrics
    if current is None:
        lines.append("- current period metrics: missing")
    else:
        lines.append(f"- revenue: {_money(current.revenue_fy)}")
        lines.append(f"- gross margin: {_pct(current.gross_margin_fy)}")
        lines.append(f"- operating margin: {_pct(current.operating_margin_fy)}")
        lines.append(f"- CFO: {_money(current.cfo_fy)}")
        lines.append(f"- capex: {_money(current.capital_expenditures_fy)}")
        lines.append(f"- FCF (CFO + signed capex): {_money(current.fcf_fy)}")
    if prior is not None and current is not None:
        lines.append(f"- prior-year period revenue: {_money(prior.revenue_fy)}")
        lines.append(f"- revenue YoY growth: {_pct(_growth(current.revenue_fy, prior.revenue_fy))}")
    lines.extend(["", "Driver Details"])
    lines.extend(answer.details)
    lines.extend(["", "Evidence IDs"])
    if not answer.citations:
        lines.append("- none")
    else:
        lines.extend(f"- {citation}" for citation in answer.citations)
    lines.extend(["", "Gaps"])
    if not answer.gaps:
        lines.append("- none")
    else:
        lines.extend(f"- {gap}" for gap in answer.gaps)
    lines.extend(
        [
            "",
            "Trace",
            f"trace_id={answer.trace_id}",
            f"verification_status={answer.verification_status}",
            f"actions={len(packet.provenance.planned_actions)}",
        ]
    )
    return "\n".join(lines)


def write_trace(trace: RuntimeTrace, packet: RevenueDriverPacket | None, answer: Answer | None) -> Path:
    out_dir = Path("outputs/qa_runs")
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{datetime.now(UTC).date().isoformat()}.jsonl"
    payload = {
        "trace": _jsonable(trace),
        "packet": None if packet is None else _jsonable(packet),
        "answer": None if answer is None else _jsonable(answer),
    }
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, sort_keys=True) + "\n")
    return path
