"""Deterministic analyst runtime slice for company-period questions."""

from __future__ import annotations

import hashlib
import json
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

import psycopg
from psycopg.rows import dict_row


MDA_SECTION_KEYS = ("item_7_mda", "part1_item2_mda")
REVENUE_SIGNAL_WEIGHTS = (
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
    (r"\btable of contents\b", 4),
    (r"\bsafe harbor\b", 5),
    (r"\bnon-gaap\b", 4),
    (r"\breconciliation table\b|\breconciliation of\b", 5),
    (r"\bfinancial measure\b", 3),
)
WEAK_EVIDENCE_SCORE = 4


@dataclass(frozen=True)
class Intent:
    ticker: str
    company_id: int
    fiscal_year: int
    fiscal_period_key: str
    topic: str
    mode: str
    source_question: str
    asof: str | None = None


@dataclass(frozen=True)
class TraceAction:
    label: str
    params_hash: str
    row_count: int
    duration_ms: float
    selected_ids: list[str] = field(default_factory=list)


@dataclass
class RuntimeTrace:
    trace_id: str
    surface: str
    source_question: str
    started_at: str
    intent: dict[str, Any] | None = None
    readiness: dict[str, Any] | None = None
    recipe_name: str | None = None
    actions: list[TraceAction] = field(default_factory=list)
    synthesizer: str | None = None
    verifier_status: str | None = None
    gaps: list[str] = field(default_factory=list)
    final_citations: list[str] = field(default_factory=list)

    @classmethod
    def start(cls, *, source_question: str, surface: str = "cli") -> "RuntimeTrace":
        return cls(
            trace_id=str(uuid4()),
            surface=surface,
            source_question=source_question,
            started_at=datetime.now(UTC).isoformat(),
        )


@dataclass(frozen=True)
class Company:
    id: int
    ticker: str
    name: str
    cik: int
    fiscal_year_end_md: str


@dataclass(frozen=True)
class FiscalMetric:
    ticker: str
    company_id: int
    fiscal_year: int
    fiscal_period_label: str
    fy_end: Any
    revenue_fy: Decimal | None
    gross_margin_fy: Decimal | None
    operating_margin_fy: Decimal | None
    cfo_fy: Decimal | None
    capital_expenditures_fy: Decimal | None
    fcf_fy: Decimal | None


@dataclass(frozen=True)
class FinancialFact:
    fact_id: int
    statement: str
    concept: str
    value: Decimal
    unit: str
    fiscal_period_label: str
    period_end: Any


@dataclass(frozen=True)
class SegmentFact:
    fact_id: int
    dimension_type: str
    dimension_key: str
    dimension_label: str
    value: Decimal
    prior_value: Decimal | None
    yoy_growth: Decimal | None
    fiscal_period_label: str
    period_end: Any


@dataclass(frozen=True)
class ArtifactPeriod:
    artifact_id: int
    artifact_type: str
    fiscal_period_key: str | None
    fiscal_period_label: str | None
    period_type: str | None
    period_end: Any
    published_at: Any
    source_document_id: str | None
    accession_number: str | None


@dataclass(frozen=True)
class EvidenceChunk:
    source_kind: str
    artifact_id: int
    chunk_id: int
    chunk_ordinal: int
    fiscal_period_key: str | None
    published_at: Any
    source_document_id: str | None
    accession_number: str | None
    unit_key: str
    unit_title: str
    heading_path: list[str]
    text: str


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


def _param_hash(params: Any) -> str:
    payload = json.dumps(_jsonable(params), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    if isinstance(value, tuple):
        return [_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if hasattr(value, "__dict__"):
        return _jsonable(asdict(value))
    return value


def _query(
    conn: psycopg.Connection,
    trace: RuntimeTrace,
    *,
    label: str,
    sql: str,
    params: tuple[Any, ...],
    selected_id_keys: tuple[str, ...] = (),
) -> list[dict[str, Any]]:
    started = time.perf_counter()
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        rows = list(cur.fetchall())
    duration_ms = (time.perf_counter() - started) * 1000
    selected_ids: list[str] = []
    for row in rows:
        for key in selected_id_keys:
            if row.get(key) is not None:
                selected_ids.append(f"{key}:{row[key]}")
    trace.actions.append(
        TraceAction(
            label=label,
            params_hash=_param_hash(params),
            row_count=len(rows),
            duration_ms=round(duration_ms, 2),
            selected_ids=selected_ids,
        )
    )
    return rows


def _one(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    return rows[0] if rows else None


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


def _evidence_score(chunk: EvidenceChunk) -> int:
    haystack = " ".join(
        [
            chunk.unit_key or "",
            chunk.unit_title or "",
            " ".join(chunk.heading_path or []),
            chunk.text,
        ]
    ).lower()
    score = 0
    for pattern, weight in REVENUE_SIGNAL_WEIGHTS:
        score += len(re.findall(pattern, haystack, re.I)) * weight
    for pattern, penalty in BOILERPLATE_PENALTIES:
        score -= len(re.findall(pattern, haystack, re.I)) * penalty
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
    return (strong or ranked)[:limit]


def _best_evidence_score(chunks: list[EvidenceChunk]) -> int | None:
    if not chunks:
        return None
    return max(_evidence_score(chunk) for chunk in chunks)


def _metric(row: dict[str, Any] | None) -> FiscalMetric | None:
    if row is None:
        return None
    return FiscalMetric(**row)


def _company(row: dict[str, Any] | None) -> Company | None:
    if row is None:
        return None
    return Company(**row)


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

    ticker_candidates = [
        token
        for token in re.findall(r"\b[A-Z]{1,6}\b", question)
        if token not in {"FY", "Q", "YTD", "TTM"}
    ]
    if not ticker_candidates:
        raise IntentError("v1 needs an explicit uppercase ticker, e.g. PLTR.")

    rows = _query(
        conn,
        trace,
        label="resolve_company",
        sql="""
            SELECT id, ticker, name, cik, fiscal_year_end_md
            FROM companies
            WHERE upper(ticker) = ANY(%s)
            ORDER BY id;
        """,
        params=([t.upper() for t in ticker_candidates],),
        selected_id_keys=("id",),
    )
    if not rows:
        raise IntentError(f"No company found for ticker candidates: {', '.join(ticker_candidates)}.")
    if len(rows) > 1:
        tickers = ", ".join(row["ticker"] for row in rows)
        raise IntentError(f"v1 needs one ticker; resolved multiple: {tickers}.")
    company = rows[0]
    intent = Intent(
        ticker=company["ticker"],
        company_id=company["id"],
        fiscal_year=fiscal_year,
        fiscal_period_key=f"FY{fiscal_year}",
        topic="revenue_growth",
        mode="single_company_period",
        source_question=question,
        asof=None,
    )
    trace.intent = asdict(intent)
    trace.recipe_name = "single_period_driver"
    return intent


def build_revenue_driver_packet(
    conn: psycopg.Connection,
    intent: Intent,
    trace: RuntimeTrace,
    *,
    limit_chunks: int = 3,
) -> RevenueDriverPacket:
    company = _company(
        _one(
            _query(
                conn,
                trace,
                label="get_company",
                sql="""
                    SELECT id, ticker, name, cik, fiscal_year_end_md
                    FROM companies
                    WHERE id = %s;
                """,
                params=(intent.company_id,),
                selected_id_keys=("id",),
            )
        )
    )
    current_metrics = _metric(
        _one(
            _query(
                conn,
                trace,
                label="get_current_fy_metrics",
                sql="""
                    SELECT
                        ticker,
                        company_id,
                        fiscal_year,
                        fiscal_period_label,
                        fy_end,
                        revenue_fy,
                        gross_margin_fy,
                        operating_margin_fy,
                        cfo_fy,
                        capital_expenditures_fy,
                        cfo_fy + capital_expenditures_fy AS fcf_fy
                    FROM v_metrics_fy
                    WHERE company_id = %s
                      AND fiscal_year = %s
                    ORDER BY fy_end DESC
                    LIMIT 1;
                """,
                params=(intent.company_id, intent.fiscal_year),
            )
        )
    )
    prior_metrics = _metric(
        _one(
            _query(
                conn,
                trace,
                label="get_prior_fy_metrics",
                sql="""
                    SELECT
                        ticker,
                        company_id,
                        fiscal_year,
                        fiscal_period_label,
                        fy_end,
                        revenue_fy,
                        gross_margin_fy,
                        operating_margin_fy,
                        cfo_fy,
                        capital_expenditures_fy,
                        cfo_fy + capital_expenditures_fy AS fcf_fy
                    FROM v_metrics_fy
                    WHERE company_id = %s
                      AND fiscal_year = %s
                    ORDER BY fy_end DESC
                    LIMIT 1;
                """,
                params=(intent.company_id, intent.fiscal_year - 1),
            )
        )
    )
    current_facts = [
        FinancialFact(**row)
        for row in _query(
            conn,
            trace,
            label="get_current_fact_ids",
            sql="""
                SELECT
                    id AS fact_id,
                    statement,
                    concept,
                    value,
                    unit,
                    fiscal_period_label,
                    period_end
                FROM financial_facts
                WHERE company_id = %s
                  AND fiscal_year = %s
                  AND period_type = 'annual'
                  AND dimension_type IS NULL
                  AND concept IN ('revenue', 'gross_profit', 'operating_income', 'cfo', 'capital_expenditures')
                  AND superseded_at IS NULL
                ORDER BY concept;
            """,
            params=(intent.company_id, intent.fiscal_year),
            selected_id_keys=("fact_id",),
        )
    ]
    prior_facts = [
        FinancialFact(**row)
        for row in _query(
            conn,
            trace,
            label="get_prior_fact_ids",
            sql="""
                SELECT
                    id AS fact_id,
                    statement,
                    concept,
                    value,
                    unit,
                    fiscal_period_label,
                    period_end
                FROM financial_facts
                WHERE company_id = %s
                  AND fiscal_year = %s
                  AND period_type = 'annual'
                  AND dimension_type IS NULL
                  AND concept IN ('revenue', 'gross_profit', 'operating_income', 'cfo', 'capital_expenditures')
                  AND superseded_at IS NULL
                ORDER BY concept;
            """,
            params=(intent.company_id, intent.fiscal_year - 1),
            selected_id_keys=("fact_id",),
        )
    ]
    artifact_periods = [
        ArtifactPeriod(**row)
        for row in _query(
            conn,
            trace,
            label="list_period_artifacts",
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
                  AND a.fiscal_year = %s
                  AND a.artifact_type IN ('10k', '10q', '8k', 'press_release')
                  AND a.superseded_at IS NULL
                ORDER BY
                    CASE WHEN a.period_type = 'annual' THEN 0 ELSE 1 END,
                    a.published_at DESC NULLS LAST,
                    a.id DESC;
            """,
            params=(intent.company_id, intent.fiscal_year),
            selected_id_keys=("artifact_id",),
        )
    ]
    current_segment_rows = _query(
        conn,
        trace,
        label="get_segment_revenue",
        sql="""
            SELECT
                id AS fact_id,
                dimension_type,
                dimension_key,
                dimension_label,
                value,
                fiscal_period_label,
                period_end
            FROM financial_facts
            WHERE company_id = %s
              AND fiscal_year = %s
              AND period_type = 'annual'
              AND statement = 'segment'
              AND concept = 'revenue'
              AND superseded_at IS NULL
            ORDER BY dimension_type, value DESC;
        """,
        params=(intent.company_id, intent.fiscal_year),
        selected_id_keys=("fact_id",),
    )
    prior_segment_rows = _query(
        conn,
        trace,
        label="get_prior_segment_revenue",
        sql="""
            SELECT
                dimension_type,
                dimension_key,
                value
            FROM financial_facts
            WHERE company_id = %s
              AND fiscal_year = %s
              AND period_type = 'annual'
              AND statement = 'segment'
              AND concept = 'revenue'
              AND superseded_at IS NULL;
        """,
        params=(intent.company_id, intent.fiscal_year - 1),
    )
    prior_segments = {
        (row["dimension_type"], row["dimension_key"]): row["value"]
        for row in prior_segment_rows
    }
    segment_facts = []
    for row in current_segment_rows:
        prior_value = prior_segments.get((row["dimension_type"], row["dimension_key"]))
        segment_facts.append(
            SegmentFact(
                fact_id=row["fact_id"],
                dimension_type=row["dimension_type"],
                dimension_key=row["dimension_key"],
                dimension_label=row["dimension_label"],
                value=row["value"],
                prior_value=prior_value,
                yoy_growth=_growth(row["value"], prior_value),
                fiscal_period_label=row["fiscal_period_label"],
                period_end=row["period_end"],
            )
        )
    candidate_limit = max(limit_chunks * 8, 12)
    mda_candidates = [
        EvidenceChunk(source_kind="mda", **row)
        for row in _query(
            conn,
            trace,
            label="get_mda_chunk_candidates",
            sql="""
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
                LIMIT %s;
            """,
            params=(intent.company_id, intent.fiscal_period_key, list(MDA_SECTION_KEYS), candidate_limit),
            selected_id_keys=("artifact_id", "chunk_id"),
        )
    ]
    mda_chunks = _rank_evidence_chunks(mda_candidates, limit=limit_chunks)
    fy_end = None if current_metrics is None else current_metrics.fy_end
    earnings_candidates = [
        EvidenceChunk(source_kind="earnings_release", **row)
        for row in _query(
            conn,
            trace,
            label="get_earnings_release_chunk_candidates",
            sql="""
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
                  AND u.unit_type = 'press_release'
                  AND (
                        COALESCE(u.fiscal_period_key, a.fiscal_period_key) = %s
                        OR (
                            a.period_type = 'quarter'
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
                LIMIT %s;
            """,
            params=(
                intent.company_id,
                intent.company_id,
                intent.fiscal_period_key,
                intent.fiscal_year,
                fy_end,
                intent.fiscal_period_key,
                candidate_limit,
            ),
            selected_id_keys=("artifact_id", "chunk_id"),
        )
    ]
    earnings_chunks = _rank_evidence_chunks(earnings_candidates, limit=limit_chunks)
    readiness, gaps = _ground_gaps(
        intent=intent,
        current_metrics=current_metrics,
        prior_metrics=prior_metrics,
        artifact_periods=artifact_periods,
        segment_facts=segment_facts,
        mda_chunks=mda_chunks,
        earnings_chunks=earnings_chunks,
        mda_candidate_count=len(mda_candidates),
        earnings_candidate_count=len(earnings_candidates),
    )
    trace.readiness = asdict(readiness)
    trace.gaps = gaps
    fact_ids = [fact.fact_id for fact in current_facts + prior_facts] + [
        segment.fact_id for segment in segment_facts
    ]
    chunk_ids = [chunk.chunk_id for chunk in mda_chunks + earnings_chunks]
    artifact_ids = sorted(
        {
            row.artifact_id
            for row in artifact_periods
        }
        | {chunk.artifact_id for chunk in mda_chunks + earnings_chunks}
    )
    provenance = Provenance(
        fact_ids=sorted(fact_ids),
        chunk_ids=sorted(chunk_ids),
        artifact_ids=artifact_ids,
        view_names=["v_metrics_fy"],
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
    mda_candidate_count: int,
    earnings_candidate_count: int,
) -> tuple[ReadinessResult, list[str]]:
    checks: list[str] = []
    gaps: list[str] = []
    hard_fail = False
    if current_metrics is None:
        checks.append(f"FAIL no v_metrics_fy row for {intent.ticker} {intent.fiscal_period_key}")
        hard_fail = True
    elif current_metrics.fiscal_period_label != intent.fiscal_period_key:
        checks.append(
            "FAIL metric period label "
            f"{current_metrics.fiscal_period_label} != {intent.fiscal_period_key}"
        )
        hard_fail = True
    else:
        checks.append(f"PASS v_metrics_fy row exists for {intent.fiscal_period_key}")

    annual_artifacts = [
        row for row in artifact_periods
        if row.period_type == "annual" and row.fiscal_period_key == intent.fiscal_period_key
    ]
    if annual_artifacts:
        checks.append(f"PASS annual artifact exists for {intent.fiscal_period_key}")
    else:
        checks.append(f"FAIL no annual artifact for {intent.fiscal_period_key}")
        hard_fail = True

    if hard_fail:
        return ReadinessResult("HARD_FAIL", checks, gaps), gaps

    if prior_metrics is None:
        gaps.append(f"Prior FY metrics missing for FY{intent.fiscal_year - 1}; YoY growth suppressed.")
    if not segment_facts:
        gaps.append("Plan requested segment revenue facts, but none were found.")
    elif not any(segment.prior_value is not None for segment in segment_facts):
        gaps.append("Segment facts found, but no prior-year segment matches were found.")
    if not mda_chunks:
        gaps.append("Plan requested MD&A evidence, but no period-aligned MD&A chunks were found.")
    elif (score := _best_evidence_score(mda_chunks)) is not None and score < WEAK_EVIDENCE_SCORE:
        gaps.append(
            "Plan requested MD&A revenue-driver evidence, but top ranked chunks were weak "
            f"(best_score={score}, candidates={mda_candidate_count})."
        )
    if not earnings_chunks:
        gaps.append(
            "Plan requested earnings-release evidence, but no exact annual or FY-end Q4 chunks were found."
        )
    elif (score := _best_evidence_score(earnings_chunks)) is not None and score < WEAK_EVIDENCE_SCORE:
        gaps.append(
            "Plan requested earnings-release revenue-driver evidence, but top ranked chunks were weak "
            f"(best_score={score}, candidates={earnings_candidate_count})."
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
        else "[M:v_metrics_fy]"
    )
    prior_key = f"FY{intent.fiscal_year - 1}"
    prior_revenue_cite = (
        f"[F:{revenue_fact_ids[prior_key]}]" if prior_key in revenue_fact_ids else "[M:v_metrics_fy]"
    )
    if current is None:
        summary = f"{intent.ticker} {intent.fiscal_period_key} cannot be answered because FY metrics are missing."
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

    citations = sorted(set(re.findall(r"\[[FS]:[^\]]+\]|\[M:[^\]]+\]", "\n".join([summary, *details]))))
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
    for chunk_id in re.findall(r"\[S:(\d+)\]", text):
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
        lines.append("- current FY metrics: missing")
    else:
        lines.append(f"- FY revenue: {_money(current.revenue_fy)}")
        lines.append(f"- gross margin: {_pct(current.gross_margin_fy)}")
        lines.append(f"- operating margin: {_pct(current.operating_margin_fy)}")
        lines.append(f"- CFO: {_money(current.cfo_fy)}")
        lines.append(f"- capex: {_money(current.capital_expenditures_fy)}")
        lines.append(f"- FCF (CFO + signed capex): {_money(current.fcf_fy)}")
    if prior is not None and current is not None:
        lines.append(f"- prior FY revenue: {_money(prior.revenue_fy)}")
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
