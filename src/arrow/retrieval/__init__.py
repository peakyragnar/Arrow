"""Search-first retrieval primitives for analyst recipes and the agent loop.

Each primitive is a pure function ``(conn, **params) -> typed result`` that
runs one logical SQL/FTS read against the analyst substrate. Primitives never
mix periods, never compose comparisons, and never touch synthesis. Recipes and
the agent loop wrap them with timing/trace metadata externally.
"""

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
from arrow.retrieval.transcripts import (
    TranscriptContext,
    TranscriptDocument,
    TranscriptMentionSummary,
    TranscriptTurn,
    compare_transcript_mentions,
    get_latest_transcripts,
    get_transcript_context,
    search_transcript_turns,
)
from arrow.retrieval.types import (
    ArtifactPeriod,
    Company,
    EvidenceChunk,
    FinancialFact,
    FiscalMetric,
    Intent,
    RuntimeTrace,
    SegmentFact,
    TraceAction,
)

__all__ = [
    "ArtifactPeriod",
    "Company",
    "EvidenceChunk",
    "FinancialFact",
    "FiscalMetric",
    "Intent",
    "RuntimeTrace",
    "SegmentFact",
    "TraceAction",
    "TranscriptContext",
    "TranscriptDocument",
    "TranscriptMentionSummary",
    "TranscriptTurn",
    "compare_transcript_mentions",
    "get_company",
    "get_financial_facts",
    "get_latest_transcripts",
    "get_metrics",
    "get_section_chunks",
    "get_segment_facts",
    "get_segment_value_index",
    "get_text_unit_chunks",
    "get_transcript_context",
    "list_documents",
    "metrics_view_name",
    "resolve_company_by_ticker",
    "search_transcript_turns",
]
