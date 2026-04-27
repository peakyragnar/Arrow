"""Build a deterministic evidence brief from earnings-call transcripts.

Usage:
    uv run scripts/analyst_transcript_brief.py NVDA --topic margins --periods 4
    uv run scripts/analyst_transcript_brief.py AMD --query "data center demand"
"""

from __future__ import annotations

import argparse
import re
import sys

from arrow.db.connection import get_conn
from arrow.retrieval.transcripts import (
    TranscriptDocument,
    TranscriptTurn,
    get_latest_transcripts,
    search_transcript_turns,
)


TOPIC_QUERIES = {
    "margins": 'margin OR margins OR profitability OR "gross margin" OR "operating margin"',
    "margin": 'margin OR margins OR profitability OR "gross margin" OR "operating margin"',
    "ai demand": '"AI" OR "data center" OR demand OR "accelerated computing"',
    "demand": 'demand OR customer OR customers OR bookings OR backlog',
    "capex": 'capex OR "capital expenditures" OR infrastructure OR "data center"',
    "guidance": 'guidance OR outlook OR forecast OR expect OR expects',
}


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Search recent earnings-call transcripts and print an evidence-first brief."
    )
    parser.add_argument("ticker", help="Company ticker, e.g. NVDA.")
    parser.add_argument(
        "--topic",
        default="margins",
        help="Analyst topic. Known: margins, ai demand, demand, capex, guidance.",
    )
    parser.add_argument(
        "--query",
        help="Override the FTS query. If omitted, derived from --topic.",
    )
    parser.add_argument(
        "--periods",
        type=int,
        default=4,
        help="Number of latest transcripts to search.",
    )
    parser.add_argument(
        "--per-period",
        type=int,
        default=3,
        help="Maximum matching chunks to print per transcript period.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    query = args.query or TOPIC_QUERIES.get(args.topic.lower(), args.topic)
    with get_conn() as conn:
        docs = get_latest_transcripts(conn, args.ticker, n=args.periods)
        results_by_period: list[tuple[TranscriptDocument, list[TranscriptTurn]]] = []
        for doc in docs:
            results = search_transcript_turns(
                conn,
                args.ticker,
                query,
                fiscal_period_key=doc.fiscal_period_key,
                limit=args.per_period,
            )
            results_by_period.append((doc, results))

    print(_render(args.ticker.upper(), args.topic, query, results_by_period))
    return 0 if any(results for _, results in results_by_period) else 1


def _render(
    ticker: str,
    topic: str,
    query: str,
    results_by_period: list[tuple[TranscriptDocument, list[TranscriptTurn]]],
) -> str:
    lines = [
        "Transcript Evidence Brief",
        f"ticker={ticker}",
        f"topic={topic}",
        f"query={query}",
        "",
        "Evidence",
    ]
    if not results_by_period:
        lines.append("- no transcript artifacts found")
    match_count = 0
    periods_with_matches = 0
    for doc, turns in results_by_period:
        lines.append(
            f"{doc.fiscal_period_label} | artifact={doc.artifact_id} | "
            f"published={_date(doc.published_at)} | turns={doc.turn_count}"
        )
        if not turns:
            lines.append("- no matching transcript turns")
            continue
        periods_with_matches += 1
        for turn in turns:
            match_count += 1
            chunk_ref = "unit" if turn.chunk_id is None else f"T:{turn.chunk_id}"
            lines.append(
                f"- {turn.speaker} [{chunk_ref}] "
                f"{_excerpt(turn.text, query)}"
            )
    lines.extend(
        [
            "",
            "Deterministic Read",
            (
                f"Found {match_count} matching transcript chunks across "
                f"{periods_with_matches}/{len(results_by_period)} recent calls."
            ),
        ]
    )
    if match_count == 0:
        lines.append(
            "Gap: FTS found no matching transcript turns for this topic/query in the selected calls."
        )
    return "\n".join(lines)


def _excerpt(value: str, query: str, *, max_chars: int = 420) -> str:
    text = re.sub(r"\s+", " ", value).strip()
    if len(text) <= max_chars:
        return text
    terms = _query_terms(query)
    first_hit = None
    lower = text.lower()
    for term in terms:
        hit = lower.find(term.lower())
        if hit >= 0 and (first_hit is None or hit < first_hit):
            first_hit = hit
    if first_hit is None:
        return text[: max_chars - 3].rstrip() + "..."
    start = max(0, first_hit - max_chars // 3)
    end = min(len(text), start + max_chars)
    snippet = text[start:end].strip()
    if start > 0:
        snippet = "..." + snippet
    if end < len(text):
        snippet = snippet.rstrip() + "..."
    return snippet


def _query_terms(query: str) -> list[str]:
    quoted = re.findall(r'"([^"]+)"', query)
    without_quoted = re.sub(r'"[^"]+"', " ", query)
    words = [
        word
        for word in re.findall(r"[A-Za-z][A-Za-z0-9.-]*", without_quoted)
        if word.upper() not in {"AND", "OR", "NOT"}
    ]
    return [*quoted, *words]


def _date(value) -> str:
    return "NA" if value is None else value.date().isoformat()


if __name__ == "__main__":
    raise SystemExit(main())
