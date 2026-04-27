"""Ask Arrow one deterministic analyst question.

MVP scope:
    What drove {TICKER} revenue growth in FY{YEAR}?
    What drove {TICKER} revenue growth in FY{YEAR} Q{N}?

Usage:
    uv run scripts/ask_arrow.py "What drove PLTR revenue growth in FY2024?"
    uv run scripts/ask_arrow.py "What drove NVDA revenue growth in FY2026 Q4?"
"""

from __future__ import annotations

import argparse
import sys

from arrow.analysis.company_context import (
    IntentError,
    RuntimeTrace,
    build_revenue_driver_packet,
    parse_revenue_driver_intent,
    render_answer,
    synthesize_revenue_driver_answer,
    write_trace,
)
from arrow.db.connection import get_conn


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ask one grounded Arrow analyst question."
    )
    parser.add_argument("question", help="Question to answer.")
    parser.add_argument(
        "--limit-chunks",
        type=int,
        default=3,
        help="Maximum MD&A and earnings-release chunks to retrieve.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    trace = RuntimeTrace.start(source_question=args.question)
    packet = None
    answer = None
    try:
        with get_conn() as conn:
            intent = parse_revenue_driver_intent(conn, args.question, trace)
            packet = build_revenue_driver_packet(
                conn,
                intent,
                trace,
                limit_chunks=args.limit_chunks,
            )
        answer = synthesize_revenue_driver_answer(packet, trace)
        print(render_answer(answer, packet))
        write_trace(trace, packet, answer)
        if packet.readiness.status == "HARD_FAIL":
            return 2
        return 0 if answer.verification_status == "verified" else 3
    except IntentError as e:
        print(f"Could not resolve question: {e}", file=sys.stderr)
        write_trace(trace, packet, answer)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
