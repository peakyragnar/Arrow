"""Agent-loop analyst CLI.

Usage:
    uv run scripts/arrow_ask.py "What drove DELL revenue growth in FY2024?"
    uv run scripts/arrow_ask.py --json "What did NVDA management say about sovereign AI in FY2025 calls?"

The agent uses Haiku for tool routing and Sonnet for synthesis. Every run
writes a JSONL trace under outputs/qa_runs/agent/.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict

from arrow.analysis.agent import ask


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ask Arrow a question via the agent loop.")
    parser.add_argument("question", help="Question to answer.")
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the full trace as JSON instead of pretty text.",
    )
    return parser.parse_args(argv)


def _print_trace(trace) -> None:
    print(f"Q: {trace.question}\n")
    if trace.answer:
        print(trace.answer)
    if trace.error:
        print(f"\n[error] {trace.error}", file=sys.stderr)

    print("\n--- trace ---")
    print(f"trace_id={trace.trace_id}")
    print(f"duration_ms={trace.duration_ms}")
    print(f"verifier={trace.verifier_status}")
    if trace.verifier_issues:
        for issue in trace.verifier_issues:
            print(f"  ! {issue}")
    if trace.verifier_warnings:
        print(f"warnings ({len(trace.verifier_warnings)}):")
        for w in trace.verifier_warnings:
            print(f"  ⚠ {w}")

    print(f"\nmodel calls ({len(trace.model_calls)}):")
    for mc in trace.model_calls:
        print(
            f"  {mc.role:<12} {mc.model:<30} "
            f"in={mc.input_tokens} out={mc.output_tokens} ms={mc.duration_ms}"
        )

    print(f"\ntool calls ({len(trace.tool_executions)}):")
    for te in trace.tool_executions:
        cited = "✓" if te.cited_in_answer else " "
        err = f" error={te.error}" if te.error else ""
        print(f"  [{cited}] {te.name:<24} rows={te.row_count:<3} ms={te.duration_ms}{err}")
        if te.evidence_ids:
            preview = ", ".join(te.evidence_ids[:5])
            extra = "" if len(te.evidence_ids) <= 5 else f" (+{len(te.evidence_ids) - 5})"
            print(f"      evidence: {preview}{extra}")

    if trace.citations:
        print(f"\ncitations: {', '.join(trace.citations)}")


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    try:
        trace = ask(args.question)
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps(asdict(trace), indent=2, default=str))
    else:
        _print_trace(trace)

    if trace.error:
        return 3
    if trace.verifier_status != "verified":
        return 4
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
