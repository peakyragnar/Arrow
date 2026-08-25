"""Export current FMP transcript artifacts as a read-only directory bundle.

Usage:
    uv run scripts/export_transcripts.py FN --since 2021-08-01 --out /tmp/fn-transcripts --expect-count 20

Writes contract ``arrow-transcript-export-v1``:
    manifest.json
    raw/FY{year}-Q{n}.json

This is a migration boundary. It copies already-cached FMP JSON after
verifying each file against ``artifacts.raw_hash``, recomputing the
canonical hash, and binding payload metadata. It does not fetch from
FMP and does not write to Postgres or ``data/raw/``.

``--expect-count`` is required. A mismatch, including zero records,
prints FAIL and publishes no bundle.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path
from typing import Any

from arrow.db.connection import get_conn
from arrow.export.transcripts import TranscriptExportError, export_transcripts


def _print_success(result: Any) -> None:
    print(f"Exported FMP transcripts for {result.ticker}:")
    print(f"  contract:                  {result.contract}")
    print(f"  since:                     {result.since.isoformat()}")
    print(f"  output:                    {result.output_dir}")
    print(f"  records:                   {result.record_count}")
    print(f"  files copied:              {result.record_count}")
    print(f"  content_sha256:            {result.content_sha256}")
    print()
    print("Status: PASS — read-only bundle written; Postgres and cache unchanged.")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("ticker", metavar="TICKER")
    parser.add_argument(
        "--since",
        required=True,
        type=date.fromisoformat,
        help="include current FMP transcripts with published_at >= this date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--out",
        required=True,
        type=Path,
        help="fresh output directory; refuses to overwrite a nonempty path",
    )
    parser.add_argument(
        "--expect-count",
        required=True,
        type=int,
        dest="expect_count",
        help="required record count; mismatch including zero is FAIL and publishes no bundle",
    )
    args = parser.parse_args(argv)

    ticker = args.ticker.upper()
    if args.expect_count < 1:
        print(
            "Status: FAIL — --expect-count must be >= 1; zero exercise is not PASS",
            file=sys.stderr,
        )
        return 1

    try:
        with get_conn() as conn:
            result = export_transcripts(
                conn,
                ticker=ticker,
                since=args.since,
                output_dir=args.out,
                expected_count=args.expect_count,
            )
    except TranscriptExportError as exc:
        print(f"Status: FAIL — {exc}", file=sys.stderr)
        return 1

    _print_success(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
