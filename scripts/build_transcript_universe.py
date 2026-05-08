"""Assemble a transcript-bulk-download universe.

Pulls a base index (Russell 1000 via IWB ETF) plus a configurable thematic
overlay of sector / theme ETFs. Deduplicates, attributes each ticker to its
contributing source(s), and writes a JSON manifest.

The manifest is consumed by ``scripts/bulk_seed_transcripts.py`` to drive
the actual ingest. Splitting universe-build from ingest lets us:
- Re-run the universe build alone to refresh thematic membership
- Diff manifests across runs to see what changed
- Hand-edit the ticker list before ingest if needed

Output: ``data/universes/transcript_bulk_<YYYYMMDD>.json``
Schema:
    {
        "generated_at": ISO timestamp,
        "base_index": "IWB (Russell 1000)",
        "themes": {theme_name: [etf_symbol, ...]},
        "ticker_count": N,
        "tickers": [
            {"ticker": "AAPL", "sources": ["IWB", "XLK", "AIQ"]},
            ...
        ]
    }

Usage:
    uv run scripts/build_transcript_universe.py
    uv run scripts/build_transcript_universe.py --output path.json
    uv run scripts/build_transcript_universe.py --add-tickers NVDA,AMD,SMCI
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(REPO_ROOT / ".env")

FMP_KEY = os.environ.get("FMP_API")
if not FMP_KEY:
    print("ERROR: FMP_API not set in .env", file=sys.stderr)
    sys.exit(1)

# Russell 1000 base via iShares Russell 1000 ETF.
BASE_INDEX = ("IWB", "Russell 1000 (via IWB ETF)")

# Thematic overlay. Each theme expands beyond Russell 1000 to capture
# specialized names (small/mid-caps, ADRs, etc.) within the theme.
# Choose multiple ETFs per theme to widen coverage; dedup happens at the end.
THEMES: dict[str, list[str]] = {
    "technology":    ["XLK", "VGT"],
    "industrials":   ["XLI", "VIS"],
    "semiconductor": ["SMH", "SOXX"],
    "ai":            ["AIQ", "BOTZ", "CHAT"],
    "minerals":      ["XLB", "REMX", "COPX", "LIT", "URA", "PICK"],
    "power":         ["XLU", "VPU", "PAVE", "GRID"],
}

# Skip tickers that aren't US common stocks. ETFs include some cash holdings
# (USD), futures-related rows, and money market funds. Filter loosely; the
# bulk-seed step will do an authoritative profile lookup.
TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,8}$")
EXCLUDE_TICKERS = {"USD", "EUR", "GBP", "JPY", "CASH", "CASH_USD", "MXN"}


def fetch_json(url: str, timeout: int = 15) -> list | dict:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        body = e.read()[:200].decode(errors="replace")
        raise RuntimeError(f"HTTP {e.code} on {url[:80]}…: {body}") from None


def fetch_index_constituents(symbol: str) -> list[str]:
    """Pull constituent tickers from FMP's stable/sp500-constituent or similar."""
    url = f"https://financialmodelingprep.com/stable/sp500-constituent?apikey={FMP_KEY}"
    rows = fetch_json(url) if symbol == "SPX" else None
    if rows is None:
        return []
    return [r["symbol"] for r in rows if "symbol" in r]


def fetch_etf_holdings(symbol: str) -> list[str]:
    """Pull holding tickers from FMP's stable/etf/holdings endpoint."""
    url = f"https://financialmodelingprep.com/stable/etf/holdings?symbol={symbol}&apikey={FMP_KEY}"
    rows = fetch_json(url)
    if not isinstance(rows, list):
        return []
    out: list[str] = []
    for r in rows:
        asset = r.get("asset")
        if not asset:
            continue
        asset = asset.strip().upper()
        if asset in EXCLUDE_TICKERS:
            continue
        if not TICKER_RE.match(asset):
            continue
        out.append(asset)
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        type=Path,
        default=REPO_ROOT / "data" / "universes" / f"transcript_bulk_{datetime.now(UTC):%Y%m%d}.json",
    )
    parser.add_argument(
        "--add-tickers",
        default="",
        help="Comma-separated tickers to force-include (e.g. NVDA,AMD,SMCI)",
    )
    parser.add_argument(
        "--throttle-ms",
        type=int,
        default=80,
        help="Inter-call sleep to keep FMP happy. Default 80ms (~12 calls/s).",
    )
    args = parser.parse_args()

    args.output.parent.mkdir(parents=True, exist_ok=True)

    sources_by_ticker: dict[str, list[str]] = defaultdict(list)

    # Base index
    base_sym, base_label = BASE_INDEX
    print(f"Pulling base index: {base_label}…", flush=True)
    base = fetch_etf_holdings(base_sym)
    for tk in base:
        sources_by_ticker[tk].append(base_sym)
    print(f"  {len(base)} tickers from {base_sym}")
    time.sleep(args.throttle_ms / 1000)

    # Thematic overlay
    for theme, etfs in THEMES.items():
        print(f"Pulling theme '{theme}' ({len(etfs)} ETFs)…", flush=True)
        for sym in etfs:
            try:
                holdings = fetch_etf_holdings(sym)
            except RuntimeError as e:
                print(f"  ⚠ {sym}: {e}")
                continue
            new = sum(1 for tk in holdings if base_sym not in sources_by_ticker[tk] or len(sources_by_ticker[tk]) == 0)
            for tk in holdings:
                if sym not in sources_by_ticker[tk]:
                    sources_by_ticker[tk].append(sym)
            beyond = sum(1 for tk in holdings if tk not in base and tk in sources_by_ticker)
            print(f"  {sym:5}  {len(holdings):4} holdings  ({beyond} beyond base)")
            time.sleep(args.throttle_ms / 1000)

    # Force-includes
    if args.add_tickers:
        adds = [t.strip().upper() for t in args.add_tickers.split(",") if t.strip()]
        for tk in adds:
            if "manual" not in sources_by_ticker[tk]:
                sources_by_ticker[tk].append("manual")
        print(f"Force-included {len(adds)} manual ticker(s)")

    # Build manifest
    tickers_sorted = sorted(sources_by_ticker.keys())
    manifest = {
        "generated_at": datetime.now(UTC).isoformat(),
        "base_index": base_label,
        "themes": THEMES,
        "ticker_count": len(tickers_sorted),
        "tickers": [{"ticker": tk, "sources": sources_by_ticker[tk]} for tk in tickers_sorted],
    }

    args.output.write_text(json.dumps(manifest, indent=2))

    # Coverage summary
    by_source_count: dict[int, int] = defaultdict(int)
    by_first_source: dict[str, int] = defaultdict(int)
    for tk, sources in sources_by_ticker.items():
        by_source_count[len(sources)] += 1
        by_first_source[sources[0]] += 1

    print(f"\n{'=' * 60}")
    print(f"Universe assembled: {len(tickers_sorted)} unique tickers")
    print(f"  Base ({base_sym}): {len(base)} tickers")
    print(f"  Beyond base: {len(tickers_sorted) - len(base)} tickers from thematic overlay")
    print(f"\n  Tickers in N sources:")
    for n in sorted(by_source_count):
        print(f"    {n} source(s): {by_source_count[n]:4} tickers")
    print(f"\n  Wrote: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
