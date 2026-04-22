"""Run named screens against the metrics platform views.

A "screen" is a SQL file under `db/queries/screens/` that returns rows
meeting some analytical criterion. Drop a new `.sql` file in that
directory and it becomes a new named screen immediately — no code
change needed.

Usage:

    # List available screens
    uv run scripts/screen.py

    # Run a named screen (tabular output)
    uv run scripts/screen.py roic_rising_4q

    # JSON output for machine consumption
    uv run scripts/screen.py roic_rising_4q --json

    # Show the SQL for a screen without running it
    uv run scripts/screen.py roic_rising_4q --show

Screens read from v_metrics_q, v_metrics_ttm, v_metrics_ttm_yoy,
v_metrics_roic, v_metrics_cy per `docs/architecture/metrics_platform.md`.
Run `uv run scripts/apply_views.py` first if views aren't applied.
"""

from __future__ import annotations

import argparse
import json
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg

from arrow.db.connection import get_conn

SCREENS_DIR = Path(__file__).resolve().parents[1] / "db" / "queries" / "screens"


def _discover() -> dict[str, Path]:
    return {p.stem: p for p in sorted(SCREENS_DIR.glob("*.sql"))}


def _format_cell(v: Any) -> str:
    if v is None:
        return "—"
    if isinstance(v, Decimal):
        return f"{float(v):g}"
    if isinstance(v, float):
        return f"{v:g}"
    return str(v)


def _print_table(cols: list[str], rows: list[tuple]) -> None:
    if not rows:
        print("(no rows)")
        return

    # Compute column widths
    widths = [len(c) for c in cols]
    formatted: list[list[str]] = []
    for r in rows:
        vals = [_format_cell(v) for v in r]
        formatted.append(vals)
        for i, v in enumerate(vals):
            widths[i] = max(widths[i], len(v))

    # Header
    print("  " + "  ".join(c.ljust(widths[i]) for i, c in enumerate(cols)))
    print("  " + "  ".join("-" * widths[i] for i in range(len(cols))))
    for vals in formatted:
        print("  " + "  ".join(v.ljust(widths[i]) for i, v in enumerate(vals)))


def _print_json(cols: list[str], rows: list[tuple]) -> None:
    out = []
    for r in rows:
        out.append({
            c: (float(v) if isinstance(v, Decimal) else v.isoformat() if hasattr(v, "isoformat") else v)
            for c, v in zip(cols, r)
        })
    print(json.dumps(out, indent=2, default=str))


def _list_screens() -> int:
    screens = _discover()
    if not screens:
        print(f"No screens under {SCREENS_DIR}.")
        return 1
    print(f"Available screens ({len(screens)}):")
    for name, path in screens.items():
        # Extract the first non-blank comment line as the description
        desc = "(no description)"
        for line in path.read_text().splitlines():
            stripped = line.lstrip("-").strip()
            if stripped and line.startswith("--"):
                desc = stripped
                break
        print(f"  {name:36s}  {desc}")
    print()
    print("Run: uv run scripts/screen.py <name>")
    return 0


def _run_screen(name: str, as_json: bool) -> int:
    screens = _discover()
    if name not in screens:
        print(f"Unknown screen: {name!r}. Run with no args to list.", file=sys.stderr)
        return 2

    sql = screens[name].read_text()
    with get_conn() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(sql)
            except psycopg.Error as e:
                print(f"Screen {name} failed: {e}", file=sys.stderr)
                return 1
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchall()

    if as_json:
        _print_json(cols, rows)
    else:
        print(f"Screen: {name}  ({len(rows)} row{'s' if len(rows) != 1 else ''})")
        print()
        _print_table(cols, rows)
    return 0


def _show_sql(name: str) -> int:
    screens = _discover()
    if name not in screens:
        print(f"Unknown screen: {name!r}.", file=sys.stderr)
        return 2
    print(screens[name].read_text())
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run named screens against the metrics platform."
    )
    parser.add_argument("name", nargs="?", help="Screen name (omit to list).")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of a table.")
    parser.add_argument("--show", action="store_true", help="Print the screen's SQL instead of running.")
    args = parser.parse_args()

    if args.name is None:
        return _list_screens()
    if args.show:
        return _show_sql(args.name)
    return _run_screen(args.name, as_json=args.json)


if __name__ == "__main__":
    raise SystemExit(main())
