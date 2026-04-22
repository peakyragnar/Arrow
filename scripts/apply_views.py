"""Apply the metrics-platform view stack from db/queries/*.sql.

Idempotent: drops all views first (reverse dependency order), then
creates them from the numbered SQL files (forward order).

Usage:
    uv run scripts/apply_views.py

Output: list of views created.

See docs/architecture/metrics_platform.md for the view stack design.
"""

from __future__ import annotations

import re
from pathlib import Path

from arrow.db.connection import get_conn

QUERIES_DIR = Path(__file__).resolve().parents[1] / "db" / "queries"

# Extract the CREATE VIEW target name from a SQL file so we can drop it
# before re-applying (shape changes need DROP, not REPLACE).
_CREATE_VIEW_RE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+(\w+)",
    re.IGNORECASE,
)


def _discover() -> list[tuple[str, Path]]:
    """Return (view_name, path) sorted by filename."""
    items: list[tuple[str, Path]] = []
    for p in sorted(QUERIES_DIR.glob("*.sql")):
        content = p.read_text()
        m = _CREATE_VIEW_RE.search(content)
        if not m:
            continue
        items.append((m.group(1), p))
    return items


def main() -> int:
    views = _discover()
    if not views:
        print(f"No view SQL files found under {QUERIES_DIR}")
        return 1

    with get_conn() as conn:
        conn.autocommit = True

        # Drop in reverse order (dependents first).
        print("Dropping existing views (reverse order)...")
        with conn.cursor() as cur:
            for name, _path in reversed(views):
                cur.execute(f"DROP VIEW IF EXISTS {name} CASCADE;")
                print(f"  dropped: {name}")

        # Create in forward order.
        print()
        print("Creating views (forward order)...")
        with conn.cursor() as cur:
            for name, path in views:
                sql = path.read_text()
                cur.execute(sql)
                print(f"  created: {name}  ({path.name})")

    print()
    print(f"Applied {len(views)} views from {QUERIES_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
