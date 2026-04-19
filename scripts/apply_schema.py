"""Apply pending SQL migrations to the Arrow database."""

from __future__ import annotations

from arrow.db.connection import get_conn
from arrow.db.migrations import apply


def main() -> None:
    with get_conn() as conn:
        applied = apply(conn)
    if applied:
        print(f"Applied {len(applied)} migration(s):")
        for name in applied:
            print(f"  - {name}")
    else:
        print("No pending migrations.")


if __name__ == "__main__":
    main()
