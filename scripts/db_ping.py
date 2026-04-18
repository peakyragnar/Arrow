"""Smoke test: connect to the arrow database and print version + identity."""

from __future__ import annotations

from arrow.db.connection import get_conn


def main() -> None:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT current_user, current_database(), version();")
        user, db, version = cur.fetchone()
    print(f"user={user}  db={db}")
    print(version)


if __name__ == "__main__":
    main()
