"""Hand-rolled migration runner.

Reads `db/schema/*.sql` files in filename-sorted order, applies any not yet
recorded in `schema_migrations`, one transaction per file. Stores each
file's SHA-256 checksum so post-hoc edits to applied migrations fail loudly
instead of drifting silently.

The `schema_migrations` bookkeeping table is bootstrapped by this runner on
first use — it is intentionally NOT a migration file, to avoid a chicken-
and-egg problem.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import psycopg

SCHEMA_DIR = Path(__file__).resolve().parents[3] / "db" / "schema"

BOOTSTRAP_SQL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    filename   text        PRIMARY KEY,
    checksum   text        NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now()
);
"""


class MigrationChanged(RuntimeError):
    """Raised when a previously-applied migration file has been edited."""


def _sha256_hex(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _discover(schema_dir: Path) -> list[Path]:
    return sorted(schema_dir.glob("*.sql"))


def apply(
    conn: psycopg.Connection,
    schema_dir: Path = SCHEMA_DIR,
) -> list[str]:
    """Apply pending migrations. Return filenames of newly-applied files."""
    conn.autocommit = True

    with conn.cursor() as cur:
        cur.execute(BOOTSTRAP_SQL)
        cur.execute("SELECT filename, checksum FROM schema_migrations;")
        applied = dict(cur.fetchall())

    newly_applied: list[str] = []
    for path in _discover(schema_dir):
        content = path.read_bytes()
        checksum = _sha256_hex(content)

        if path.name in applied:
            if applied[path.name] != checksum:
                raise MigrationChanged(
                    f"Migration {path.name} has changed since it was applied "
                    f"(stored {applied[path.name][:12]}…, file {checksum[:12]}…). "
                    f"Edits to applied migrations are not allowed — add a new "
                    f"numbered file instead."
                )
            continue

        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(content.decode("utf-8"))
                cur.execute(
                    "INSERT INTO schema_migrations (filename, checksum) VALUES (%s, %s);",
                    (path.name, checksum),
                )
        newly_applied.append(path.name)

    return newly_applied
