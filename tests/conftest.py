"""Pytest session setup — isolate tests from the dev database.

This conftest runs before any test module is imported. It repoints
`DATABASE_URL` at `TEST_DATABASE_URL` for the test session so that
integration tests — which TRUNCATE / DROP tables in their teardown
fixtures — never touch the dev Postgres database that powers the
dashboard and backfill scripts.

Setup prerequisites (one-time):
    createdb arrow_test
    echo 'TEST_DATABASE_URL=postgresql://arrow:arrow@localhost:5433/arrow_test' >> .env

Every session the conftest:
    1. Verifies TEST_DATABASE_URL is set (fails early otherwise)
    2. Sets DATABASE_URL = TEST_DATABASE_URL via os.environ, BEFORE any
       test module imports `arrow.db.connection`. python-dotenv's
       load_dotenv() defaults to override=False, so the .env file's
       DATABASE_URL won't overwrite the test value at import time.
    3. Applies migrations + the view stack in a session fixture so the
       test DB always has current schema without manual bootstrapping.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
from dotenv import load_dotenv

# Load .env first so TEST_DATABASE_URL becomes visible.
REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")

TEST_DB_URL = os.environ.get("TEST_DATABASE_URL")
if not TEST_DB_URL:
    sys.stderr.write(
        "\n"
        "ERROR: TEST_DATABASE_URL is not set.\n"
        "       Tests must run against a separate database so they don't\n"
        "       clobber dev data. Set up once with:\n"
        "\n"
        "           createdb arrow_test\n"
        "           echo 'TEST_DATABASE_URL=postgresql://arrow:arrow@localhost:5433/arrow_test' >> .env\n"
        "\n"
    )
    # Raise so pytest exits with a non-zero code and a clear message.
    raise RuntimeError("TEST_DATABASE_URL not configured — see tests/conftest.py")

# Repoint DATABASE_URL before any arrow.* module imports connection.py.
os.environ["DATABASE_URL"] = TEST_DB_URL


@pytest.fixture(scope="session", autouse=True)
def _bootstrap_test_schema():
    """Apply migrations + views on the test DB before any test runs.

    Idempotent: migrations skip if already applied; apply_views is
    DROP+CREATE each time. The overhead is ~sub-second on the view stack
    and a no-op on an already-migrated database.
    """
    # Import after DATABASE_URL has been repointed so arrow.db.connection
    # reads the test URL.
    from arrow.db.connection import get_conn
    from arrow.db.migrations import apply as apply_migrations
    from scripts.apply_views import main as apply_views_main

    conn = get_conn()
    try:
        apply_migrations(conn)
    finally:
        conn.close()

    apply_views_main()
    yield
