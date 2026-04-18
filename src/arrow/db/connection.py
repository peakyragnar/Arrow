from __future__ import annotations

import os

import psycopg
from dotenv import load_dotenv

load_dotenv()


def _database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL is not set. Add it to .env or the environment."
        )
    return url


def get_conn() -> psycopg.Connection:
    return psycopg.connect(_database_url())
