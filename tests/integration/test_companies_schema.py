"""Integration tests for the companies schema.

Warning: these tests DROP and recreate the `public` schema in the
configured DATABASE_URL. Run only against a dev or dedicated test
database — never production.
"""

from __future__ import annotations

import psycopg
import pytest

from arrow.db.connection import get_conn
from arrow.db.migrations import apply


def _reset(conn: psycopg.Connection) -> None:
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE;")
        cur.execute("CREATE SCHEMA public;")
    apply(conn)


def _insert(conn: psycopg.Connection, **fields: object) -> int:
    cols = list(fields.keys())
    vals = list(fields.values())
    placeholders = ", ".join(["%s"] * len(cols))
    sql = f"INSERT INTO companies ({', '.join(cols)}) VALUES ({placeholders}) RETURNING id;"
    with conn.cursor() as cur:
        cur.execute(sql, vals)
        return cur.fetchone()[0]


def test_minimal_insert_succeeds() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _insert(
            conn,
            cik=1045810,
            ticker="NVDA",
            name="NVIDIA Corporation",
            fiscal_year_end_md="01-26",
        )
        assert cid > 0


def test_cik_unique() -> None:
    with get_conn() as conn:
        _reset(conn)
        _insert(conn, cik=1045810, ticker="NVDA", name="NVIDIA", fiscal_year_end_md="01-26")
        with pytest.raises(psycopg.errors.UniqueViolation):
            _insert(conn, cik=1045810, ticker="DUPE", name="dupe", fiscal_year_end_md="01-26")


def test_status_check_rejects_unknown() -> None:
    with get_conn() as conn:
        _reset(conn)
        with pytest.raises(psycopg.errors.CheckViolation):
            _insert(
                conn,
                cik=1045810,
                ticker="NVDA",
                name="NVIDIA",
                fiscal_year_end_md="01-26",
                status="not_a_status",
            )


def test_status_accepts_all_declared_values() -> None:
    declared = ["active", "delisted", "merged", "acquired", "private"]
    with get_conn() as conn:
        _reset(conn)
        for i, s in enumerate(declared):
            _insert(
                conn,
                cik=1000 + i,
                ticker=f"T{i}",
                name=f"Company {i}",
                fiscal_year_end_md="12-31",
                status=s,
            )


def test_fiscal_year_end_md_format() -> None:
    with get_conn() as conn:
        _reset(conn)
        # Bad formats
        for bad in ["1-26", "01-2", "13-01", "01-32", "00-15", "01/26", "Jan-26"]:
            with pytest.raises(psycopg.errors.CheckViolation):
                _insert(
                    conn,
                    cik=hash(bad) % 1_000_000_000,
                    ticker="X",
                    name="x",
                    fiscal_year_end_md=bad,
                )
        # Valid formats from real companies
        for good_cik, good in [(1, "01-26"), (2, "06-30"), (3, "09-30"), (4, "12-31")]:
            _insert(
                conn,
                cik=good_cik,
                ticker=f"T{good_cik}",
                name=f"c{good_cik}",
                fiscal_year_end_md=good,
            )


def test_cik_must_be_positive() -> None:
    with get_conn() as conn:
        _reset(conn)
        with pytest.raises(psycopg.errors.CheckViolation):
            _insert(conn, cik=0, ticker="X", name="x", fiscal_year_end_md="12-31")
        with pytest.raises(psycopg.errors.CheckViolation):
            _insert(conn, cik=-1, ticker="X", name="x", fiscal_year_end_md="12-31")
