"""Integration tests for the financial_facts schema.

Warning: these tests DROP and recreate the `public` schema in the
configured DATABASE_URL. Run only against a dev or dedicated test
database — never production.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import psycopg
import pytest

from arrow.db.connection import get_conn
from arrow.db.migrations import apply

H32 = b"\x00" * 32


def _reset(conn: psycopg.Connection) -> None:
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE;")
        cur.execute("CREATE SCHEMA public;")
    apply(conn)


def _seed_company(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO companies (cik, ticker, name, fiscal_year_end_md) "
            "VALUES (1045810, 'NVDA', 'NVIDIA Corporation', '01-26') RETURNING id;"
        )
        return cur.fetchone()[0]


def _seed_run(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ingest_runs (run_kind, vendor, status, finished_at) "
            "VALUES ('manual', 'fmp', 'succeeded', now()) RETURNING id;"
        )
        return cur.fetchone()[0]


def _seed_raw_response(conn: psycopg.Connection, run_id: int, *, params_hash: bytes = H32) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw_responses (
                ingest_run_id, vendor, endpoint, params_hash,
                http_status, content_type, body_jsonb,
                raw_hash, canonical_hash
            )
            VALUES (%s, 'fmp', '/income-statement', %s, 200, 'application/json',
                    '{}'::jsonb, %s, %s)
            RETURNING id;
            """,
            (run_id, params_hash, params_hash, params_hash),
        )
        return cur.fetchone()[0]


def _fact(
    conn: psycopg.Connection,
    *,
    company_id: int,
    raw_response_id: int,
    concept: str = "revenue",
    value: Decimal = Decimal("18120000000"),
    period_end: date = date(2024, 10, 27),
    period_type: str = "quarter",
    fiscal_year: int = 2025,
    fiscal_quarter: int | None = 3,
    fiscal_period_label: str = "FY2025 Q3",
    calendar_year: int = 2024,
    calendar_quarter: int = 4,
    calendar_period_label: str = "CY2024 Q4",
    statement: str = "income_statement",
    unit: str = "USD",
    extraction_version: str = "v1",
    published_at: datetime = datetime(2024, 11, 20, tzinfo=timezone.utc),
    superseded_at: datetime | None = None,
    ingest_run_id: int | None = None,
    source_artifact_id: int | None = None,
    dimension_type: str | None = None,
    dimension_key: str | None = None,
    dimension_label: str | None = None,
    dimension_source: str | None = None,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO financial_facts (
                ingest_run_id, company_id, statement, concept, value, unit,
                dimension_type, dimension_key, dimension_label, dimension_source,
                fiscal_year, fiscal_quarter, fiscal_period_label,
                period_end, period_type,
                calendar_year, calendar_quarter, calendar_period_label,
                published_at, superseded_at,
                source_raw_response_id, source_artifact_id, extraction_version
            )
            VALUES (%s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s)
            RETURNING id;
            """,
            (
                ingest_run_id, company_id, statement, concept, value, unit,
                dimension_type, dimension_key, dimension_label, dimension_source,
                fiscal_year, fiscal_quarter, fiscal_period_label,
                period_end, period_type,
                calendar_year, calendar_quarter, calendar_period_label,
                published_at, superseded_at,
                raw_response_id, source_artifact_id, extraction_version,
            ),
        )
        return cur.fetchone()[0]


# ---------- happy path ----------

def test_minimal_fact_insert() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn)
        run_id = _seed_run(conn)
        rr = _seed_raw_response(conn, run_id)
        fid = _fact(conn, company_id=cid, raw_response_id=rr)
        assert fid > 0


# ---------- statement / period_type CHECKs ----------

def test_statement_check_rejects_unknown() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn)
        rr = _seed_raw_response(conn, _seed_run(conn))
        with pytest.raises(psycopg.errors.CheckViolation):
            _fact(conn, company_id=cid, raw_response_id=rr, statement="not_a_statement")


def test_period_type_quarter_iff_fiscal_quarter() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn)
        rr = _seed_raw_response(conn, _seed_run(conn))
        # quarter without fiscal_quarter → reject
        with pytest.raises(psycopg.errors.CheckViolation):
            _fact(
                conn, company_id=cid, raw_response_id=rr,
                period_type="quarter", fiscal_quarter=None,
                fiscal_period_label="FY2025",
            )
        # annual with fiscal_quarter → reject
        with pytest.raises(psycopg.errors.CheckViolation):
            _fact(
                conn, company_id=cid, raw_response_id=rr,
                period_type="annual", fiscal_quarter=4,
                fiscal_period_label="FY2025",
            )


def test_label_regex_enforced() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn)
        rr = _seed_raw_response(conn, _seed_run(conn))
        with pytest.raises(psycopg.errors.CheckViolation):
            _fact(conn, company_id=cid, raw_response_id=rr, fiscal_period_label="fy2025 q3")
        with pytest.raises(psycopg.errors.CheckViolation):
            _fact(conn, company_id=cid, raw_response_id=rr, calendar_period_label="2024Q4")


def test_segment_dimension_contract() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn)
        rr = _seed_raw_response(conn, _seed_run(conn))

        with pytest.raises(psycopg.errors.CheckViolation):
            _fact(
                conn,
                company_id=cid,
                raw_response_id=rr,
                statement="segment",
                concept="revenue",
            )

        with pytest.raises(psycopg.errors.CheckViolation):
            _fact(
                conn,
                company_id=cid,
                raw_response_id=rr,
                dimension_type="product",
                dimension_key="data_center",
                dimension_label="Data Center",
                dimension_source="fmp:revenue-product-segmentation",
            )

        fid = _fact(
            conn,
            company_id=cid,
            raw_response_id=rr,
            statement="segment",
            concept="revenue",
            dimension_type="product",
            dimension_key="data_center",
            dimension_label="Data Center",
            dimension_source="fmp:revenue-product-segmentation",
        )
        assert fid > 0


def test_dimension_key_format_enforced() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn)
        rr = _seed_raw_response(conn, _seed_run(conn))
        with pytest.raises(psycopg.errors.CheckViolation):
            _fact(
                conn,
                company_id=cid,
                raw_response_id=rr,
                statement="segment",
                concept="revenue",
                dimension_type="product",
                dimension_key="Data Center",
                dimension_label="Data Center",
                dimension_source="fmp:revenue-product-segmentation",
            )


# ---------- identity / uniqueness ----------

def test_idempotent_extraction_unique_constraint() -> None:
    """Re-extracting the same payload with the same version cannot duplicate facts."""
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn)
        rr = _seed_raw_response(conn, _seed_run(conn))
        _fact(conn, company_id=cid, raw_response_id=rr)
        with pytest.raises(psycopg.errors.UniqueViolation):
            _fact(conn, company_id=cid, raw_response_id=rr)  # same identity → reject


def test_segment_uniqueness_is_dimension_scoped() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn)
        rr = _seed_raw_response(conn, _seed_run(conn))

        data_center = {
            "statement": "segment",
            "concept": "revenue",
            "dimension_type": "product",
            "dimension_key": "data_center",
            "dimension_label": "Data Center",
            "dimension_source": "fmp:revenue-product-segmentation",
        }
        _fact(conn, company_id=cid, raw_response_id=rr, **data_center)
        _fact(
            conn,
            company_id=cid,
            raw_response_id=rr,
            statement="segment",
            concept="revenue",
            dimension_type="product",
            dimension_key="gaming",
            dimension_label="Gaming",
            dimension_source="fmp:revenue-product-segmentation",
        )
        with pytest.raises(psycopg.errors.UniqueViolation):
            _fact(conn, company_id=cid, raw_response_id=rr, **data_center)


def test_segment_current_uniqueness_is_dimension_scoped() -> None:
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn)
        run_id = _seed_run(conn)
        rr1 = _seed_raw_response(conn, run_id, params_hash=b"\x01" * 32)
        rr2 = _seed_raw_response(conn, run_id, params_hash=b"\x02" * 32)
        kwargs = {
            "statement": "segment",
            "concept": "revenue",
            "dimension_type": "product",
            "dimension_key": "data_center",
            "dimension_label": "Data Center",
            "dimension_source": "fmp:revenue-product-segmentation",
        }

        _fact(conn, company_id=cid, raw_response_id=rr1, **kwargs)
        with pytest.raises(psycopg.errors.UniqueViolation):
            _fact(conn, company_id=cid, raw_response_id=rr2, **kwargs)

        _fact(
            conn,
            company_id=cid,
            raw_response_id=rr2,
            statement="segment",
            concept="revenue",
            dimension_type="product",
            dimension_key="gaming",
            dimension_label="Gaming",
            dimension_source="fmp:revenue-product-segmentation",
        )


def test_at_most_one_current_per_fact() -> None:
    """Two distinct raw_responses → two rows. Both un-superseded → reject."""
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn)
        run_id = _seed_run(conn)
        rr1 = _seed_raw_response(conn, run_id, params_hash=b"\x01" * 32)
        rr2 = _seed_raw_response(conn, run_id, params_hash=b"\x02" * 32)
        _fact(conn, company_id=cid, raw_response_id=rr1)
        # Second insert for same (company, concept, period, period_type, version)
        # without superseding the first → must violate the partial unique index
        with pytest.raises(psycopg.errors.UniqueViolation):
            _fact(conn, company_id=cid, raw_response_id=rr2)


def test_supersession_allows_two_rows() -> None:
    """When the prior row is marked superseded, a new current row is allowed."""
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn)
        run_id = _seed_run(conn)
        rr1 = _seed_raw_response(conn, run_id, params_hash=b"\x01" * 32)
        rr2 = _seed_raw_response(conn, run_id, params_hash=b"\x02" * 32)
        f1 = _fact(conn, company_id=cid, raw_response_id=rr1, value=Decimal("100"))
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE financial_facts SET superseded_at = now() WHERE id = %s;",
                (f1,),
            )
        # Now a new current row is permitted
        f2 = _fact(conn, company_id=cid, raw_response_id=rr2, value=Decimal("98"))
        assert f2 != f1

        # PIT query: current value
        with conn.cursor() as cur:
            cur.execute(
                "SELECT value FROM financial_facts WHERE superseded_at IS NULL "
                "AND company_id = %s AND concept = 'revenue';",
                (cid,),
            )
            assert cur.fetchall() == [(Decimal("98.0000"),)]


# ---------- provenance ----------

def test_source_raw_response_required() -> None:
    """source_raw_response_id is NOT NULL — every fact has lineage."""
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn)
        with pytest.raises(psycopg.errors.NotNullViolation):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO financial_facts (
                        company_id, statement, concept, value, unit,
                        fiscal_year, fiscal_quarter, fiscal_period_label,
                        period_end, period_type,
                        calendar_year, calendar_quarter, calendar_period_label,
                        published_at, extraction_version
                    )
                    VALUES (%s, 'income_statement', 'revenue', 100, 'USD',
                            2025, 3, 'FY2025 Q3',
                            '2024-10-27', 'quarter',
                            2024, 4, 'CY2024 Q4',
                            '2024-11-20Z', 'v1');
                    """,
                    (cid,),
                )


def test_company_fk_blocks_orphan_facts() -> None:
    with get_conn() as conn:
        _reset(conn)
        rr = _seed_raw_response(conn, _seed_run(conn))
        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            _fact(conn, company_id=999_999, raw_response_id=rr)


def test_pit_lookup_pattern() -> None:
    """The canonical PIT query: 'value as of date D' returns the value
    that was current at that moment."""
    with get_conn() as conn:
        _reset(conn)
        cid = _seed_company(conn)
        run_id = _seed_run(conn)
        rr1 = _seed_raw_response(conn, run_id, params_hash=b"\x01" * 32)
        rr2 = _seed_raw_response(conn, run_id, params_hash=b"\x02" * 32)

        # Original published 2024-11-20, restated 2025-02-15
        f1 = _fact(
            conn, company_id=cid, raw_response_id=rr1,
            value=Decimal("100"),
            published_at=datetime(2024, 11, 20, tzinfo=timezone.utc),
        )
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE financial_facts SET superseded_at = %s WHERE id = %s;",
                (datetime(2025, 2, 15, tzinfo=timezone.utc), f1),
            )
        _fact(
            conn, company_id=cid, raw_response_id=rr2,
            value=Decimal("98"),
            published_at=datetime(2025, 2, 15, tzinfo=timezone.utc),
        )

        # As of Jan 1, 2025: should see 100 (original)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT value FROM financial_facts
                WHERE company_id = %s AND concept = 'revenue'
                  AND period_end = '2024-10-27'
                  AND published_at <= %s
                  AND (superseded_at IS NULL OR superseded_at > %s)
                ORDER BY published_at DESC
                LIMIT 1;
                """,
                (cid, datetime(2025, 1, 1, tzinfo=timezone.utc),
                      datetime(2025, 1, 1, tzinfo=timezone.utc)),
            )
            assert cur.fetchone()[0] == Decimal("100.0000")

        # As of Mar 1, 2025: should see 98 (restated)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT value FROM financial_facts
                WHERE company_id = %s AND concept = 'revenue'
                  AND period_end = '2024-10-27'
                  AND published_at <= %s
                  AND (superseded_at IS NULL OR superseded_at > %s)
                ORDER BY published_at DESC
                LIMIT 1;
                """,
                (cid, datetime(2025, 3, 1, tzinfo=timezone.utc),
                      datetime(2025, 3, 1, tzinfo=timezone.utc)),
            )
            assert cur.fetchone()[0] == Decimal("98.0000")
