"""Contract tests for the read-only FMP transcript export boundary.

No network. Postgres is a duck-typed cursor (SELECT-only). Cache files
live under a temporary directory resolved through ``fmp_transcript_path``.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from arrow.export.transcripts import (
    CONTRACT,
    MANIFEST_KEYS,
    RECORD_KEYS,
    TranscriptExportError,
    export_transcripts,
)
from arrow.ingest.fmp.paths import fmp_transcript_path

WITNESS_RAW = (
    b'[{"symbol":"ZZZ","period":"Q4","year":2021,"date":"2021-08-16",'
    b'"content":"Operator: Welcome.\\nJane Doe: Thanks.\\n"}]'
)
WITNESS_RAW_SHA256 = "562aa4ced542380274032e7591cc9abf6b98b0756e6cdc340e1c69f1875ec1a0"
WITNESS_CANONICAL_SHA256 = "23f246c631fafee3de90f478c17885719b549c6a7e981bb05b21d59c01644900"
WITNESS_MANIFEST_CONTENT_SHA256 = (
    "8dc07e7c979e738f38204cdfb70223583a827f98596bb99296bf472a65f2e591"
)
WITNESS_SOURCE_DOCUMENT_ID = "fmp:earning-call-transcript:ZZZ:FY2021-Q4"


class FakeCursor:
    def __init__(self, conn: FakeConn) -> None:
        self._conn = conn
        self._rows: list[tuple[Any, ...]] = []
        self._one: tuple[Any, ...] | None = None

    def execute(self, sql: str, params: Any = None) -> None:
        self._conn.statements.append(sql)
        self._conn.params.append(params)
        lowered = " ".join(sql.lower().split())
        if not lowered.startswith("select"):
            raise AssertionError(f"export issued a non-SELECT: {sql}")
        for verb in ("insert", "update", "delete", "truncate", "drop", "alter"):
            if f"{verb} " in lowered:
                raise AssertionError(f"export issued a mutating statement: {sql}")
        if "from companies" in lowered:
            self._one = self._conn.company_row
            self._rows = [self._conn.company_row] if self._conn.company_row else []
        elif "from artifacts" in lowered:
            self._one = None
            self._rows = list(self._conn.artifact_rows)
        else:
            raise AssertionError(f"unexpected SQL: {sql}")

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._one

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None


class FakeConn:
    def __init__(
        self,
        *,
        company_row: tuple[Any, ...] | None,
        artifact_rows: list[tuple[Any, ...]],
    ) -> None:
        self.company_row = company_row
        self.artifact_rows = artifact_rows
        self.statements: list[str] = []
        self.params: list[Any] = []

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)


def _digest(body: bytes) -> bytes:
    return hashlib.sha256(body).digest()


def _canonical_digest(body: bytes) -> bytes:
    parsed = json.loads(body)
    canonical = json.dumps(parsed, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canonical).digest()


def _fmp_raw(
    *,
    symbol: str = "ACME",
    year: int = 2025,
    period: str = "Q1",
    call_date: str = "2025-01-15",
    content: str = "Operator: Hello",
    extra: dict[str, Any] | None = None,
) -> bytes:
    row: dict[str, Any] = {
        "symbol": symbol,
        "period": period,
        "year": year,
        "date": call_date,
        "content": content,
    }
    if extra:
        row.update(extra)
    return json.dumps([row], separators=(",", ":")).encode("utf-8")


def _write_cache(ticker: str, year: int, quarter: int, body: bytes) -> Path:
    path = fmp_transcript_path(ticker, year, quarter)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    return path


def _row(
    *,
    ticker: str = "ACME",
    company_ticker: str | None = "ACME",
    cik: int | None = 11111,
    fiscal_year: int | None = 2025,
    fiscal_quarter: int | None = 1,
    period_end: date | None = date(2024, 12, 31),
    published_at: datetime | None = datetime(2025, 1, 15, tzinfo=timezone.utc),
    source: str = "fmp",
    source_document_id: str | None = "fmp:earning-call-transcript:ACME:FY2025-Q1",
    raw_hash: bytes | None = None,
    canonical_hash: bytes | None = None,
    body: bytes | None = None,
) -> tuple[Any, ...]:
    payload = body if body is not None else _fmp_raw()
    if raw_hash is None:
        raw_hash = _digest(payload)
    if canonical_hash is None:
        canonical_hash = _canonical_digest(payload)
    return (
        ticker,
        company_ticker,
        cik,
        fiscal_year,
        fiscal_quarter,
        period_end,
        published_at,
        source,
        source_document_id,
        raw_hash,
        canonical_hash,
    )


def _export(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    rows: list[tuple[Any, ...]],
    bodies: dict[tuple[int, int], bytes] | None = None,
    company_row: tuple[Any, ...] | None = (11111,),
    since: date = date(2021, 8, 1),
    output_name: str = "bundle",
    exported_at: datetime | None = None,
    ticker: str = "ACME",
    expected_count: int | None = None,
) -> tuple[Any, FakeConn, Path]:
    cache_root = tmp_path / "raw"
    monkeypatch.setattr("arrow.ingest.common.cache.RAW_DIR", cache_root)
    bodies = bodies or {}
    for row in rows:
        year = int(row[3])
        quarter = int(row[4])
        body = bodies.get((year, quarter), _fmp_raw())
        _write_cache(ticker, year, quarter, body)
    conn = FakeConn(company_row=company_row, artifact_rows=rows)
    output_dir = tmp_path / output_name
    result = export_transcripts(
        conn,  # type: ignore[arg-type]
        ticker=ticker,
        since=since,
        output_dir=output_dir,
        exported_at=exported_at or datetime(2026, 8, 25, 12, 0, tzinfo=timezone.utc),
        expected_count=expected_count,
    )
    return result, conn, output_dir


def _load_cli() -> Any:
    path = Path(__file__).resolve().parents[2] / "scripts" / "export_transcripts.py"
    spec = importlib.util.spec_from_file_location("export_transcripts_cli", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_happy_path_writes_exact_bytes_and_contract(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    q1 = _fmp_raw(period="Q1", call_date="2025-01-15", content="Operator: Hello")
    q2 = _fmp_raw(period="Q2", call_date="2025-04-20", content="Jane Doe: Growth")
    rows = [
        _row(
            fiscal_year=2025,
            fiscal_quarter=2,
            period_end=date(2025, 3, 31),
            published_at=datetime(2025, 4, 20, tzinfo=timezone.utc),
            source_document_id="fmp:earning-call-transcript:ACME:FY2025-Q2",
            body=q2,
        ),
        _row(
            fiscal_year=2025,
            fiscal_quarter=1,
            period_end=date(2024, 12, 31),
            published_at=datetime(2025, 1, 15, tzinfo=timezone.utc),
            source_document_id="fmp:earning-call-transcript:ACME:FY2025-Q1",
            body=q1,
        ),
    ]
    result, conn, output_dir = _export(
        tmp_path,
        monkeypatch,
        rows=rows,
        bodies={(2025, 1): q1, (2025, 2): q2},
        expected_count=2,
    )

    assert result.contract == CONTRACT
    assert result.record_count == 2
    assert result.ticker == "ACME"
    manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    assert set(manifest) == set(MANIFEST_KEYS)
    assert manifest["contract"] == "arrow-transcript-export-v1"
    assert manifest["cutoff"] == {"since": "2021-08-01", "ticker": "ACME"}
    assert manifest["source"] == {
        "artifact_type": "transcript",
        "provider": "fmp",
        "selection": "current",
        "system": "arrow",
    }
    assert [item["fiscal_quarter"] for item in manifest["records"]] == ["Q1", "Q2"]
    first, second = manifest["records"]
    assert set(first) == set(RECORD_KEYS)
    assert first["ticker"] == "ACME"
    assert first["cik"] == 11111
    assert first["fiscal_year"] == 2025
    assert first["fiscal_quarter"] == "Q1"
    assert first["period_end"] == "2024-12-31"
    assert first["call_date"] == "2025-01-15"
    assert first["published_at_claim"] == "2025-01-15T00:00:00Z"
    assert first["source"] == "fmp"
    assert first["provider"] == "fmp"
    assert first["source_document_id"] == "fmp:earning-call-transcript:ACME:FY2025-Q1"
    assert first["raw_path"] == "raw/FY2025-Q1.json"
    assert first["raw_sha256"] == _digest(q1).hex()
    assert first["canonical_sha256"] == _canonical_digest(q1).hex()
    assert second["canonical_sha256"] == _canonical_digest(q2).hex()
    assert (output_dir / "raw" / "FY2025-Q1.json").read_bytes() == q1
    assert (output_dir / "raw" / "FY2025-Q2.json").read_bytes() == q2
    assert second["raw_path"] == "raw/FY2025-Q2.json"
    for statement in conn.statements:
        assert " ".join(statement.lower().split()).startswith("select")


def test_content_digest_ignores_exported_at(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    body = _fmp_raw()
    rows = [_row(body=body)]
    first, _, out1 = _export(
        tmp_path,
        monkeypatch,
        rows=rows,
        bodies={(2025, 1): body},
        exported_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        output_name="a",
    )
    second, _, out2 = _export(
        tmp_path,
        monkeypatch,
        rows=rows,
        bodies={(2025, 1): body},
        exported_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
        output_name="b",
    )
    man1 = json.loads((out1 / "manifest.json").read_text(encoding="utf-8"))
    man2 = json.loads((out2 / "manifest.json").read_text(encoding="utf-8"))
    assert first.content_sha256 == second.content_sha256
    assert man1["content_sha256"] == man2["content_sha256"]
    assert man1["exported_at"] != man2["exported_at"]
    assert man1["records"] == man2["records"]


def test_missing_cache_file_leaves_no_complete_bundle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("arrow.ingest.common.cache.RAW_DIR", tmp_path / "raw")
    conn = FakeConn(company_row=(11111,), artifact_rows=[_row()])
    output_dir = tmp_path / "bundle"
    with pytest.raises(TranscriptExportError, match="missing cache file"):
        export_transcripts(
            conn,  # type: ignore[arg-type]
            ticker="ACME",
            since=date(2021, 8, 1),
            output_dir=output_dir,
        )
    assert not output_dir.exists()
    leftovers = list(tmp_path.glob(".bundle.export-tmp-*"))
    assert leftovers == []


def test_hash_mismatch_refuses_and_does_not_publish(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("arrow.ingest.common.cache.RAW_DIR", tmp_path / "raw")
    body = _fmp_raw()
    _write_cache("ACME", 2025, 1, body)
    row = _row(body=body, raw_hash=_digest(b"not-the-file"))
    conn = FakeConn(company_row=(11111,), artifact_rows=[row])
    output_dir = tmp_path / "bundle"
    with pytest.raises(TranscriptExportError, match="raw hash mismatch"):
        export_transcripts(
            conn,  # type: ignore[arg-type]
            ticker="ACME",
            since=date(2021, 8, 1),
            output_dir=output_dir,
        )
    assert not output_dir.exists()


def test_canonical_hash_mismatch_refuses_and_does_not_publish(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    body = _fmp_raw()
    with pytest.raises(TranscriptExportError, match="canonical hash mismatch"):
        _export(
            tmp_path,
            monkeypatch,
            rows=[_row(body=body, canonical_hash=_digest(b"wrong-canonical"))],
            bodies={(2025, 1): body},
        )
    assert not (tmp_path / "bundle").exists()


def test_duplicate_source_document_id(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    q1 = _fmp_raw(period="Q1", call_date="2025-01-15")
    q2 = _fmp_raw(period="Q2", call_date="2025-04-20")
    rows = [
        _row(body=q1, fiscal_quarter=1),
        _row(
            body=q2,
            fiscal_quarter=2,
            published_at=datetime(2025, 4, 20, tzinfo=timezone.utc),
            source_document_id="fmp:earning-call-transcript:ACME:FY2025-Q1",
            period_end=date(2025, 3, 31),
        ),
    ]
    with pytest.raises(TranscriptExportError, match="duplicate source_document_id"):
        _export(tmp_path, monkeypatch, rows=rows, bodies={(2025, 1): q1, (2025, 2): q2})


def test_duplicate_fiscal_slot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    body = _fmp_raw()
    rows = [
        _row(body=body, source_document_id="fmp:earning-call-transcript:ACME:FY2025-Q1"),
        _row(body=body, source_document_id="fmp:earning-call-transcript:ACME:FY2025-Q1-b"),
    ]
    with pytest.raises(TranscriptExportError, match="duplicate fiscal slot"):
        _export(tmp_path, monkeypatch, rows=rows, bodies={(2025, 1): body})


def test_malformed_metadata_missing_cik(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    body = _fmp_raw()
    with pytest.raises(TranscriptExportError, match="malformed transcript metadata"):
        _export(tmp_path, monkeypatch, rows=[_row(cik=None, body=body)], bodies={(2025, 1): body})


def test_malformed_quarter_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    body = _fmp_raw()
    monkeypatch.setattr("arrow.ingest.common.cache.RAW_DIR", tmp_path / "raw")
    conn = FakeConn(
        company_row=(11111,),
        artifact_rows=[_row(fiscal_quarter=5, body=body)],
    )
    with pytest.raises(TranscriptExportError, match="fiscal_quarter must be 1..4"):
        export_transcripts(
            conn,  # type: ignore[arg-type]
            ticker="ACME",
            since=date(2021, 8, 1),
            output_dir=tmp_path / "bundle",
        )


def test_refuses_nonempty_destination(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    body = _fmp_raw()
    dest = tmp_path / "bundle"
    dest.mkdir()
    (dest / "already").write_text("nope", encoding="utf-8")
    with pytest.raises(TranscriptExportError, match="nonempty directory"):
        _export(
            tmp_path,
            monkeypatch,
            rows=[_row(body=body)],
            bodies={(2025, 1): body},
            output_name="bundle",
        )
    assert (dest / "already").read_text(encoding="utf-8") == "nope"
    assert not (dest / "manifest.json").exists()


def test_empty_destination_is_replaced(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    body = _fmp_raw()
    dest = tmp_path / "bundle"
    dest.mkdir()
    result, _, output_dir = _export(
        tmp_path,
        monkeypatch,
        rows=[_row(body=body)],
        bodies={(2025, 1): body},
        output_name="bundle",
    )
    assert output_dir == dest.resolve()
    assert result.record_count == 1
    assert (dest / "manifest.json").is_file()


def test_company_not_seeded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("arrow.ingest.common.cache.RAW_DIR", tmp_path / "raw")
    conn = FakeConn(company_row=None, artifact_rows=[])
    with pytest.raises(TranscriptExportError, match="not in companies"):
        export_transcripts(
            conn,  # type: ignore[arg-type]
            ticker="ACME",
            since=date(2021, 8, 1),
            output_dir=tmp_path / "bundle",
        )


def test_empty_window_writes_valid_empty_bundle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    result, _, output_dir = _export(tmp_path, monkeypatch, rows=[], bodies={})
    assert result.record_count == 0
    manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["records"] == []
    assert manifest["record_count"] == 0
    assert set(manifest) == set(MANIFEST_KEYS)
    assert (output_dir / "raw").is_dir()
    assert list((output_dir / "raw").iterdir()) == []


def test_expected_count_mismatch_does_not_publish(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    body = _fmp_raw()
    with pytest.raises(TranscriptExportError, match="expected 20 records, got 1"):
        _export(
            tmp_path,
            monkeypatch,
            rows=[_row(body=body)],
            bodies={(2025, 1): body},
            expected_count=20,
        )
    assert not (tmp_path / "bundle").exists()


def test_expected_count_zero_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(TranscriptExportError, match="expected_count must be >= 1"):
        _export(tmp_path, monkeypatch, rows=[], bodies={}, expected_count=0)
    assert not (tmp_path / "bundle").exists()


def test_wrong_payload_symbol_refuses(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    body = _fmp_raw(symbol="OTHER")
    with pytest.raises(TranscriptExportError, match="symbol"):
        _export(
            tmp_path,
            monkeypatch,
            rows=[_row(body=body)],
            bodies={(2025, 1): body},
        )
    assert not (tmp_path / "bundle").exists()


def test_wrong_payload_year_refuses(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    body = _fmp_raw(year=1999)
    with pytest.raises(TranscriptExportError, match="year"):
        _export(
            tmp_path,
            monkeypatch,
            rows=[_row(body=body)],
            bodies={(2025, 1): body},
        )
    assert not (tmp_path / "bundle").exists()


def test_wrong_payload_period_refuses(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    body = _fmp_raw(period="Q2")
    with pytest.raises(TranscriptExportError, match="period"):
        _export(
            tmp_path,
            monkeypatch,
            rows=[_row(body=body)],
            bodies={(2025, 1): body},
        )
    assert not (tmp_path / "bundle").exists()


def test_wrong_payload_date_refuses(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    body = _fmp_raw(call_date="2020-01-01")
    with pytest.raises(TranscriptExportError, match="date"):
        _export(
            tmp_path,
            monkeypatch,
            rows=[_row(body=body)],
            bodies={(2025, 1): body},
        )
    assert not (tmp_path / "bundle").exists()


def test_multi_row_payload_refuses(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    row = {
        "symbol": "ACME",
        "period": "Q1",
        "year": 2025,
        "date": "2025-01-15",
        "content": "Operator: Hello",
    }
    body = json.dumps([row, row], separators=(",", ":")).encode("utf-8")
    with pytest.raises(TranscriptExportError, match="got 2 rows"):
        _export(
            tmp_path,
            monkeypatch,
            rows=[_row(body=body)],
            bodies={(2025, 1): body},
        )
    assert not (tmp_path / "bundle").exists()


def test_missing_content_refuses(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    body = json.dumps(
        [{"symbol": "ACME", "period": "Q1", "year": 2025, "date": "2025-01-15"}],
        separators=(",", ":"),
    ).encode("utf-8")
    with pytest.raises(TranscriptExportError, match="missing content"):
        _export(
            tmp_path,
            monkeypatch,
            rows=[_row(body=body)],
            bodies={(2025, 1): body},
        )
    assert not (tmp_path / "bundle").exists()


def test_synthetic_contract_witness_pins(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert hashlib.sha256(WITNESS_RAW).hexdigest() == WITNESS_RAW_SHA256
    assert _canonical_digest(WITNESS_RAW).hex() == WITNESS_CANONICAL_SHA256
    monkeypatch.setattr("arrow.ingest.common.cache.RAW_DIR", tmp_path / "raw")
    _write_cache("ZZZ", 2021, 4, WITNESS_RAW)
    row = _row(
        ticker="ZZZ",
        company_ticker="ZZZ",
        cik=999999,
        fiscal_year=2021,
        fiscal_quarter=4,
        period_end=date(2021, 7, 31),
        published_at=datetime(2021, 8, 16, tzinfo=timezone.utc),
        source_document_id=WITNESS_SOURCE_DOCUMENT_ID,
        body=WITNESS_RAW,
    )
    conn = FakeConn(company_row=(999999,), artifact_rows=[row])
    output_dir = tmp_path / "bundle"
    result = export_transcripts(
        conn,  # type: ignore[arg-type]
        ticker="ZZZ",
        since=date(2021, 8, 1),
        output_dir=output_dir,
        exported_at=datetime(2026, 8, 25, 12, 0, tzinfo=timezone.utc),
        expected_count=1,
    )
    manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    record = manifest["records"][0]
    assert result.content_sha256 == WITNESS_MANIFEST_CONTENT_SHA256
    assert manifest["content_sha256"] == WITNESS_MANIFEST_CONTENT_SHA256
    assert record["raw_sha256"] == WITNESS_RAW_SHA256
    assert record["canonical_sha256"] == WITNESS_CANONICAL_SHA256
    assert record["cik"] == 999999
    assert record["period_end"] == "2021-07-31"
    assert record["call_date"] == "2021-08-16"
    assert record["published_at_claim"] == "2021-08-16T00:00:00Z"
    assert record["source_document_id"] == WITNESS_SOURCE_DOCUMENT_ID
    assert set(record) == set(RECORD_KEYS)
    assert set(manifest) == set(MANIFEST_KEYS)
    assert (output_dir / "raw" / "FY2021-Q4.json").read_bytes() == WITNESS_RAW


def test_cli_requires_expect_count() -> None:
    cli = _load_cli()
    with pytest.raises(SystemExit) as exc:
        cli.main(["FN", "--since", "2021-08-01", "--out", "/tmp/fn-transcripts"])
    assert exc.value.code == 2


def test_cli_expect_count_zero_is_fail(capsys: pytest.CaptureFixture[str]) -> None:
    cli = _load_cli()
    code = cli.main(
        [
            "FN",
            "--since",
            "2021-08-01",
            "--out",
            "/tmp/fn-transcripts",
            "--expect-count",
            "0",
        ]
    )
    assert code == 1
    err = capsys.readouterr().err
    assert "Status: FAIL" in err
    assert "zero exercise" in err


def test_does_not_mutate_cache_bytes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    body = _fmp_raw(extra={"keep": True})
    _export(tmp_path, monkeypatch, rows=[_row(body=body)], bodies={(2025, 1): body})
    cache_path = fmp_transcript_path("ACME", 2025, 1)
    assert cache_path.read_bytes() == body
