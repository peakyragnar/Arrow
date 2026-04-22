"""Review restatement discrepancies for a ticker.

Usage:
    uv run scripts/review_restatements.py SYM

Read-only. Uses:
- current `financial_facts`
- unresolved `data_quality_flags`
- latest stored SEC companyfacts payload in `raw_responses`
- benchmark pairs from `docs/benchmarks/golden_eval.xlsx` `restatements` tab

Goal:
- show only items that still need manual review
- show what Arrow currently stores
- show what SEC latest-filed XBRL says for the same concept/period
- show what the benchmark workbook records as original vs amended
- avoid telling the analyst what to do
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET
from zipfile import ZipFile

from arrow.agents.amendment_detect import _find_matching_xbrl_fact
from arrow.db.connection import get_conn


STRICT_NS = "http://purl.oclc.org/ooxml/spreadsheetml/main"
DOCREL_NS = "http://purl.oclc.org/ooxml/officeDocument/relationships"

BENCHMARK_PATH = Path("docs/benchmarks/golden_eval.xlsx")

BENCHMARK_TO_CANONICAL = {
    "revenue_q": "revenue",
    "cogs_q": "cogs",
    "operating_income_q": "operating_income",
    "income_tax_expense_q": "tax",
    "pretax_income_q": "ebt_incl_unusual",
    "equity_q": "total_equity",
    "accounts_payable_q": "accounts_payable",
    "net_income_q": "net_income",
    "total_assets_q": "total_assets",
}

MAPPING_CLASS = {
    "revenue": "exact_equivalent",
    "cogs": "derived_only",
    "operating_income": "derived_only",
    "tax": "exact_equivalent",
    "ebt_incl_unusual": "exact_equivalent",
    "net_income": "exact_equivalent",
    "total_assets": "equivalent_if_packaged",
    "total_equity": "equivalent_if_packaged",
    "accounts_payable": "bundled_mismatch",
}


@dataclass(frozen=True)
class BenchmarkPair:
    restatement_case_id: str
    ticker: str
    form: str
    accession: str
    filed_at: str
    fiscal_year: int
    fiscal_period: str
    period_end: date
    review_status: str
    notes: str
    values: dict[str, Decimal]


def _col_letters(cell_ref: str) -> str:
    return "".join(ch for ch in cell_ref if ch.isalpha())


def _shared_strings(zf: ZipFile) -> list[str]:
    root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    out: list[str] = []
    for si in root:
        texts = []
        for t in si.iter(f"{{{STRICT_NS}}}t"):
            texts.append(t.text or "")
        out.append("".join(texts))
    return out


def _sheet_target(zf: ZipFile, sheet_name: str) -> str:
    wb = ET.fromstring(zf.read("xl/workbook.xml"))
    rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
    rel_map = {r.attrib["Id"]: r.attrib["Target"] for r in rels}
    sheets = wb.find(f"{{{STRICT_NS}}}sheets")
    if sheets is None:
        raise RuntimeError("Workbook missing sheets")
    for sheet in sheets:
        if sheet.attrib["name"] == sheet_name:
            rid = sheet.attrib[f"{{{DOCREL_NS}}}id"]
            return "xl/" + rel_map[rid]
    raise RuntimeError(f"Sheet {sheet_name!r} not found in benchmark workbook")


def _read_sheet_rows(path: Path, sheet_name: str) -> list[dict[str, str]]:
    with ZipFile(path) as zf:
        sst = _shared_strings(zf)
        target = _sheet_target(zf, sheet_name)
        root = ET.fromstring(zf.read(target))
        sheet_data = root.find(f"{{{STRICT_NS}}}sheetData")
        if sheet_data is None:
            return []
        rows = list(sheet_data)
        if not rows:
            return []

        header_map: dict[str, str] = {}
        for cell in rows[0]:
            v = cell.find(f"{{{STRICT_NS}}}v")
            value = "" if v is None else (v.text or "")
            if cell.attrib.get("t") == "s":
                value = sst[int(value)]
            header_map[_col_letters(cell.attrib["r"])] = value

        out: list[dict[str, str]] = []
        for row in rows[1:]:
            row_data: dict[str, str] = {}
            for cell in row:
                col = _col_letters(cell.attrib["r"])
                header = header_map.get(col)
                if not header:
                    continue
                v = cell.find(f"{{{STRICT_NS}}}v")
                value = "" if v is None else (v.text or "")
                if cell.attrib.get("t") == "s":
                    value = sst[int(value)]
                row_data[header] = value
            if row_data:
                out.append(row_data)
        return out


def _load_benchmark_pairs(ticker: str) -> dict[tuple[str, date], list[BenchmarkPair]]:
    rows = _read_sheet_rows(BENCHMARK_PATH, "restatements")
    grouped: dict[tuple[str, date], list[BenchmarkPair]] = {}
    for row in rows:
        if row.get("ticker", "").upper() != ticker.upper():
            continue
        values: dict[str, Decimal] = {}
        for benchmark_col, canonical in BENCHMARK_TO_CANONICAL.items():
            raw = row.get(benchmark_col, "")
            if raw == "":
                continue
            values[canonical] = Decimal(raw)
        pair = BenchmarkPair(
            restatement_case_id=row["restatement_case_id"],
            ticker=row["ticker"],
            form=row["form"],
            accession=row.get("accession", ""),
            filed_at=row.get("filed_at", ""),
            fiscal_year=int(row["fiscal_year"]),
            fiscal_period=row["fiscal_period"],
            period_end=date.fromisoformat(row["period_end"]),
            review_status=row.get("review_status", ""),
            notes=row.get("notes", ""),
            values=values,
        )
        grouped.setdefault((pair.restatement_case_id, pair.period_end), []).append(pair)
    return grouped


def _fmt_decimal(value: Decimal | None) -> str:
    if value is None:
        return "-"
    integral = value.quantize(Decimal("1"))
    return f"{integral:,}"


def _issue_note(
    *,
    canonical: str,
    current_value: Decimal | None,
    sec_value: Decimal | None,
    amended_value: Decimal | None,
) -> str:
    mapping_class = MAPPING_CLASS.get(canonical, "unknown")
    if current_value == amended_value and amended_value is not None:
        return "resolved"
    if mapping_class == "bundled_mismatch":
        return "bundled mismatch"
    if sec_value is None:
        return "no SEC match"
    if amended_value is not None and sec_value != amended_value:
        return "SEC != benchmark amended"
    if mapping_class == "equivalent_if_packaged":
        return "package review"
    if mapping_class == "derived_only":
        return "derived line"
    if mapping_class == "unknown":
        return "mapping unclear"
    return "review"


def _load_company(conn: Any, ticker: str) -> tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, cik
            FROM companies
            WHERE upper(ticker) = %s
            LIMIT 1;
            """,
            (ticker.upper(),),
        )
        row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"{ticker.upper()} is not seeded")
    return row[0], row[1]


def _load_latest_companyfacts(conn: Any, cik: int) -> dict[str, Any]:
    endpoint = f"api/xbrl/companyfacts/CIK{cik:010d}.json"
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT body_jsonb
            FROM raw_responses
            WHERE vendor = 'sec'
              AND endpoint = %s
              AND body_jsonb IS NOT NULL
            ORDER BY fetched_at DESC
            LIMIT 1;
            """,
            (endpoint,),
        )
        row = cur.fetchone()
    if row is None:
        raise RuntimeError(
            f"No stored SEC companyfacts payload found for CIK {cik}. "
            "Run a backfill or reconciliation first."
        )
    return row[0]


def _load_current_facts(
    conn: Any,
    *,
    company_id: int,
    period_end: date,
    concepts: set[str],
) -> dict[str, Decimal]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT concept, value
            FROM financial_facts
            WHERE company_id = %s
              AND period_end = %s
              AND superseded_at IS NULL
              AND concept = ANY(%s)
            ORDER BY concept;
            """,
            (company_id, period_end, list(concepts)),
        )
        return {concept: value for concept, value in cur.fetchall()}


def _flag_summary(conn: Any, *, company_id: int) -> list[tuple[str, int]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT flag_type, count(*)
            FROM data_quality_flags
            WHERE company_id = %s
              AND resolved_at IS NULL
            GROUP BY flag_type
            ORDER BY flag_type;
            """,
            (company_id,),
        )
        return cur.fetchall()


def _period_flag_summary(conn: Any, *, company_id: int, period_end: date) -> list[tuple[str, str, int]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT flag_type, severity, count(*)
            FROM data_quality_flags
            WHERE company_id = %s
              AND resolved_at IS NULL
              AND period_end = %s
            GROUP BY flag_type, severity
            ORDER BY flag_type, severity;
            """,
            (company_id, period_end),
        )
        return cur.fetchall()


def _period_flag_details(conn: Any, *, company_id: int, period_end: date) -> list[tuple[str, str, str, Decimal | None, Decimal | None]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT coalesce(concept, '(period)'), flag_type, severity, expected_value, computed_value
            FROM data_quality_flags
            WHERE company_id = %s
              AND resolved_at IS NULL
              AND period_end = %s
            ORDER BY flag_type, concept NULLS FIRST;
            """,
            (company_id, period_end),
        )
        return cur.fetchall()


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: review_restatements.py TICKER", file=sys.stderr)
        return 2

    ticker = sys.argv[1].upper()
    benchmark_pairs = _load_benchmark_pairs(ticker)

    with get_conn() as conn:
        company_id, cik = _load_company(conn, ticker)
        companyfacts = _load_latest_companyfacts(conn, cik)
        us_gaap = companyfacts.get("facts", {}).get("us-gaap", {})

        print(f"Restatement review for {ticker}")
        print(f"  company_id: {company_id}")
        print(f"  cik:        {cik}")
        print()
        print("Unresolved flags:")
        for flag_type, count in _flag_summary(conn, company_id=company_id):
            print(f"  - {flag_type}: {count}")

        if not benchmark_pairs:
            print()
            print("No benchmark restatement pairs found for this ticker in golden_eval.xlsx.")
            return 0

        for (case_id, period_end), pair_rows in sorted(benchmark_pairs.items(), key=lambda item: item[0][1]):
            print()
            print("=" * 88)
            print(f"{case_id}  period_end={period_end.isoformat()}")
            forms = ", ".join(f"{p.form} filed {p.filed_at or '?'}" for p in pair_rows)
            print(f"  benchmark filings: {forms}")
            notes = " | ".join(filter(None, {p.notes for p in pair_rows}))
            if notes:
                print(f"  notes: {notes}")

            original = next((p for p in pair_rows if not p.form.endswith("/A")), pair_rows[0])
            amended = next((p for p in pair_rows if p.form.endswith("/A")), pair_rows[-1])
            concepts = set(original.values) | set(amended.values)
            current_values = _load_current_facts(
                conn, company_id=company_id, period_end=period_end, concepts=concepts,
            )

            print("  period flags:")
            for flag_type, severity, count in _period_flag_summary(
                conn, company_id=company_id, period_end=period_end,
            ):
                print(f"    - {flag_type} / {severity}: {count}")

            print("  failed items:")
            flag_concepts = {
                concept for concept, _flag_type, _severity, _expected, _computed
                in _period_flag_details(conn, company_id=company_id, period_end=period_end)
                if concept != "(period)"
            }

            print()
            print("    concept | current_db | sec_latest | workbook_original | workbook_amended | note")
            printed_any = False
            for concept in sorted(concepts):
                sec_fact = _find_matching_xbrl_fact(
                    us_gaap,
                    statement="income_statement" if concept in {
                        "revenue", "cogs", "operating_income", "tax",
                        "ebt_incl_unusual", "net_income",
                    } else "balance_sheet",
                    canonical_concept=concept,
                    period_end=period_end,
                    period_type="quarter",
                )
                sec_value = None if sec_fact is None else Decimal(str(sec_fact["val"]))
                current_value = current_values.get(concept)
                original_value = original.values.get(concept)
                amended_value = amended.values.get(concept)
                note = _issue_note(
                    canonical=concept,
                    current_value=current_value,
                    sec_value=sec_value,
                    amended_value=amended_value,
                )
                needs_review = (
                    concept in flag_concepts
                    or (amended_value is not None and current_value != amended_value)
                    or (sec_value is not None and current_value != sec_value)
                )
                if not needs_review:
                    continue
                printed_any = True
                print(
                    f"    {concept} | {_fmt_decimal(current_value)} | {_fmt_decimal(sec_value)} | "
                    f"{_fmt_decimal(original_value)} | {_fmt_decimal(amended_value)} | "
                    f"{note}"
                )
            if not printed_any:
                print("    none")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
