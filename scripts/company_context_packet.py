"""Print a one-period company context packet.

This is a read-only period-integrity diagnostic, not an analyst product.

Usage:
    uv run scripts/company_context_packet.py PLTR --fiscal-year 2024
"""

from __future__ import annotations

import argparse
import re
import sys
from decimal import Decimal
from typing import Any

from psycopg.rows import dict_row

from arrow.db.connection import get_conn


MDA_SECTION_KEYS = ("item_7_mda", "part1_item2_mda")


def _expected_fiscal_period_key(fiscal_year: int) -> str:
    return f"FY{fiscal_year}"


def _clean_text(value: str, *, max_chars: int = 520) -> str:
    text = re.sub(r"\s+", " ", value).strip()
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3].rstrip() + "..."


def _fmt_money(value: Any) -> str:
    if value is None:
        return "NA"
    dec = Decimal(value)
    sign = "-" if dec < 0 else ""
    dec = abs(dec)
    if dec >= Decimal("1000000000"):
        return f"{sign}${dec / Decimal('1000000000'):.2f}B"
    if dec >= Decimal("1000000"):
        return f"{sign}${dec / Decimal('1000000'):.2f}M"
    return f"{sign}${dec:,.0f}"


def _fmt_pct(value: Any) -> str:
    if value is None:
        return "NA"
    return f"{Decimal(value) * Decimal('100'):.1f}%"


def _fmt_date(value: Any) -> str:
    return "NA" if value is None else str(value)


def _fetch_company(conn, ticker: str) -> dict[str, Any] | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT id, ticker, name, cik, fiscal_year_end_md
            FROM companies
            WHERE upper(ticker) = upper(%s)
            ORDER BY id
            LIMIT 1;
            """,
            (ticker,),
        )
        return cur.fetchone()


def _fetch_fy_metrics(
    conn, *, company_id: int, fiscal_year: int
) -> dict[str, Any] | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
                ticker,
                company_id,
                fiscal_year,
                fiscal_period_label,
                fy_end,
                revenue_fy,
                gross_margin_fy,
                operating_margin_fy,
                cfo_fy,
                capital_expenditures_fy,
                cfo_fy + capital_expenditures_fy AS fcf_fy
            FROM v_metrics_fy
            WHERE company_id = %s
              AND fiscal_year = %s
            ORDER BY fy_end DESC
            LIMIT 1;
            """,
            (company_id, fiscal_year),
        )
        return cur.fetchone()


def _fetch_artifact_periods(
    conn, *, company_id: int, ticker: str, fiscal_year: int
) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
                a.id AS artifact_id,
                a.artifact_type,
                a.title,
                a.fiscal_year,
                a.fiscal_period_key,
                a.fiscal_period_label,
                a.period_type,
                a.period_end,
                a.published_at,
                a.accession_number,
                a.source_document_id
            FROM artifacts a
            WHERE (a.company_id = %s OR upper(a.ticker) = upper(%s))
              AND a.fiscal_year = %s
              AND a.artifact_type IN ('10k', '10q', '8k', 'press_release')
              AND a.superseded_at IS NULL
            ORDER BY
                CASE WHEN a.period_type = 'annual' THEN 0 ELSE 1 END,
                a.published_at DESC NULLS LAST,
                a.id DESC;
            """,
            (company_id, ticker, fiscal_year),
        )
        return list(cur.fetchall())


def _fetch_mda_chunks(
    conn, *, company_id: int, fiscal_period_key: str, limit: int
) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
                a.id AS artifact_id,
                a.accession_number,
                a.source_document_id,
                a.published_at,
                a.period_end,
                s.fiscal_period_key,
                s.form_family,
                s.section_key,
                s.section_title,
                c.id AS chunk_id,
                c.chunk_ordinal,
                c.heading_path,
                c.text
            FROM artifact_sections s
            JOIN artifact_section_chunks c ON c.section_id = s.id
            JOIN artifacts a ON a.id = s.artifact_id
            WHERE s.company_id = %s
              AND s.fiscal_period_key = %s
              AND s.section_key = ANY(%s)
              AND a.superseded_at IS NULL
            ORDER BY
                a.published_at DESC NULLS LAST,
                CASE s.section_key WHEN 'item_7_mda' THEN 0 ELSE 1 END,
                c.chunk_ordinal
            LIMIT %s;
            """,
            (company_id, fiscal_period_key, list(MDA_SECTION_KEYS), limit),
        )
        return list(cur.fetchall())


def _fetch_press_release_chunks(
    conn,
    *,
    company_id: int,
    ticker: str,
    fiscal_period_key: str,
    fiscal_year: int,
    fy_end: Any,
    limit: int,
) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
                a.id AS artifact_id,
                a.accession_number,
                a.source_document_id,
                a.period_type,
                a.published_at,
                a.period_end,
                COALESCE(u.fiscal_period_key, a.fiscal_period_key) AS fiscal_period_key,
                u.unit_key,
                u.unit_title,
                c.id AS chunk_id,
                c.chunk_ordinal,
                c.heading_path,
                c.text
            FROM artifact_text_units u
            JOIN artifact_text_chunks c ON c.text_unit_id = u.id
            JOIN artifacts a ON a.id = u.artifact_id
            WHERE (u.company_id = %s OR a.company_id = %s OR upper(a.ticker) = upper(%s))
              AND u.unit_type = 'press_release'
              AND (
                    COALESCE(u.fiscal_period_key, a.fiscal_period_key) = %s
                    OR (
                        a.period_type = 'quarter'
                        AND a.fiscal_year = %s
                        AND a.fiscal_quarter = 4
                        AND a.period_end = %s
                    )
              )
              AND a.superseded_at IS NULL
            ORDER BY
                CASE
                    WHEN COALESCE(u.fiscal_period_key, a.fiscal_period_key) = %s THEN 0
                    ELSE 1
                END,
                a.published_at DESC NULLS LAST,
                u.unit_ordinal,
                c.chunk_ordinal
            LIMIT %s;
            """,
            (
                company_id,
                company_id,
                ticker,
                fiscal_period_key,
                fiscal_year,
                fy_end,
                fiscal_period_key,
                limit,
            ),
        )
        return list(cur.fetchall())


def _period_mismatches(
    artifacts: list[dict[str, Any]], *, expected_key: str
) -> list[dict[str, Any]]:
    return [
        artifact
        for artifact in artifacts
        if artifact["period_type"] == "annual"
        if artifact["fiscal_period_key"] is not None
        and artifact["fiscal_period_key"] != expected_key
    ]


def _integrity_status(
    *,
    metrics: dict[str, Any] | None,
    expected_key: str,
    period_mismatches: list[dict[str, Any]],
    mda_chunks: list[dict[str, Any]],
    press_release_chunks: list[dict[str, Any]],
) -> tuple[str, list[str]]:
    reasons: list[str] = []

    if metrics is None:
        reasons.append("No annual FMP metrics row found for requested fiscal year.")
    elif metrics["fiscal_period_label"] != expected_key:
        reasons.append(
            "FMP annual metrics row has fiscal_period_label="
            f"{metrics['fiscal_period_label']}, expected {expected_key}."
        )

    for row in period_mismatches:
        reasons.append(
            f"{row['artifact_type']} artifact {row['artifact_id']} has "
            f"fiscal_period_key={row['fiscal_period_key']}, expected {expected_key}."
        )

    if reasons:
        return "HARD_FAIL", reasons

    if not mda_chunks:
        reasons.append("No period-aligned MD&A chunk found.")
    if not press_release_chunks:
        reasons.append("No period-aligned earnings-release chunk found.")

    if reasons:
        return "SOFT_FAIL", reasons
    return "PASS", ["FMP facts, MD&A chunks, and earnings-release chunks align to the requested fiscal year."]


def _print_artifact_spine(artifacts: list[dict[str, Any]], *, expected_key: str) -> None:
    print("SEC Period Rows For Fiscal Year")
    if not artifacts:
        print("  none")
        return
    for row in artifacts:
        if row["fiscal_period_key"] == expected_key:
            marker = "JOIN"
        elif row["period_type"] == "annual":
            marker = "CHECK"
        else:
            marker = "NEARBY"
        print(
            f"  {marker} artifact_id={row['artifact_id']} "
            f"type={row['artifact_type']} period_type={row['period_type']} "
            f"key={row['fiscal_period_key']} "
            f"label={row['fiscal_period_label']} period_end={_fmt_date(row['period_end'])} "
            f"published_at={_fmt_date(row['published_at'])}"
        )
        source_id = row["accession_number"] or row["source_document_id"]
        if source_id:
            print(f"      source_id={source_id}")


def _print_chunks(title: str, chunks: list[dict[str, Any]]) -> None:
    print(title)
    if not chunks:
        print("  none")
        return
    for row in chunks:
        print(
            f"  artifact_id={row['artifact_id']} chunk_id={row['chunk_id']} "
            f"chunk_ordinal={row['chunk_ordinal']} "
            f"key={row['fiscal_period_key']} published_at={_fmt_date(row['published_at'])}"
        )
        source_id = row["accession_number"] or row["source_document_id"]
        if source_id:
            print(f"  source_id={source_id}")
        label = row.get("section_key") or row.get("unit_key")
        title_value = row.get("section_title") or row.get("unit_title")
        print(f"  unit={label} | {title_value}")
        heading_path = row.get("heading_path") or []
        if heading_path:
            print(f"  heading_path={' > '.join(heading_path)}")
        print(f"  snippet={_clean_text(row['text'])}")
        print()


def _render_packet(
    *,
    company: dict[str, Any],
    fiscal_year: int,
    expected_key: str,
    metrics: dict[str, Any] | None,
    artifacts: list[dict[str, Any]],
    mda_chunks: list[dict[str, Any]],
    press_release_chunks: list[dict[str, Any]],
    status: str,
    status_reasons: list[str],
) -> None:
    print(f"{company['ticker']} FY{fiscal_year} Company Context Packet")
    print("=" * 72)
    print()
    print("Company")
    print(f"  company_id={company['id']}")
    print(f"  ticker={company['ticker']}")
    print(f"  name={company['name']}")
    print(f"  cik={company['cik']}")
    print(f"  fiscal_year_end_md={company['fiscal_year_end_md']}")
    print()

    print("Period Spine")
    print(f"  requested_fiscal_year={fiscal_year}")
    print(f"  expected_fiscal_period_key={expected_key}")
    if metrics is None:
        print("  fmp_fiscal_period_label=NA")
        print("  fmp_fy_end=NA")
    else:
        print(f"  fmp_fiscal_period_label={metrics['fiscal_period_label']}")
        print(f"  fmp_fy_end={_fmt_date(metrics['fy_end'])}")
    print()

    print("Financial Facts")
    if metrics is None:
        print("  none")
    else:
        print(f"  revenue={_fmt_money(metrics['revenue_fy'])}")
        print(f"  gross_margin={_fmt_pct(metrics['gross_margin_fy'])}")
        print(f"  operating_margin={_fmt_pct(metrics['operating_margin_fy'])}")
        print(f"  cfo={_fmt_money(metrics['cfo_fy'])}")
        print(f"  capex={_fmt_money(metrics['capital_expenditures_fy'])}")
        print(f"  fcf_signed_cfo_plus_capex={_fmt_money(metrics['fcf_fy'])}")
    print()

    _print_artifact_spine(artifacts, expected_key=expected_key)
    print()
    _print_chunks("MD&A Evidence", mda_chunks)
    _print_chunks("Earnings-Release Evidence", press_release_chunks)

    print("Integrity Result")
    print(f"  status={status}")
    for reason in status_reasons:
        print(f"  - {reason}")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Print a read-only company-period context packet."
    )
    parser.add_argument("ticker", help="Company ticker, e.g. PLTR")
    parser.add_argument(
        "--fiscal-year",
        type=int,
        required=True,
        help="Fiscal year to test, e.g. 2024",
    )
    parser.add_argument(
        "--limit-chunks",
        type=int,
        default=2,
        help="Maximum chunks to print per evidence type.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    ticker = args.ticker.upper()
    expected_key = _expected_fiscal_period_key(args.fiscal_year)

    with get_conn() as conn:
        company = _fetch_company(conn, ticker)
        if company is None:
            print(f"No company found for ticker {ticker}.", file=sys.stderr)
            return 1

        metrics = _fetch_fy_metrics(
            conn, company_id=company["id"], fiscal_year=args.fiscal_year
        )
        artifacts = _fetch_artifact_periods(
            conn,
            company_id=company["id"],
            ticker=ticker,
            fiscal_year=args.fiscal_year,
        )
        mda_chunks = _fetch_mda_chunks(
            conn,
            company_id=company["id"],
            fiscal_period_key=expected_key,
            limit=args.limit_chunks,
        )
        press_release_chunks = _fetch_press_release_chunks(
            conn,
            company_id=company["id"],
            ticker=ticker,
            fiscal_period_key=expected_key,
            fiscal_year=args.fiscal_year,
            fy_end=None if metrics is None else metrics["fy_end"],
            limit=args.limit_chunks,
        )

    mismatches = _period_mismatches(artifacts, expected_key=expected_key)
    status, status_reasons = _integrity_status(
        metrics=metrics,
        expected_key=expected_key,
        period_mismatches=mismatches,
        mda_chunks=mda_chunks,
        press_release_chunks=press_release_chunks,
    )
    _render_packet(
        company=company,
        fiscal_year=args.fiscal_year,
        expected_key=expected_key,
        metrics=metrics,
        artifacts=artifacts,
        mda_chunks=mda_chunks,
        press_release_chunks=press_release_chunks,
        status=status,
        status_reasons=status_reasons,
    )
    return 0 if status != "HARD_FAIL" else 2


if __name__ == "__main__":
    raise SystemExit(main())
