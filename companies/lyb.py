"""
LyondellBasell (LYB) specific extraction overrides.

Known quirks:
- MarketableSecuritiesCurrent is a footnote disclosure of the composition of
  CashAndCashEquivalentsAtCarryingValue, not a separate balance sheet line item.
  LYB states: "marketable securities classified as Cash and cash equivalents."
  The master script picks this up as short_term_investments_q, which double-counts
  since the amount is already included in cash_q. Override to zero.
- From FY2023 Q3 onward, AccountsReceivableNetCurrent and AccountsPayableTradeCurrent
  are tagged only under NonrelatedPartyMember dimension (no non-dimensioned total).
  10-Ks use AccountsPayableCurrentAndNoncurrent, also dimensioned. Need to extract
  from the dimensioned context.
- FY2025 Q4: 10-K's DepreciationDepletionAndAmortization includes $139M of
  impairments that the 10-Q 9M YTD does not. Override with pure D&A value.
"""

import json
import os
import xml.etree.ElementTree as ET
from extract import parse_xbrl, DATA_DIR


def _get_dimensioned_bs_value(filing_dir: str, meta: dict, concepts: list,
                               member: str) -> int | None:
    """Extract a balance sheet value from a specific dimension member context."""
    xbrl_path = os.path.join(filing_dir, meta["xbrl_filename"])
    if not os.path.exists(xbrl_path):
        return None

    contexts, facts, nsmap, _ = parse_xbrl(xbrl_path)
    tree = ET.parse(xbrl_path)
    root = tree.getroot()
    xbrli_ns = nsmap.get("", "http://www.xbrl.org/2003/instance")

    # Map context IDs to instant dates for contexts with the target member
    dim_contexts = {}
    for ctx in root.findall(f"{{{xbrli_ns}}}context"):
        ctx_id = ctx.get("id")
        period = ctx.find(f"{{{xbrli_ns}}}period")
        instant = period.find(f"{{{xbrli_ns}}}instant") if period is not None else None
        if instant is None:
            continue
        segment = ctx.find(f".//{{{xbrli_ns}}}segment")
        if segment is None:
            continue
        for dim_elem in segment:
            if dim_elem.text and member in dim_elem.text:
                dim_contexts[ctx_id] = instant.text

    if not dim_contexts:
        return None

    report_date = meta["report_date"]
    for ctx_id, inst_date in dim_contexts.items():
        if abs((int(inst_date[:4]) - int(report_date[:4])) * 400 +
               (int(inst_date[5:7]) - int(report_date[5:7])) * 31 +
               (int(inst_date[8:10]) - int(report_date[8:10]))) > 5:
            continue

        for concept in concepts:
            for elem in root:
                if concept in elem.tag and elem.get("contextRef") == ctx_id:
                    try:
                        return int(float(elem.text))
                    except (TypeError, ValueError):
                        pass

    return None


def post_process(record: dict, all_extractions: list) -> dict:
    """Zero out short_term_investments_q — it's a subset of cash, not a separate line."""
    record["short_term_investments_q"] = 0

    # Fix AR/AP from dimensioned contexts when master script returns 0
    _fix_dimensioned_bs(record)

    # FY2025 Q4: 10-K's DepreciationDepletionAndAmortization ($1,390M) includes
    # $139M of impairments that the 10-Q 9M YTD ($1,005M) does not. Override
    # with the pure D&A value from the filing.
    if record.get("fiscal_year") == 2025 and record.get("fiscal_period") == "Q4":
        record["dna_q"] = 246000000

    return record


def _fix_dimensioned_bs(record: dict):
    """Fix AR and AP for quarters where LYB only tags them with dimensions."""
    ticker_dir = os.path.join(DATA_DIR, "LYB")
    accession = record.get("accession")
    if not accession:
        return

    filing_dir = os.path.join(ticker_dir, accession)
    meta_path = os.path.join(filing_dir, "filing_meta.json")
    if not os.path.exists(meta_path):
        return

    with open(meta_path) as f:
        meta = json.load(f)

    member = "NonrelatedPartyMember"

    if record.get("accounts_receivable_q", 0) == 0:
        val = _get_dimensioned_bs_value(
            filing_dir, meta, ["AccountsReceivableNetCurrent"], member)
        if val is not None:
            record["accounts_receivable_q"] = val

    if record.get("accounts_payable_q", 0) == 0:
        val = _get_dimensioned_bs_value(
            filing_dir, meta,
            ["AccountsPayableTradeCurrent", "AccountsPayableCurrentAndNoncurrent",
             "AccountsPayableCurrent"], member)
        if val is not None:
            record["accounts_payable_q"] = val
