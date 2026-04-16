"""
Filing fetcher: downloads 10-Q and 10-K filings from SEC EDGAR.

Downloads both the XBRL instance document (XML) and the iXBRL document (HTML)
for each filing. Stores them locally in data/filings/{TICKER}/{ACCESSION}/.

Usage:
    python3 fetch.py --cik 0001045810 --ticker NVDA
    python3 fetch.py --cik 0001045810 --ticker NVDA --fy-start 2024 --fy-end 2026
"""

import argparse
import json
import os
import time
import urllib.request
from datetime import date

USER_AGENT = "Arrow research@arrow.dev"
DATA_DIR = "data/filings"
SEC_RATE_LIMIT = 0.15  # seconds between requests (SEC asks for 10 req/sec max)
DEFAULT_FY_SPAN = 6  # fetch 6 fiscal years of filings


def sec_fetch(url: str) -> bytes:
    """Fetch a URL from SEC EDGAR with proper headers and rate limiting."""
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    time.sleep(SEC_RATE_LIMIT)
    with urllib.request.urlopen(req) as resp:
        return resp.read()


def get_filing_list(cik: str) -> dict:
    """Fetch the full submission history for a company."""
    cik_padded = cik.lstrip("0").zfill(10)
    url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    data = json.loads(sec_fetch(url))
    return data


def get_fy_end_month(submission_data: dict) -> int:
    """Extract fiscal year-end month from SEC submission data (e.g. '0129' -> 1)."""
    fy_end = submission_data.get("fiscalYearEnd", "")
    if len(fy_end) == 4:
        return int(fy_end[:2])
    return 12  # default to calendar year


def report_date_to_fy(report_date: str, fy_end_month: int) -> int:
    """Determine fiscal year from a filing's report date and the company's FY-end month.

    If the report date's month is after the FY-end month, the filing belongs
    to the next calendar year's fiscal year. Otherwise it belongs to the
    current calendar year's fiscal year.
    """
    year = int(report_date[:4])
    month = int(report_date[5:7])
    if month > fy_end_month:
        return year + 1
    return year


def compute_fy_range(fy_end_month: int, span: int = DEFAULT_FY_SPAN) -> tuple[int, int]:
    """Compute (fy_start, fy_end) from today's date and the company's FY-end month."""
    today = date.today()
    if today.month > fy_end_month:
        current_fy = today.year + 1
    else:
        current_fy = today.year
    return (current_fy - span + 1, current_fy)


def extract_filings(submission_data: dict, fy_start: int, fy_end: int,
                    fy_end_month: int) -> list:
    """
    Extract 10-Q and 10-K filing metadata from submission data.
    Filters to filings whose fiscal year falls within [fy_start, fy_end].
    """
    recent = submission_data.get("filings", {}).get("recent", {})
    if not recent:
        return []

    forms = recent.get("form", [])
    accessions = recent.get("accessionNumber", [])
    filing_dates = recent.get("filingDate", [])
    report_dates = recent.get("reportDate", [])
    primary_docs = recent.get("primaryDocument", [])

    filings = []
    for i in range(len(forms)):
        form = forms[i]
        if form not in ("10-Q", "10-K", "10-Q/A", "10-K/A"):
            continue

        filing = {
            "form": form,
            "accession": accessions[i],
            "filing_date": filing_dates[i],
            "report_date": report_dates[i],
            "primary_document": primary_docs[i],
        }

        fy = report_date_to_fy(filing["report_date"], fy_end_month)
        if fy < fy_start or fy > fy_end:
            continue

        filing["fiscal_year"] = fy
        filings.append(filing)

    return filings


def build_file_urls(cik: str, filing: dict) -> dict:
    """
    Build download URLs for a filing's XBRL and HTML documents.
    Returns dict with 'xbrl_url', 'html_url', and local filenames.
    """
    cik_num = cik.lstrip("0")
    accession_path = filing["accession"].replace("-", "")
    base_url = f"https://www.sec.gov/Archives/edgar/data/{cik_num}/{accession_path}"

    primary = filing["primary_document"]

    # The XBRL instance doc is typically {primary}_htm.xml
    # The primary doc is typically {ticker}-{date}.htm
    xbrl_name = primary.replace(".htm", "_htm.xml")
    base_name = primary.replace(".htm", "")

    return {
        "html_url": f"{base_url}/{primary}",
        "html_filename": primary,
        "xbrl_url": f"{base_url}/{xbrl_name}",
        "xbrl_filename": xbrl_name,
        "cal_url": f"{base_url}/{base_name}_cal.xml",
        "cal_filename": f"{base_name}_cal.xml",
        "pre_url": f"{base_url}/{base_name}_pre.xml",
        "pre_filename": f"{base_name}_pre.xml",
        "def_url": f"{base_url}/{base_name}_def.xml",
        "def_filename": f"{base_name}_def.xml",
    }


def download_filing(cik: str, ticker: str, filing: dict) -> str:
    """
    Download a single filing's XBRL and HTML documents.
    Returns the local directory path where files were saved.
    """
    accession = filing["accession"]
    filing_dir = os.path.join(DATA_DIR, ticker, accession)

    # Check if already downloaded
    meta_path = os.path.join(filing_dir, "filing_meta.json")
    if os.path.exists(meta_path):
        return filing_dir

    os.makedirs(filing_dir, exist_ok=True)

    urls = build_file_urls(cik, filing)

    # Download XBRL instance document
    xbrl_path = os.path.join(filing_dir, urls["xbrl_filename"])
    if not os.path.exists(xbrl_path):
        try:
            data = sec_fetch(urls["xbrl_url"])
            with open(xbrl_path, "wb") as f:
                f.write(data)
        except Exception as e:
            print(f"    Warning: could not download XBRL: {e}")

    # Download HTML (iXBRL) document
    html_path = os.path.join(filing_dir, urls["html_filename"])
    if not os.path.exists(html_path):
        try:
            data = sec_fetch(urls["html_url"])
            with open(html_path, "wb") as f:
                f.write(data)
        except Exception as e:
            print(f"    Warning: could not download HTML: {e}")

    # Download XBRL linkbase files (calculation, presentation, definition)
    for lb in ("cal", "pre", "def"):
        lb_path = os.path.join(filing_dir, urls[f"{lb}_filename"])
        if not os.path.exists(lb_path):
            try:
                data = sec_fetch(urls[f"{lb}_url"])
                with open(lb_path, "wb") as f:
                    f.write(data)
            except Exception as e:
                print(f"    Warning: could not download {lb} linkbase: {e}")

    # Save metadata
    meta = {
        "cik": cik,
        "ticker": ticker,
        "form": filing["form"],
        "accession": accession,
        "filing_date": filing["filing_date"],
        "report_date": filing["report_date"],
        "primary_document": filing["primary_document"],
        "xbrl_filename": urls["xbrl_filename"],
        "html_filename": urls["html_filename"],
        "cal_filename": urls["cal_filename"],
        "pre_filename": urls["pre_filename"],
        "def_filename": urls["def_filename"],
    }
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)

    return filing_dir


def fetch_company(cik: str, ticker: str, fy_start: int = None, fy_end: int = None):
    """Download all 10-Q and 10-K filings for a company."""
    print(f"Fetching submission history for {ticker} (CIK {cik})...")
    submission_data = get_filing_list(cik)

    company_name = submission_data.get("name", ticker)
    fy_end_month = get_fy_end_month(submission_data)
    print(f"Company: {company_name}")
    print(f"Fiscal year ends: month {fy_end_month}")

    if fy_start is None or fy_end is None:
        auto_start, auto_end = compute_fy_range(fy_end_month)
        fy_start = fy_start or auto_start
        fy_end = fy_end or auto_end

    print(f"Fiscal year range: FY{fy_start} – FY{fy_end}")

    filings = extract_filings(submission_data, fy_start, fy_end, fy_end_month)

    # Report what we expect vs what we found
    expected_10k = fy_end - fy_start + 1
    expected_10q = expected_10k * 3
    actual_10k = sum(1 for f in filings if f["form"] == "10-K")
    actual_10q = sum(1 for f in filings if f["form"] == "10-Q")
    print(f"Found {len(filings)} filings ({actual_10k} 10-K, {actual_10q} 10-Q)")
    if actual_10k < expected_10k:
        print(f"  Note: expected {expected_10k} 10-Ks, got {actual_10k} (FY not yet complete?)")
    if actual_10q < expected_10q:
        print(f"  Note: expected {expected_10q} 10-Qs, got {actual_10q} (FY not yet complete?)")

    for filing in filings:
        label = f"FY{filing['fiscal_year']} {filing['form']} {filing['report_date']} ({filing['accession']})"
        print(f"  Downloading {label}...")
        download_filing(cik, ticker, filing)

    print(f"\nDone. Files saved to {DATA_DIR}/{ticker}/")


def main():
    parser = argparse.ArgumentParser(description="Download SEC filings")
    parser.add_argument("--cik", required=True, help="SEC CIK number")
    parser.add_argument("--ticker", required=True, help="Stock ticker")
    parser.add_argument("--fy-start", type=int, help="Start fiscal year (inclusive, default: auto from today)")
    parser.add_argument("--fy-end", type=int, help="End fiscal year (inclusive, default: auto from today)")
    args = parser.parse_args()

    fetch_company(args.cik, args.ticker, args.fy_start, args.fy_end)


if __name__ == "__main__":
    main()
