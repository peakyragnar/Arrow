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

USER_AGENT = "Arrow research@arrow.dev"
DATA_DIR = "data/filings"
SEC_RATE_LIMIT = 0.15  # seconds between requests (SEC asks for 10 req/sec max)


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


def extract_filings(submission_data: dict, fy_start: int = None, fy_end: int = None) -> list:
    """
    Extract 10-Q and 10-K filing metadata from submission data.
    Returns list of dicts with accession, form, reportDate, filingDate, primaryDocument.
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
        if form not in ("10-Q", "10-K"):
            continue

        filing = {
            "form": form,
            "accession": accessions[i],
            "filing_date": filing_dates[i],
            "report_date": report_dates[i],
            "primary_document": primary_docs[i],
        }

        # Filter by fiscal year if specified
        # report_date is the period end date; use year as rough FY filter
        if fy_start or fy_end:
            year = int(filing["report_date"][:4])
            if fy_start and year < fy_start - 1:
                continue
            if fy_end and year > fy_end + 1:
                continue

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

    return {
        "html_url": f"{base_url}/{primary}",
        "html_filename": primary,
        "xbrl_url": f"{base_url}/{xbrl_name}",
        "xbrl_filename": xbrl_name,
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
    }
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)

    return filing_dir


def fetch_company(cik: str, ticker: str, fy_start: int = None, fy_end: int = None):
    """Download all 10-Q and 10-K filings for a company."""
    print(f"Fetching submission history for {ticker} (CIK {cik})...")
    submission_data = get_filing_list(cik)

    company_name = submission_data.get("name", ticker)
    print(f"Company: {company_name}")

    filings = extract_filings(submission_data, fy_start, fy_end)
    print(f"Found {len(filings)} 10-Q/10-K filings")

    for filing in filings:
        label = f"{filing['form']} {filing['report_date']} ({filing['accession']})"
        print(f"  Downloading {label}...")
        download_filing(cik, ticker, filing)

    print(f"\nDone. Files saved to {DATA_DIR}/{ticker}/")


def main():
    parser = argparse.ArgumentParser(description="Download SEC filings")
    parser.add_argument("--cik", required=True, help="SEC CIK number")
    parser.add_argument("--ticker", required=True, help="Stock ticker")
    parser.add_argument("--fy-start", type=int, help="Start fiscal year (inclusive)")
    parser.add_argument("--fy-end", type=int, help="End fiscal year (inclusive)")
    args = parser.parse_args()

    fetch_company(args.cik, args.ticker, args.fy_start, args.fy_end)


if __name__ == "__main__":
    main()
