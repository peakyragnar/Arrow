from __future__ import annotations

from datetime import date

from scripts.audit_sec_qualitative import (
    ExpectedFiling,
    _coverage_rows_from_stored,
    _chunk_warning_bucket,
    _expected_count_rows,
    _is_amendment_extraction_note,
    _is_hard_weak_filing,
    _missing_standard_sections_for_filing,
)


def test_amendments_do_not_require_full_standard_section_inventory() -> None:
    filing = {
        "artifact_type": "10k",
        "form_type": "10-K/A",
        "is_amendment": True,
    }

    assert _missing_standard_sections_for_filing(filing, {"item_7_mda": {}}) == []


def test_partial_amendment_with_repaired_section_is_note_not_hard_failure() -> None:
    filing = {
        "artifact_type": "10k",
        "form_type": "10-K/A",
        "is_amendment": True,
        "sections": 1,
        "fallbacks": 0,
        "repairs": 1,
        "min_confidence": 0.3,
    }

    assert not _is_hard_weak_filing(filing)
    assert _is_amendment_extraction_note(filing)


def test_base_filing_with_low_confidence_remains_hard_failure() -> None:
    filing = {
        "artifact_type": "10k",
        "form_type": "10-K",
        "is_amendment": False,
        "sections": 1,
        "fallbacks": 0,
        "repairs": 1,
        "min_confidence": 0.3,
    }

    assert _is_hard_weak_filing(filing)
    assert not _is_amendment_extraction_note(filing)


def test_coverage_counts_split_base_filings_from_amendments() -> None:
    stored = [
        {
            "artifact_type": "10k",
            "form_type": "10-K",
            "display_type": "10-K",
            "fiscal_year": 2025,
            "published_at": date(2026, 2, 3),
        },
        {
            "artifact_type": "10k",
            "form_type": "10-K/A",
            "display_type": "10-K/A",
            "fiscal_year": 2025,
            "published_at": date(2026, 2, 4),
        },
        {
            "artifact_type": "10q",
            "form_type": "10-Q",
            "display_type": "10-Q",
            "fiscal_year": 2025,
            "published_at": date(2025, 11, 4),
        },
    ]
    expected = [
        ExpectedFiling("base", "10k", "10-K", "FY2025", 2025, date(2026, 2, 3)),
        ExpectedFiling(
            "amendment",
            "10k",
            "10-K/A",
            "FY2025",
            2025,
            date(2026, 2, 4),
        ),
        ExpectedFiling(
            "quarter",
            "10q",
            "10-Q",
            "FY2025 Q3",
            2025,
            date(2025, 11, 4),
        ),
    ]

    coverage = {
        row["artifact_type"]: row["stored"]
        for row in _coverage_rows_from_stored(stored, expected)
    }
    expected_counts = {
        row["artifact_type"]: (row["expected"], row["stored"])
        for row in _expected_count_rows(stored, expected)
    }

    assert coverage["10-K"] == 1
    assert coverage["10-K/A"] == 1
    assert coverage["10-Q"] == 1
    assert expected_counts["10-K"] == (1, 1)
    assert expected_counts["10-K/A"] == (1, 1)
    assert expected_counts["10-Q/A"] == (0, 0)


def test_market_risk_cross_reference_is_short_valid_not_boundary_issue() -> None:
    row = {
        "section_key": "part1_item3_market_risk",
        "chars": 430,
        "heading_path": [
            "Part I Item 3. Quantitative and Qualitative Disclosures About Market Risk",
            "ITEM 3. QUANTITATIVE AND QUALITATIVE DISCLOSURES ABOUT MARKET RISK",
        ],
        "starts_with": "Reference is made to Part II, Item 7A, Quantitative and Qualitative Disclosures About Market Risk.",
        "ends_with": "There have not been any material changes in market risk.",
        "text": "Reference is made to Part II, Item 7A, Quantitative and Qualitative Disclosures About Market Risk. There have not been any material changes in market risk.",
    }

    bucket, reason = _chunk_warning_bucket(row)

    assert bucket == "short_valid_section"
    assert "SEC section type" in reason


def test_signature_chunk_remains_possible_boundary_issue() -> None:
    row = {
        "section_key": "part2_item1a_risk_factors",
        "chars": 445,
        "heading_path": ["Part II Item 1A. Risk Factors", "ITEM 6. EXHIBITS"],
        "starts_with": "61 Table of Contents SIGNATURE Pursuant to the requirements of the Securities Exchange Act of 1934.",
        "ends_with": "Signing on behalf of the Registrant as the Principal Financial Officer",
        "text": "61 Table of Contents SIGNATURE Pursuant to the requirements of the Securities Exchange Act of 1934. Signing on behalf of the Registrant as the Principal Financial Officer",
    }

    bucket, reason = _chunk_warning_bucket(row)

    assert bucket == "possible_boundary_issue"
    assert "boundary marker" in reason
