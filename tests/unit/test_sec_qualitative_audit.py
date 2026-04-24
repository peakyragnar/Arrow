from __future__ import annotations

from scripts.audit_sec_qualitative import (
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
