"""Unit tests for arrow.ingest.audio.q4inc — URL parser + manual fallback validator."""

from __future__ import annotations

import pytest

from arrow.ingest.audio.q4inc import (
    accept_pasted_url,
    attendee_url,
    parse_q4_audio_url,
)


# -- attendee_url ----------------------------------------------------------

def test_attendee_url_format():
    assert attendee_url("658779279") == "https://events.q4inc.com/attendee/658779279"


# -- parse_q4_audio_url ----------------------------------------------------

def test_parse_q4_audio_url_canonical():
    """The canonical Q4 edited-recording URL pattern must parse cleanly."""
    url = (
        "https://static.events.q4inc.com/edited-recordings/658779279/"
        "9149fbc1-b0db-476f-8e08-ef3f4eff300e.mp4"
    )
    parsed = parse_q4_audio_url(url)
    assert parsed is not None
    event_id, uuid = parsed
    assert event_id == "658779279"
    assert uuid == "9149fbc1-b0db-476f-8e08-ef3f4eff300e"


def test_parse_q4_audio_url_case_insensitive():
    """UUIDs sometimes appear with mixed case; pattern must accept either."""
    url = (
        "https://static.events.q4inc.com/edited-recordings/123/"
        "ABCDEF12-3456-7890-ABCD-EF1234567890.mp4"
    )
    assert parse_q4_audio_url(url) is not None


def test_parse_q4_audio_url_rejects_unrelated():
    """Random non-Q4 URLs must not match — defensive against pasted YouTube/etc."""
    bad_urls = [
        "https://www.youtube.com/watch?v=abc123",
        "https://example.com/some.mp4",
        "https://static.events.q4inc.com/live-stream/123/abc.m3u8",
        "not-a-url",
        "",
    ]
    for url in bad_urls:
        assert parse_q4_audio_url(url) is None, f"Should not match: {url}"


# -- accept_pasted_url -----------------------------------------------------

def test_accept_pasted_url_returns_audio_ref():
    url = (
        "https://static.events.q4inc.com/edited-recordings/658779279/"
        "9149fbc1-b0db-476f-8e08-ef3f4eff300e.mp4"
    )
    ref = accept_pasted_url(url)
    assert ref.vendor == "q4inc"
    assert ref.event_id == "658779279"
    assert ref.source_uuid == "9149fbc1-b0db-476f-8e08-ef3f4eff300e"
    assert ref.discovered_via == "manual_paste"
    assert ref.source_url == url


def test_accept_pasted_url_rejects_non_q4():
    with pytest.raises(ValueError, match="does not match Q4"):
        accept_pasted_url("https://www.youtube.com/watch?v=abc")


def test_accept_pasted_url_event_id_mismatch():
    """If the operator pastes the wrong call's URL, we must catch it."""
    url = (
        "https://static.events.q4inc.com/edited-recordings/658779279/"
        "9149fbc1-b0db-476f-8e08-ef3f4eff300e.mp4"
    )
    with pytest.raises(ValueError, match="wrong call's URL"):
        accept_pasted_url(url, expected_event_id="111111111")


def test_accept_pasted_url_event_id_match_ok():
    url = (
        "https://static.events.q4inc.com/edited-recordings/658779279/"
        "9149fbc1-b0db-476f-8e08-ef3f4eff300e.mp4"
    )
    ref = accept_pasted_url(url, expected_event_id="658779279")
    assert ref.event_id == "658779279"
