from __future__ import annotations

from arrow.ingest.fmp.transcript_parse import (
    canonicalize_transcript_content,
    parse_speaker_turns,
)


def test_parse_speaker_turns_extends_until_next_marker() -> None:
    content = (
        "Operator: Welcome to the call.\n"
        "Please stand by for questions.\n"
        "Jane Doe: Revenue grew because demand improved.\n"
        "We also expanded capacity.\n"
        "John Smith: Thank you. My first question is about margins.\n"
    )

    turns = parse_speaker_turns(content)

    assert [turn.speaker for turn in turns] == ["Operator", "Jane Doe", "John Smith"]
    assert turns[0].text == "Operator: Welcome to the call.\nPlease stand by for questions."
    assert turns[1].text.startswith("Jane Doe:")
    assert turns[1].start_offset == content.index("Jane Doe:")
    assert turns[2].end_offset == len(content.rstrip())


def test_parse_speaker_turns_returns_empty_when_coverage_too_low() -> None:
    content = (
        "This preamble is not speaker marked. " * 40
        + "\nOperator: One short marked line.\n"
    )

    assert parse_speaker_turns(content) == []


def test_parse_speaker_turns_returns_empty_without_markers() -> None:
    assert parse_speaker_turns("No speaker markers live in this content.") == []


def test_canonicalize_transcript_content_preserves_offsets_after_line_endings() -> None:
    content = "Operator: Hello\r\nJane Doe: Hi\x00"
    canonical = canonicalize_transcript_content(content)

    assert canonical == "Operator: Hello\nJane Doe: Hi"
    turns = parse_speaker_turns(canonical)
    assert [turn.start_offset for turn in turns] == [0, len("Operator: Hello\n")]
