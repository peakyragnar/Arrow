"""SEC qualitative extraction: section detection and standardized chunking."""

from __future__ import annotations

import html
import re
import unicodedata
from dataclasses import dataclass
from typing import Literal

import psycopg

EXTRACTOR_VERSION = "sec_sections_v2"
CHUNKER_VERSION = "sec_chunks_v5"

ExtractionMethod = Literal["deterministic", "repair", "unparsed_fallback"]
FormFamily = Literal["10-K", "10-Q"]

_MAX_WORDS = 1800
_TARGET_MIN_WORDS = 1000
_TARGET_MAX_WORDS = 1500

_BLOCK_TAG_PATTERN = re.compile(
    r"(?is)</?(?:p|div|tr|table|section|article|header|footer|li|ul|ol|h[1-6])[^>]*>"
)
_BR_PATTERN = re.compile(r"(?is)<br\s*/?>")
_SCRIPT_STYLE_PATTERN = re.compile(r"(?is)<(script|style)[^>]*>.*?</\1>")
_COMMENT_PATTERN = re.compile(r"(?is)<!--.*?-->")
_TAG_PATTERN = re.compile(r"(?is)<[^>]+>")
_WHITESPACE_PATTERN = re.compile(r"[ \t\f\v]+")
_SENTENCE_SPLIT_PATTERN = re.compile(r"(?<=[.!?])\s+")
_TOC_DOTS_PATTERN = re.compile(r"\.{2,}\s*\d+$")
_PART_HEADING_PATTERN = re.compile(r"(?i)^\s*part\s+(i|ii)\b")
_EMBEDDED_SUBHEADINGS = tuple(
    sorted(
        {
            "Acquisition Termination Cost",
            "Adoption of New and Recently Issued Accounting Pronouncements",
            "Capital Return to Shareholders",
            "Climate Change",
            "Concentration of Revenue",
            "Critical Accounting Policies and Estimates",
            "Critical Accounting Estimates",
            "Global Trade",
            "Gross Margin",
            "Gross Profit and Gross Margin",
            "License and Development Arrangements",
            "Liquidity and Capital Resources",
            "Market Platform Highlights",
            "Material Cash Requirements and Other Obligations",
            "Off-Balance Sheet Arrangements",
            "Operating Expenses",
            "Outstanding Indebtedness and Commercial Paper Program",
            "Outstanding Indebtedness and Commercial Paper",
            "Product Sales Revenue",
            "Product Transitions and New Product Introductions",
            "Recent Accounting Pronouncements",
            "Recently Issued Accounting Pronouncements",
            "Results of Operations",
        },
        key=len,
        reverse=True,
    )
)
_EMBEDDED_SUBHEADING_PATTERN = re.compile(
    r"\b(" + "|".join(re.escape(heading) for heading in _EMBEDDED_SUBHEADINGS) + r")\b"
)

_TEN_K_SECTIONS: list[tuple[str, str, str]] = [
    ("item_1_business", "Item 1", r"item\s+1(?:\s*[\.\-:]\s*|\s+)business\b"),
    ("item_1a_risk_factors", "Item 1A", r"item\s+1a(?:\s*[\.\-:]\s*|\s+)risk\s+factors\b"),
    ("item_1c_cybersecurity", "Item 1C", r"item\s+1c(?:\s*[\.\-:]\s*|\s+)cybersecurity\b"),
    ("item_3_legal_proceedings", "Item 3", r"item\s+3(?:\s*[\.\-:]\s*|\s+)legal\s+proceedings\b"),
    (
        "item_7_mda",
        "Item 7",
        r"item\s+7(?:\s*[\.\-:]\s*|\s+)management(?:['’]s)?\s+discussion\s+and\s+analysis\b",
    ),
    (
        "item_7a_market_risk",
        "Item 7A",
        r"item\s+7a(?:\s*[\.\-:]\s*|\s+)quantitative\s+and\s+qualitative\s+disclosures\s+about\s+market\s+risk\b",
    ),
    ("item_9a_controls", "Item 9A", r"item\s+9a(?:\s*[\.\-:]\s*|\s+)controls\s+and\s+procedures\b"),
    ("item_9b_other_information", "Item 9B", r"item\s+9b(?:\s*[\.\-:]\s*|\s+)other\s+information\b"),
]

_TEN_Q_SECTIONS: list[tuple[str, str, str, str]] = [
    (
        "part1_item2_mda",
        "Part I",
        "Item 2",
        r"item\s+2(?:\s*[\.\-:]\s*|\s+)management(?:['’]s)?\s+discussion\s+and\s+analysis\b",
    ),
    (
        "part1_item3_market_risk",
        "Part I",
        "Item 3",
        r"item\s+3(?:\s*[\.\-:]\s*|\s+)quantitative\s+and\s+qualitative\s+disclosures\s+about\s+market\s+risk\b",
    ),
    ("part1_item4_controls", "Part I", "Item 4", r"item\s+4(?:\s*[\.\-:]\s*|\s+)controls\s+and\s+procedures\b"),
    (
        "part2_item1_legal_proceedings",
        "Part II",
        "Item 1",
        r"item\s+1(?:\s*[\.\-:]\s*|\s+)legal\s+proceedings\b",
    ),
    (
        "part2_item1a_risk_factors",
        "Part II",
        "Item 1A",
        r"item\s+1a(?:\s*[\.\-:]\s*|\s+)risk\s+factors\b",
    ),
    (
        "part2_item5_other_information",
        "Part II",
        "Item 5",
        r"item\s+5(?:\s*[\.\-:]\s*|\s+)other\s+information\b",
    ),
]


@dataclass(frozen=True)
class SectionCandidate:
    key: str
    section_title: str
    part_label: str | None
    item_label: str
    line_index: int
    start_offset: int
    end_offset: int
    is_toc_like: bool


@dataclass(frozen=True)
class ExtractedSection:
    section_key: str
    section_title: str
    part_label: str | None
    item_label: str | None
    text: str
    start_offset: int
    end_offset: int
    confidence: float
    extraction_method: ExtractionMethod


@dataclass(frozen=True)
class ChunkRow:
    chunk_ordinal: int
    text: str
    search_text: str
    heading_path: list[str]
    start_offset: int
    end_offset: int


@dataclass(frozen=True)
class _Line:
    text: str
    start: int
    end: int


@dataclass(frozen=True)
class _SentenceUnit:
    text: str
    start_offset: int
    end_offset: int
    heading_path: list[str]
    is_heading: bool = False
    force_boundary: bool = False


@dataclass(frozen=True)
class _Block:
    text: str
    start: int
    end: int
    embedded_heading: bool = False


def normalize_filing_body(body: bytes, content_type: str | None) -> str:
    """Return normalized filing body text used for canonical hashes/extraction."""

    lower = (content_type or "").lower()
    text = body.decode("utf-8", errors="replace")
    if "html" in lower or lower.startswith("text/") or "xml" in lower:
        text = _COMMENT_PATTERN.sub(" ", text)
        text = _SCRIPT_STYLE_PATTERN.sub(" ", text)
        text = _BR_PATTERN.sub("\n", text)
        text = _BLOCK_TAG_PATTERN.sub("\n\n", text)
        text = _TAG_PATTERN.sub(" ", text)
    text = html.unescape(text)
    text = unicodedata.normalize("NFKC", text)
    raw_lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    normalized_lines: list[str] = []
    blank_run = 0
    for raw in raw_lines:
        line = _WHITESPACE_PATTERN.sub(" ", raw).strip()
        if not line:
            blank_run += 1
            if blank_run <= 1:
                normalized_lines.append("")
            continue
        blank_run = 0
        normalized_lines.append(line)
    normalized = "\n".join(normalized_lines).strip()
    return normalized


def search_text_from_text(text: str) -> str:
    normalized = unicodedata.normalize("NFKC", html.unescape(text))
    normalized = normalized.lower()
    normalized = _WHITESPACE_PATTERN.sub(" ", normalized).strip()
    return normalized


def extract_sections(form_family: FormFamily, normalized_body: str) -> list[ExtractedSection]:
    lines = _indexed_lines(normalized_body)
    if not lines:
        return [
            ExtractedSection(
                section_key="unparsed_body",
                section_title="Unparsed Body",
                part_label=None,
                item_label=None,
                text=normalized_body,
                start_offset=0,
                end_offset=len(normalized_body),
                confidence=0.0,
                extraction_method="unparsed_fallback",
            )
        ]

    candidates = _collect_candidates(form_family, lines, normalized_body)
    primary = _materialize_sections(
        form_family,
        lines=lines,
        body=normalized_body,
        selected=_select_candidates(candidates, form_family=form_family, strategy="forward"),
        strategy="deterministic",
    )
    if primary and min(section.confidence for section in primary) >= 0.85:
        return primary

    repair = _materialize_sections(
        form_family,
        lines=lines,
        body=normalized_body,
        selected=_select_candidates(candidates, form_family=form_family, strategy="reverse"),
        strategy="repair",
    )
    if repair:
        return repair

    return [
        ExtractedSection(
            section_key="unparsed_body",
            section_title="Unparsed Body",
            part_label=None,
            item_label=None,
            text=normalized_body,
            start_offset=0,
            end_offset=len(normalized_body),
            confidence=0.0,
            extraction_method="unparsed_fallback",
        )
    ]


def build_chunks(section: ExtractedSection) -> list[ChunkRow]:
    units = _section_units(section)
    if not units:
        return [
            ChunkRow(
                chunk_ordinal=1,
                text=section.text,
                search_text=search_text_from_text(section.text),
                heading_path=[section.section_title],
                start_offset=section.start_offset,
                end_offset=section.end_offset,
            )
        ]

    chunks: list[ChunkRow] = []
    current: list[_SentenceUnit] = []
    current_words = 0
    overlap_units: list[_SentenceUnit] = []
    ordinal = 1
    idx = 0

    while idx < len(units):
        unit = units[idx]
        unit_words = _word_count(unit.text)
        next_is_heading = idx + 1 < len(units) and units[idx + 1].is_heading

        if current and unit.is_heading and unit.force_boundary and _has_content_units(current):
            chunks.append(_emit_chunk(section, ordinal, current))
            ordinal += 1
            current = []
            current_words = 0
            overlap_units = []

        if current:
            would_exceed = current_words + unit_words > _TARGET_MAX_WORDS
            at_structure_boundary = current_words >= _TARGET_MIN_WORDS and unit.is_heading
            if would_exceed or at_structure_boundary:
                chunks.append(_emit_chunk(section, ordinal, current))
                ordinal += 1
                overlap_units = _sentence_overlap(current)
                current = list(overlap_units)
                current_words = sum(_word_count(item.text) for item in current)
                continue

        current.append(unit)
        current_words += unit_words

        if current_words >= _TARGET_MAX_WORDS or (
            current_words >= _TARGET_MIN_WORDS and next_is_heading
        ):
            chunks.append(_emit_chunk(section, ordinal, current))
            ordinal += 1
            overlap_units = _sentence_overlap(current)
            current = list(overlap_units)
            current_words = sum(_word_count(item.text) for item in current)
        idx += 1

    if current:
        if chunks and current == overlap_units:
            return chunks
        chunks.append(_emit_chunk(section, ordinal, current))
    return chunks


def replace_sections_and_chunks(
    conn: psycopg.Connection,
    *,
    artifact_id: int,
    company_id: int,
    fiscal_period_key: str,
    form_family: FormFamily,
    sections: list[ExtractedSection],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM artifact_section_chunks
            WHERE section_id IN (
                SELECT id FROM artifact_sections WHERE artifact_id = %s
            );
            """,
            (artifact_id,),
        )
        cur.execute("DELETE FROM artifact_sections WHERE artifact_id = %s;", (artifact_id,))

        for section in sections:
            cur.execute(
                """
                INSERT INTO artifact_sections (
                    artifact_id, company_id, fiscal_period_key, form_family,
                    section_key, section_title, part_label, item_label,
                    text, start_offset, end_offset,
                    extractor_version, confidence, extraction_method
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s
                )
                RETURNING id;
                """,
                (
                    artifact_id,
                    company_id,
                    fiscal_period_key,
                    form_family,
                    section.section_key,
                    section.section_title,
                    section.part_label,
                    section.item_label,
                    section.text,
                    section.start_offset,
                    section.end_offset,
                    EXTRACTOR_VERSION,
                    section.confidence,
                    section.extraction_method,
                ),
            )
            section_id = cur.fetchone()[0]
            for chunk in build_chunks(section):
                cur.execute(
                    """
                    INSERT INTO artifact_section_chunks (
                        section_id, chunk_ordinal, text, search_text,
                        heading_path, start_offset, end_offset, chunker_version
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        section_id,
                        chunk.chunk_ordinal,
                        chunk.text,
                        chunk.search_text,
                        chunk.heading_path,
                        chunk.start_offset,
                        chunk.end_offset,
                        CHUNKER_VERSION,
                    ),
                )


def find_amends_artifact_id(
    conn: psycopg.Connection,
    *,
    company_id: int,
    fiscal_period_key: str | None,
    form_family: str | None,
    published_at,
) -> int | None:
    if fiscal_period_key is None or form_family is None:
        return None
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id
            FROM artifacts
            WHERE company_id = %s
              AND fiscal_period_key = %s
              AND form_family = %s
              AND published_at < %s
            ORDER BY published_at DESC, id DESC
            LIMIT 1;
            """,
            (company_id, fiscal_period_key, form_family, published_at),
        )
        row = cur.fetchone()
    return None if row is None else row[0]


def _indexed_lines(body: str) -> list[_Line]:
    if not body:
        return []
    out: list[_Line] = []
    cursor = 0
    for raw_line in body.split("\n"):
        start = cursor
        end = start + len(raw_line)
        if raw_line:
            out.append(_Line(text=raw_line, start=start, end=end))
        cursor = end + 1
    return out


def _collect_candidates(
    form_family: FormFamily, lines: list[_Line], body: str
) -> dict[str, list[SectionCandidate]]:
    if form_family == "10-K":
        spec = _TEN_K_SECTIONS
        patterns = [
            (
                key,
                re.compile(rf"(?i)^\s*(?:part\s+[ivx]+\s*[-:]\s*)?{pattern}"),
                None,
                item_label,
            )
            for key, item_label, pattern in spec
        ]
    else:
        patterns = [
            (
                key,
                re.compile(rf"(?i)^\s*(?:part\s+(?:i|ii)\s*[-:]\s*)?{pattern}"),
                part_label,
                item_label,
            )
            for key, part_label, item_label, pattern in _TEN_Q_SECTIONS
        ]

    candidates: dict[str, list[SectionCandidate]] = {}
    current_part: str | None = None
    for idx, line in enumerate(lines):
        inline_part = _extract_part_heading(line.text)
        if inline_part is not None:
            current_part = inline_part
        for key, compiled, expected_part, item_label in patterns:
            if not compiled.search(line.text):
                continue
            actual_part = inline_part or current_part
            if form_family == "10-Q" and expected_part != actual_part:
                continue
            candidates.setdefault(key, []).append(
                SectionCandidate(
                    key=key,
                    section_title=_section_title_for_key(key),
                    part_label=expected_part,
                    item_label=item_label,
                    line_index=idx,
                    start_offset=line.start,
                    end_offset=line.end,
                    is_toc_like=_is_toc_like(lines, idx, body),
                )
            )
    return candidates


def _select_candidates(
    candidates: dict[str, list[SectionCandidate]],
    *,
    form_family: FormFamily,
    strategy: Literal["forward", "reverse"],
) -> list[SectionCandidate]:
    ordered_keys = [
        spec[0] for spec in (_TEN_K_SECTIONS if form_family == "10-K" else _TEN_Q_SECTIONS)
    ]
    selected: list[SectionCandidate] = []
    if strategy == "forward":
        last_idx = -1
        for key in ordered_keys:
            options = [c for c in candidates.get(key, []) if c.line_index > last_idx]
            if not options:
                continue
            preferred = [c for c in options if not c.is_toc_like]
            chosen = preferred[0] if preferred else options[0]
            selected.append(chosen)
            last_idx = chosen.line_index
        return selected

    next_idx = 10**9
    reverse_selected: list[SectionCandidate] = []
    for key in reversed(ordered_keys):
        options = [c for c in candidates.get(key, []) if c.line_index < next_idx]
        if not options:
            continue
        preferred = [c for c in options if not c.is_toc_like]
        chosen = preferred[-1] if preferred else options[-1]
        reverse_selected.append(chosen)
        next_idx = chosen.line_index
    return list(reversed(reverse_selected))


def _materialize_sections(
    form_family: FormFamily,
    *,
    lines: list[_Line],
    body: str,
    selected: list[SectionCandidate],
    strategy: ExtractionMethod,
) -> list[ExtractedSection]:
    if not selected:
        return []
    ordered = sorted(selected, key=lambda candidate: candidate.start_offset)
    out: list[ExtractedSection] = []
    for idx, candidate in enumerate(ordered):
        start = candidate.start_offset
        end = ordered[idx + 1].start_offset if idx + 1 < len(ordered) else len(body)
        if idx + 1 < len(ordered) and form_family == "10-Q":
            next_candidate = ordered[idx + 1]
            if candidate.part_label != next_candidate.part_label:
                for probe in lines[candidate.line_index + 1 : next_candidate.line_index + 1]:
                    if _extract_part_heading(probe.text) == next_candidate.part_label:
                        end = probe.start
                        break
        text = body[start:end].strip()
        if not text:
            continue
        confidence = _confidence_for_section(
            body=body,
            candidate=candidate,
            selected=selected,
            text=text,
            strategy=strategy,
        )
        method: ExtractionMethod
        if strategy == "deterministic" and confidence >= 0.85:
            method = "deterministic"
        else:
            method = "repair"
            confidence = min(confidence, 0.84)
            confidence = max(confidence, 0.01)
        out.append(
            ExtractedSection(
                section_key=candidate.key,
                section_title=candidate.section_title,
                part_label=candidate.part_label,
                item_label=candidate.item_label,
                text=text,
                start_offset=start,
                end_offset=end,
                confidence=confidence,
                extraction_method=method,
            )
        )
    return out


def _confidence_for_section(
    *,
    body: str,
    candidate: SectionCandidate,
    selected: list[SectionCandidate],
    text: str,
    strategy: ExtractionMethod,
) -> float:
    score = 1.0 if strategy == "deterministic" else 0.8
    if candidate.is_toc_like:
        score -= 0.35
    dup_count = sum(1 for c in selected if c.key == candidate.key)
    if dup_count > 1:
        score -= 0.1
    if candidate.start_offset < int(len(body) * 0.05):
        score -= 0.15
    if _word_count(text) < 15:
        score -= 0.05
    if len(selected) < 2:
        score -= 0.15
    return round(max(0.0, min(1.0, score)), 3)


def _section_units(section: ExtractedSection) -> list[_SentenceUnit]:
    blocks = _paragraph_blocks(section.text, base_offset=section.start_offset)
    units: list[_SentenceUnit] = []
    heading_stack = [section.section_title]
    for block in blocks:
        if _is_subheading(block.text):
            heading_stack = [section.section_title, block.text]
            units.append(
                _SentenceUnit(
                    text=block.text,
                    start_offset=block.start,
                    end_offset=block.end,
                    heading_path=list(heading_stack),
                    is_heading=True,
                    force_boundary=block.embedded_heading,
                )
            )
            continue
        for sentence_text, sent_start, sent_end in _sentence_units(block.text, block.start):
            units.append(
                _SentenceUnit(
                    text=sentence_text,
                    start_offset=sent_start,
                    end_offset=sent_end,
                    heading_path=list(heading_stack),
                )
            )
    return units


def _paragraph_blocks(text: str, *, base_offset: int) -> list[_Block]:
    blocks: list[_Block] = []
    pattern = re.compile(r"\n\s*\n")
    cursor = 0
    for match in pattern.finditer(text):
        block = text[cursor:match.start()].strip()
        if block:
            start = base_offset + cursor + text[cursor:match.start()].find(block)
            blocks.extend(_split_embedded_subheadings(block, base_offset=start))
        cursor = match.end()
    tail = text[cursor:].strip()
    if tail:
        start = base_offset + cursor + text[cursor:].find(tail)
        blocks.extend(_split_embedded_subheadings(tail, base_offset=start))
    return blocks


def _split_embedded_subheadings(text: str, *, base_offset: int) -> list[_Block]:
    pieces: list[_Block] = []
    cursor = 0
    for match in _EMBEDDED_SUBHEADING_PATTERN.finditer(text):
        if not _is_embedded_subheading_match(text, match):
            continue
        _append_block_piece(pieces, text[cursor : match.start()], base_offset + cursor)
        _append_block_piece(
            pieces,
            match.group(1),
            base_offset + match.start(),
            embedded_heading=True,
        )
        cursor = match.end()
    _append_block_piece(pieces, text[cursor:], base_offset + cursor)
    return pieces


def _append_block_piece(
    out: list[_Block], raw: str, start: int, *, embedded_heading: bool = False
) -> None:
    text = raw.strip()
    if not text:
        return
    relative_start = raw.find(text)
    absolute_start = start + relative_start
    out.append(
        _Block(
            text=text,
            start=absolute_start,
            end=absolute_start + len(text),
            embedded_heading=embedded_heading,
        )
    )


def _is_embedded_subheading_match(text: str, match: re.Match[str]) -> bool:
    before = text[: match.start()].rstrip()
    after = text[match.end() :].lstrip()
    if before and before[-1] not in ".!?":
        return False
    if not after:
        return True
    next_char = after[0]
    if not (next_char.isupper() or next_char.isdigit() or next_char in "$("):
        return False
    return True


def _sentence_units(text: str, start_offset: int) -> list[tuple[str, int, int]]:
    if not text:
        return []
    units: list[tuple[str, int, int]] = []
    cursor = 0
    for part in _SENTENCE_SPLIT_PATTERN.split(text):
        sentence = part.strip()
        if not sentence:
            cursor += len(part)
            continue
        relative = text.find(sentence, cursor)
        if relative == -1:
            relative = cursor
        sent_start = start_offset + relative
        sent_end = sent_start + len(sentence)
        units.append((sentence, sent_start, sent_end))
        cursor = relative + len(sentence)
    return units or [(text, start_offset, start_offset + len(text))]


def _sentence_overlap(units: list[_SentenceUnit]) -> list[_SentenceUnit]:
    content_units = [unit for unit in units if not unit.is_heading]
    if len(content_units) <= 1:
        return content_units[-1:] if content_units else []
    chunk_words = sum(_word_count(unit.text) for unit in content_units)
    overlap = content_units[-2:]
    overlap_words = sum(_word_count(unit.text) for unit in overlap)
    if chunk_words and overlap_words / chunk_words > 0.18:
        return overlap[-1:]
    return overlap


def _has_content_units(units: list[_SentenceUnit]) -> bool:
    return any(not unit.is_heading for unit in units)


def _emit_chunk(section: ExtractedSection, ordinal: int, units: list[_SentenceUnit]) -> ChunkRow:
    text = " ".join(unit.text for unit in units).strip()
    first_content = next((unit for unit in units if not unit.is_heading), units[0])
    return ChunkRow(
        chunk_ordinal=ordinal,
        text=text,
        search_text=search_text_from_text(text),
        heading_path=list(first_content.heading_path),
        start_offset=units[0].start_offset,
        end_offset=units[-1].end_offset,
    )


def _word_count(text: str) -> int:
    return len(re.findall(r"\b\w+\b", text))


def _extract_part_heading(line: str) -> str | None:
    text = line.strip()
    if len(text) > 160 or _word_count(text) > 14:
        return None
    match = _PART_HEADING_PATTERN.match(text)
    if match is None:
        return None
    roman = match.group(1).upper()
    if roman == "I":
        return "Part I"
    if roman == "II":
        return "Part II"
    return None


def _is_toc_like(lines: list[_Line], index: int, body: str) -> bool:
    line = lines[index].text
    if _TOC_DOTS_PATTERN.search(line):
        return True
    if lines[index].start > int(len(body) * 0.15):
        return False
    nearby_headings = 0
    for probe in lines[index : min(len(lines), index + 6)]:
        if re.search(r"(?i)\bitem\s+\d+[a-z]?\b", probe.text):
            nearby_headings += 1
    return nearby_headings >= 3


def _is_subheading(text: str) -> bool:
    if not text or _word_count(text) > 12 or len(text) > 120:
        return False
    if text.endswith((".", "?", "!", ";")):
        return False
    words = re.findall(r"[A-Za-z][A-Za-z&/\-']*", text)
    if not words:
        return False
    title_case = sum(1 for word in words if word[:1].isupper())
    return title_case >= max(1, len(words) // 2)


def _section_title_for_key(section_key: str) -> str:
    mapping = {
        "item_1_business": "Item 1. Business",
        "item_1a_risk_factors": "Item 1A. Risk Factors",
        "item_1c_cybersecurity": "Item 1C. Cybersecurity",
        "item_3_legal_proceedings": "Item 3. Legal Proceedings",
        "item_7_mda": "Item 7. Management's Discussion and Analysis",
        "item_7a_market_risk": "Item 7A. Quantitative and Qualitative Disclosures About Market Risk",
        "item_9a_controls": "Item 9A. Controls and Procedures",
        "item_9b_other_information": "Item 9B. Other Information",
        "part1_item2_mda": "Part I Item 2. Management's Discussion and Analysis",
        "part1_item3_market_risk": "Part I Item 3. Quantitative and Qualitative Disclosures About Market Risk",
        "part1_item4_controls": "Part I Item 4. Controls and Procedures",
        "part2_item1_legal_proceedings": "Part II Item 1. Legal Proceedings",
        "part2_item1a_risk_factors": "Part II Item 1A. Risk Factors",
        "part2_item5_other_information": "Part II Item 5. Other Information",
    }
    return mapping[section_key]
