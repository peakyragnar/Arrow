"""SEC qualitative extraction: section detection and standardized chunking."""

from __future__ import annotations

import html
import re
import unicodedata
from dataclasses import dataclass
from typing import Literal

import psycopg

EXTRACTOR_VERSION = "sec_sections_v10"
CHUNKER_VERSION = "sec_chunks_v10"
TEXT_UNIT_EXTRACTOR_VERSION = "press_release_units_v4"
TEXT_CHUNKER_VERSION = "artifact_text_chunks_v1"

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
_TOC_SUFFIX_PATTERN = re.compile(r"(?i)\s+table\s+of\s+conten\s*t\s*s\s*$")
_PART_HEADING_PATTERN = re.compile(r"(?i)^\s*part\s+(i|ii)\b")
_TEN_Q_ITEM_HEADING_PATTERN = re.compile(
    r"(?i)^\s*(?:part\s+(?P<part>i|ii)\s*[\.\-:]*\s*)?"
    r"item\s+(?P<item>\d+[a-z]?)(?:\s*[\.\-:]\s*|\s+)"
)
_TEN_K_ITEM_HEADING_PATTERN = re.compile(
    r"(?i)^\s*(?:part\s+[ivx]+\s*[\.\-:]*\s*)?"
    r"item\s+(?P<item>\d+[a-z]?)(?:\s*[\.\-:]\s*|\s+)"
)
_STANDALONE_MDA_HEADING_RE = re.compile(
    r"(?i)^management(?:['’]s)?\s+discussion\s+and\s+analysis"
    r"(?:\s+of\s+financial\s+condition\s+and\s+results\s+of\s+operations)?"
    r"(?:\s+\(md&a\))?$"
)
_STANDALONE_MARKET_RISK_HEADING_RE = re.compile(
    r"(?i)^quantitative\s+and\s+qualitative\s+disclosures\s+about\s+market\s+risk$"
)
_STANDALONE_CONTROLS_HEADING_RE = re.compile(r"(?i)^controls\s+and\s+procedures$")
_STANDALONE_RISK_FACTORS_HEADING_RE = re.compile(r"(?i)^risk\s+factors$")
_STANDALONE_LEGAL_HEADING_RE = re.compile(r"(?i)^legal\s+proceedings$")
_STANDALONE_OTHER_INFO_HEADING_RE = re.compile(r"(?i)^other\s+information$")
_STANDALONE_BUSINESS_HEADING_RE = re.compile(r"(?i)^(?:our\s+business|business)$")
_FILING_FURNITURE_TAIL_RE = re.compile(
    r"(?i)\b("
    r"inline\s+xbrl\s+document|"
    r"management\s+contracts|"
    r"compensatory\s+plans|"
    r"exhibit\s+index|"
    r"signatures?|"
    r"signing\s+on\s+behalf|"
    r"principal\s+financial\s+officer|"
    r"table\s+of\s+contents|"
    r"see\s+accompanying\s+notes\s+to\s+(?:the\s+)?(?:condensed\s+)?consolidated\s+financial\s+statements"
    r")\b"
)
_TRAILING_FURNITURE_PAGE_NUMBER_RE = re.compile(r"^(?P<body>.+\.)\s+\d{1,3}$")
_TRAILING_SIGNATURE_PAGE_NUMBER_RE = re.compile(r"^(?P<body>.+?)\s+\d{1,3}$")
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
_PRESS_RELEASE_HEADLINE_TERMS_RE = re.compile(
    r"(?i)\b("
    r"announces?|reports?|results?|financial\s+results|"
    r"quarter|quarterly|fiscal|year[- ]end|full[- ]year|earnings"
    r")\b"
)
_PRESS_RELEASE_PREFIX_BOILERPLATE_RE = re.compile(
    r"(?ix)^("
    r"ex-?\s*99(?:\.\d+)?|"
    r"exhibit\s+99(?:\.\d+)?|"
    r"document|"
    r"news\s+release|"
    r"\d{1,4}|"
    r".*\.(?:htm|html|txt)|"
    r".*\b(?:blvd|boulevard|street|st\.|road|rd\.|avenue|ave\.|drive|dr\.|suite)\b\.?|"
    r"[A-Z][A-Za-z .'-]+,\s*[A-Z]{2}\s+\d{5}(?:-\d{4})?"
    r")$"
)
_PRESS_RELEASE_PAGE_FURNITURE_RE = re.compile(
    r"(?i)^[A-Z][A-Za-z0-9&.,' -]{1,80}/Page\s+\d+$"
)
_PRESS_RELEASE_UNIT_MARKERS: tuple[tuple[str, str, re.Pattern[str]], ...] = (
    ("news_summary", "News Summary", re.compile(r"(?i)^news\s+summary$")),
    (
        "financial_results",
        "Financial Results",
        re.compile(r"(?i)^(?:q[1-4]|first|second|third|fourth|full[- ]year).*\bfinancial\s+results$"),
    ),
    (
        "business_unit_summary",
        "Business Unit Summary",
        re.compile(r"(?i)^business\s+unit\s+summary$"),
    ),
    ("business_outlook", "Business Outlook", re.compile(r"(?i)^business\s+outlook$")),
    ("earnings_webcast", "Earnings Webcast", re.compile(r"(?i)^earnings\s+webcast$")),
    (
        "forward_looking_statements",
        "Forward-Looking Statements",
        re.compile(r"(?i)^forward[- ]looking\s+statements?$"),
    ),
    ("about_company", "About Company", re.compile(r"(?i)^about\s+[A-Z][A-Za-z0-9&.,' -]{1,80}$")),
    (
        "non_gaap_measures",
        "Explanation of Non-GAAP Measures",
        re.compile(r"(?i)^(?:explanation\s+of\s+)?non-gaap\s+(?:financial\s+)?measures$"),
    ),
    (
        "non_gaap_reconciliations",
        "Non-GAAP Reconciliations",
        re.compile(r"(?i).*\breconciliations?\b.*\b(?:gaap|non-gaap)\b.*"),
    ),
    (
        "financial_tables",
        "Financial Tables",
        re.compile(
            r"(?i)^("
            r"(?:consolidated\s+)?condensed\s+(?:consolidated\s+)?"
            r"(?:statements?|balance\s+sheets?|cash\s+flows?).*|"
            r"supplemental\s+operating\s+segment\s+results"
            r")$"
        ),
    ),
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
_TEN_Q_BOUNDARY_ITEMS = {
    "Part I": {"Item 1", "Item 2", "Item 3", "Item 4"},
    "Part II": {"Item 1", "Item 1A", "Item 2", "Item 3", "Item 4", "Item 5", "Item 6"},
}
_TEN_K_BOUNDARY_ITEMS = {
    "Item 1",
    "Item 1A",
    "Item 1B",
    "Item 1C",
    "Item 2",
    "Item 3",
    "Item 4",
    "Item 5",
    "Item 6",
    "Item 7",
    "Item 7A",
    "Item 8",
    "Item 9",
    "Item 9A",
    "Item 9B",
    "Item 9C",
    "Item 10",
    "Item 11",
    "Item 12",
    "Item 13",
    "Item 14",
    "Item 15",
    "Item 16",
}


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
class TextUnit:
    unit_ordinal: int
    unit_type: str
    unit_key: str
    unit_title: str
    text: str
    start_offset: int
    end_offset: int
    confidence: float
    extraction_method: ExtractionMethod


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
    text = body.decode("utf-8", errors="replace").replace("\x00", "")
    if "html" in lower or lower.startswith("text/") or "xml" in lower:
        text = _COMMENT_PATTERN.sub(" ", text)
        text = _SCRIPT_STYLE_PATTERN.sub(" ", text)
        text = _BR_PATTERN.sub("\n", text)
        text = _BLOCK_TAG_PATTERN.sub("\n\n", text)
        text = _TAG_PATTERN.sub(" ", text)
    text = html.unescape(text)
    text = unicodedata.normalize("NFKC", text)
    raw_lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    cleaned_lines = [_WHITESPACE_PATTERN.sub(" ", raw).strip() for raw in raw_lines]
    normalized_lines: list[str] = []
    blank_run = 0
    for idx, line in enumerate(cleaned_lines):
        if not line:
            blank_run += 1
            if blank_run <= 1:
                normalized_lines.append("")
            continue
        if _is_probable_running_header_line(cleaned_lines, idx):
            continue
        if _is_probable_page_number_line(cleaned_lines, idx):
            continue
        line = _strip_probable_filing_furniture_page_number(line)
        line = _strip_table_of_contents_marker(line)
        if not line:
            continue
        blank_run = 0
        normalized_lines.append(line)
    normalized = "\n".join(normalized_lines).strip()
    return normalized


def search_text_from_text(text: str) -> str:
    normalized = unicodedata.normalize("NFKC", html.unescape(text)).replace("\x00", "")
    normalized = normalized.lower()
    normalized = _WHITESPACE_PATTERN.sub(" ", normalized).strip()
    return normalized


def _is_probable_page_number_line(lines: list[str], index: int) -> bool:
    line = lines[index]
    if not re.fullmatch(r"\d{1,3}", line):
        return False
    previous_line = _nearest_nonblank_line(lines, index, step=-1)
    next_line = _nearest_nonblank_line(lines, index, step=1)
    if previous_line is None:
        return False
    if next_line is None:
        return bool(_FILING_FURNITURE_TAIL_RE.search(previous_line[-360:]))
    if _looks_like_heading(previous_line) and (_word_count(next_line) >= 8 or _looks_like_heading(next_line)):
        return True
    if not previous_line.endswith((".", "?", "!", ")", "]", "”", '"')):
        return False
    if _word_count(previous_line) < 8:
        return False
    return _looks_like_heading(next_line) or _word_count(next_line) >= 8


def _is_probable_running_header_line(lines: list[str], index: int) -> bool:
    line = lines[index]
    if len(line) > 140 or _word_count(line) > 10:
        return False
    next_line = _nearest_nonblank_line(lines, index, step=1)
    if next_line is None or not re.fullmatch(r"\d{1,3}", next_line):
        return False
    previous_line = _nearest_nonblank_line(lines, index, step=-1)
    if previous_line is None:
        return False
    if _TOC_DOTS_PATTERN.search(line):
        return False
    return _word_count(previous_line) >= 8 or previous_line.endswith((".", "?", "!", ")", "]", "”", '"'))


def _strip_probable_filing_furniture_page_number(line: str) -> str:
    match = _TRAILING_FURNITURE_PAGE_NUMBER_RE.match(line)
    if match is not None and _FILING_FURNITURE_TAIL_RE.search(line[-360:]):
        return match.group("body").rstrip()
    if re.search(r"(?i)\b(signing\s+on\s+behalf|principal\s+financial\s+officer|signatures?)\b", line[-360:]):
        signature_match = _TRAILING_SIGNATURE_PAGE_NUMBER_RE.match(line)
        if signature_match is not None:
            return signature_match.group("body").rstrip()
    return line


def _strip_table_of_contents_marker(line: str) -> str:
    if _is_table_of_contents_heading(line):
        return ""
    return _TOC_SUFFIX_PATTERN.sub("", line).rstrip()


def _nearest_nonblank_line(lines: list[str], index: int, *, step: int) -> str | None:
    cursor = index + step
    while 0 <= cursor < len(lines):
        if lines[cursor]:
            return lines[cursor]
        cursor += step
    return None


def _looks_like_heading(text: str) -> bool:
    if len(text) > 160 or _word_count(text) > 14:
        return False
    if re.match(r"(?i)^(item|part)\s+\d*[a-z]?", text):
        return True
    words = re.findall(r"[A-Za-z][A-Za-z&/\-']*", text)
    if not words:
        return False
    title_case = sum(1 for word in words if word[:1].isupper())
    upper_words = sum(1 for word in words if word.isupper() and len(word) > 1)
    return title_case >= max(1, len(words) // 2) or upper_words >= max(1, len(words) // 2)


def _press_release_headline_line(lines: list[_Line]) -> _Line | None:
    for line in lines[:40]:
        text = line.text.strip()
        if _is_press_release_prefix_boilerplate(text):
            continue
        if _looks_like_press_release_headline(text):
            return line
    return None


def _is_press_release_prefix_boilerplate(text: str) -> bool:
    normalized = _WHITESPACE_PATTERN.sub(" ", text).strip()
    if not normalized:
        return True
    if _PRESS_RELEASE_PREFIX_BOILERPLATE_RE.match(normalized):
        return True
    if re.search(r"(?i)\b(?:form\s+8-k|united\s+states\s+securities)\b", normalized):
        return True
    return False


def _looks_like_press_release_headline(text: str) -> bool:
    if len(text) > 220 or _word_count(text) > 24:
        return False
    if text.endswith((".", ":", ";")):
        return False
    if re.search(r"(?i)\b(exhibit\s+99|document|news\s+release)\b", text):
        return False
    if not _PRESS_RELEASE_HEADLINE_TERMS_RE.search(text):
        return False
    words = re.findall(r"[A-Za-z][A-Za-z&/\-']*", text)
    if not words:
        return False
    title_like = sum(1 for word in words if word[:1].isupper() or word.isupper())
    return title_like >= max(2, len(words) // 2)


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


def extract_press_release_units(normalized_body: str) -> list[TextUnit]:
    """Extract broad deterministic units from an earnings press release.

    v2 strips the SEC exhibit wrapper, finds the release headline, and splits
    the release into a small set of common earnings-release units. Anything not
    matching a known unit label remains searchable through a broad fallback unit.
    """
    body = normalized_body.strip()
    if not body:
        return [
            TextUnit(
                unit_ordinal=1,
                unit_type="press_release",
                unit_key="full_release_body",
                unit_title="Full Release Body",
                text=normalized_body,
                start_offset=0,
                end_offset=len(normalized_body),
                confidence=0.0,
                extraction_method="unparsed_fallback",
            )
        ]

    body_start_offset = normalized_body.find(body)
    lines = _indexed_lines(body)
    headline_line = _press_release_headline_line(lines)
    if headline_line is None:
        return [
            TextUnit(
                unit_ordinal=1,
                unit_type="press_release",
                unit_key="full_release_body",
                unit_title="Full Release Body",
                text=body,
                start_offset=body_start_offset,
                end_offset=body_start_offset + len(body),
                confidence=1.0,
                extraction_method="deterministic",
            )
        ]

    units: list[TextUnit] = []
    headline = headline_line.text.strip()
    headline_start = body_start_offset + headline_line.start
    units.append(
        TextUnit(
            unit_ordinal=1,
            unit_type="press_release",
            unit_key="headline",
            unit_title="Headline",
            text=headline,
            start_offset=headline_start,
            end_offset=headline_start + len(headline),
            confidence=1.0,
            extraction_method="deterministic",
        )
    )

    unit_ordinal = 2
    for key, title, start, end in _press_release_unit_spans(
        body, lines=lines, after_line=headline_line
    ):
        raw_text = body[start:end]
        unit_text = _clean_press_release_unit_text(raw_text)
        if not unit_text:
            continue
        absolute_start = body_start_offset + start + max(0, raw_text.find(unit_text[:80]))
        units.append(
            TextUnit(
                unit_ordinal=unit_ordinal,
                unit_type="press_release",
                unit_key=key,
                unit_title=title,
                text=unit_text,
                start_offset=absolute_start,
                end_offset=absolute_start + len(unit_text),
                confidence=1.0,
                extraction_method="deterministic",
            )
        )
        unit_ordinal += 1
    return units


def _press_release_unit_spans(
    body: str, *, lines: list[_Line], after_line: _Line
) -> list[tuple[str, str, int, int]]:
    markers: list[tuple[str, str, int]] = []
    for line in lines:
        if line.start <= after_line.start:
            continue
        marker = _press_release_unit_marker(line.text)
        if marker is None:
            continue
        key, title = marker
        if markers and markers[-1][0] == key:
            continue
        markers.append((key, title, line.start))

    if not markers:
        start = after_line.end
        return [("release_body", "Release Body", start, len(body))]

    spans: list[tuple[str, str, int, int]] = []
    if markers[0][2] > after_line.end:
        spans.append(("release_body", "Release Body", after_line.end, markers[0][2]))
    for idx, (key, title, start) in enumerate(markers):
        end = markers[idx + 1][2] if idx + 1 < len(markers) else len(body)
        spans.append((key, title, start, end))
    return spans


def _press_release_unit_marker(text: str) -> tuple[str, str] | None:
    candidate = _WHITESPACE_PATTERN.sub(" ", text).strip()
    if not candidate or candidate.endswith((".", ";", ",")):
        return None
    if len(candidate) > 140 or _word_count(candidate) > 14:
        return None
    for key, title, pattern in _PRESS_RELEASE_UNIT_MARKERS:
        if pattern.match(candidate):
            if key == "financial_tables" and "reconciliation" in candidate.lower():
                continue
            return key, title
    return None


def _clean_press_release_unit_text(text: str) -> str:
    lines = text.splitlines()
    cleaned: list[str] = []
    skip_possible_masthead = False
    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            if cleaned and cleaned[-1]:
                cleaned.append("")
            continue
        if _is_press_release_page_furniture_line(line):
            skip_possible_masthead = True
            if cleaned and cleaned[-1]:
                cleaned.append("")
            continue
        if skip_possible_masthead and _looks_like_press_release_masthead(line):
            skip_possible_masthead = False
            continue
        skip_possible_masthead = False
        cleaned.append(line)

    while cleaned and not cleaned[0]:
        cleaned.pop(0)
    while cleaned and not cleaned[-1]:
        cleaned.pop()
    return "\n".join(cleaned)


def _is_press_release_page_furniture_line(text: str) -> bool:
    return bool(_PRESS_RELEASE_PAGE_FURNITURE_RE.match(text))


def _looks_like_press_release_masthead(text: str) -> bool:
    if len(text) > 90 or _word_count(text) > 8:
        return False
    if text.endswith((".", ":", ";")):
        return False
    if _press_release_unit_marker(text) is not None:
        return False
    words = re.findall(r"[A-Za-z][A-Za-z&.'-]*", text)
    if not words:
        return False
    title_like = sum(1 for word in words if word[:1].isupper() or word.isupper())
    return title_like == len(words)


def build_text_unit_chunks(unit: TextUnit) -> list[ChunkRow]:
    section = ExtractedSection(
        section_key=unit.unit_key,
        section_title=unit.unit_title,
        part_label=None,
        item_label=None,
        text=unit.text,
        start_offset=unit.start_offset,
        end_offset=unit.end_offset,
        confidence=unit.confidence,
        extraction_method=unit.extraction_method,
    )
    return build_chunks(section)


def replace_text_units_and_chunks(
    conn: psycopg.Connection,
    *,
    artifact_id: int,
    company_id: int | None,
    fiscal_period_key: str | None,
    units: list[TextUnit],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM artifact_text_chunks
            WHERE text_unit_id IN (
                SELECT id FROM artifact_text_units WHERE artifact_id = %s
            );
            """,
            (artifact_id,),
        )
        cur.execute("DELETE FROM artifact_text_units WHERE artifact_id = %s;", (artifact_id,))

        for unit in units:
            cur.execute(
                """
                INSERT INTO artifact_text_units (
                    artifact_id, company_id, fiscal_period_key,
                    unit_ordinal, unit_type, unit_key, unit_title,
                    text, start_offset, end_offset,
                    extractor_version, confidence, extraction_method
                ) VALUES (
                    %s, %s, %s,
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
                    unit.unit_ordinal,
                    unit.unit_type,
                    unit.unit_key,
                    unit.unit_title,
                    unit.text,
                    unit.start_offset,
                    unit.end_offset,
                    TEXT_UNIT_EXTRACTOR_VERSION,
                    unit.confidence,
                    unit.extraction_method,
                ),
            )
            unit_id = cur.fetchone()[0]
            for chunk in build_text_unit_chunks(unit):
                cur.execute(
                    """
                    INSERT INTO artifact_text_chunks (
                        text_unit_id, chunk_ordinal, text, search_text,
                        heading_path, start_offset, end_offset, chunker_version
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        unit_id,
                        chunk.chunk_ordinal,
                        chunk.text,
                        chunk.search_text,
                        chunk.heading_path,
                        chunk.start_offset,
                        chunk.end_offset,
                        TEXT_CHUNKER_VERSION,
                    ),
                )


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


def artifact_has_current_text_units_and_chunks(
    conn: psycopg.Connection,
    artifact_id: int,
) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM artifact_text_units u
                WHERE u.artifact_id = %s
            )
            AND NOT EXISTS (
                SELECT 1
                FROM artifact_text_units u
                WHERE u.artifact_id = %s
                  AND u.extractor_version <> %s
            )
            AND NOT EXISTS (
                SELECT 1
                FROM artifact_text_chunks ch
                JOIN artifact_text_units u ON u.id = ch.text_unit_id
                WHERE u.artifact_id = %s
                  AND ch.chunker_version <> %s
            )
            AND NOT EXISTS (
                SELECT 1
                FROM artifact_text_units u
                WHERE u.artifact_id = %s
                  AND NOT EXISTS (
                      SELECT 1
                      FROM artifact_text_chunks ch
                      WHERE ch.text_unit_id = u.id
                  )
            );
            """,
            (
                artifact_id,
                artifact_id,
                TEXT_UNIT_EXTRACTOR_VERSION,
                artifact_id,
                TEXT_CHUNKER_VERSION,
                artifact_id,
            ),
        )
        return bool(cur.fetchone()[0])


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
        standalone = _standalone_section_heading(form_family, line.text)
        if standalone is None:
            continue
        key, expected_part, item_label = standalone
        actual_part = inline_part or current_part
        if form_family == "10-Q" and actual_part is not None and expected_part != actual_part:
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
        for key in ordered_keys:
            options = candidates.get(key, [])
            if not options:
                continue
            preferred = [c for c in options if not c.is_toc_like]
            if not preferred:
                continue
            chosen = preferred[0]
            selected.append(chosen)
        return selected

    reverse_selected: list[SectionCandidate] = []
    for key in reversed(ordered_keys):
        options = candidates.get(key, [])
        if not options:
            continue
        preferred = [c for c in options if not c.is_toc_like]
        if not preferred:
            continue
        chosen = preferred[-1]
        reverse_selected.append(chosen)
    return list(reversed(reverse_selected))


def _standalone_section_heading(
    form_family: FormFamily, line: str
) -> tuple[str, str | None, str] | None:
    text = _WHITESPACE_PATTERN.sub(" ", line).strip()
    if len(text) > 160 or _word_count(text) > 14:
        return None
    if form_family == "10-K":
        if _STANDALONE_BUSINESS_HEADING_RE.match(text):
            return ("item_1_business", None, "Item 1")
        if _STANDALONE_RISK_FACTORS_HEADING_RE.match(text):
            return ("item_1a_risk_factors", None, "Item 1A")
        if _STANDALONE_LEGAL_HEADING_RE.match(text):
            return ("item_3_legal_proceedings", None, "Item 3")
        if _STANDALONE_MDA_HEADING_RE.match(text):
            return ("item_7_mda", None, "Item 7")
        if _STANDALONE_MARKET_RISK_HEADING_RE.match(text):
            return ("item_7a_market_risk", None, "Item 7A")
        if _STANDALONE_CONTROLS_HEADING_RE.match(text):
            return ("item_9a_controls", None, "Item 9A")
        if _STANDALONE_OTHER_INFO_HEADING_RE.match(text):
            return ("item_9b_other_information", None, "Item 9B")
        return None

    if _STANDALONE_MDA_HEADING_RE.match(text):
        return ("part1_item2_mda", "Part I", "Item 2")
    if _STANDALONE_MARKET_RISK_HEADING_RE.match(text):
        return ("part1_item3_market_risk", "Part I", "Item 3")
    if _STANDALONE_CONTROLS_HEADING_RE.match(text):
        return ("part1_item4_controls", "Part I", "Item 4")
    if _STANDALONE_RISK_FACTORS_HEADING_RE.match(text):
        return ("part2_item1a_risk_factors", "Part II", "Item 1A")
    if _STANDALONE_OTHER_INFO_HEADING_RE.match(text):
        return ("part2_item5_other_information", "Part II", "Item 5")
    return None


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
        if form_family == "10-K":
            end = min(end, _next_10k_item_boundary(candidate, lines, body, end))
        if form_family == "10-Q":
            end = min(end, _next_10q_item_boundary(candidate, lines, body, end))
        text = body[start:end].strip()
        text, end = _trim_extracted_section_tail(text, start, end, candidate.key)
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


def _trim_extracted_section_tail(
    text: str, start_offset: int, end_offset: int, section_key: str
) -> tuple[str, int]:
    trim_markers = [
        "\nForm 10-K Cross-Reference Index\n",
        "\nForm 10-Q Cross-Reference Index\n",
        "\nItem Number Item\n",
    ]
    if section_key in {"item_3_legal_proceedings", "part2_item1_legal_proceedings"}:
        trim_markers.extend(["\nKey Terms\n", "\nIndex to Supplemental Details\n"])
    trim_at: int | None = None
    for marker in trim_markers:
        idx = text.find(marker)
        if idx <= 0:
            continue
        trim_at = idx if trim_at is None else min(trim_at, idx)
    if trim_at is None:
        return text, end_offset
    trimmed = text[:trim_at].rstrip()
    return trimmed, start_offset + len(trimmed)


def _next_10k_item_boundary(
    candidate: SectionCandidate,
    lines: list[_Line],
    body: str,
    fallback_end: int,
) -> int:
    """Find the next Form 10-K item heading, including unextracted items."""
    current_order = _item_order(candidate.item_label)
    for idx, line in enumerate(lines[candidate.line_index + 1 :], start=candidate.line_index + 1):
        if line.start >= fallback_end:
            break
        item_label = _extract_10k_item_heading(line.text)
        if item_label is None:
            continue
        if item_label not in _TEN_K_BOUNDARY_ITEMS:
            continue
        if _item_order(item_label) <= current_order:
            continue
        if _is_toc_like(lines, idx, body):
            continue
        return line.start
    return fallback_end


def _next_10q_item_boundary(
    candidate: SectionCandidate,
    lines: list[_Line],
    body: str,
    fallback_end: int,
) -> int:
    """Find the next same-part Form 10-Q item heading, including unextracted items."""
    if candidate.part_label is None:
        return fallback_end
    current_order = _item_order(candidate.item_label)
    current_part = candidate.part_label
    for idx, line in enumerate(lines[candidate.line_index + 1 :], start=candidate.line_index + 1):
        if line.start >= fallback_end:
            break
        inline_part = _extract_part_heading(line.text)
        if inline_part is not None:
            current_part = inline_part
            if current_part != candidate.part_label:
                return line.start
        item_label = _extract_10q_item_heading(line.text)
        if item_label is None:
            continue
        actual_part = inline_part or current_part
        if actual_part != candidate.part_label:
            continue
        if item_label not in _TEN_Q_BOUNDARY_ITEMS.get(actual_part, set()):
            continue
        if _item_order(item_label) <= current_order:
            continue
        if _is_toc_like(lines, idx, body):
            continue
        return line.start
    return fallback_end


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
    return sum(1 for _ in re.finditer(r"\b\w+\b", text))


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


def _extract_10q_item_heading(line: str) -> str | None:
    text = line.strip()
    if len(text) > 220 or _word_count(text) > 18:
        return None
    match = _TEN_Q_ITEM_HEADING_PATTERN.match(text)
    if match is None:
        return None
    item = match.group("item").upper()
    return f"Item {item}"


def _extract_10k_item_heading(line: str) -> str | None:
    text = line.strip()
    if len(text) > 220 or _word_count(text) > 18:
        return None
    match = _TEN_K_ITEM_HEADING_PATTERN.match(text)
    if match is None:
        return None
    item = match.group("item").upper()
    return f"Item {item}"


def _item_order(item_label: str) -> int:
    match = re.search(r"(?i)item\s+(\d+)([a-z]?)", item_label)
    if match is None:
        return -1
    base = int(match.group(1)) * 10
    suffix = match.group(2).upper()
    return base + (ord(suffix) - ord("A") + 1 if suffix else 0)


def _is_toc_like(lines: list[_Line], index: int, body: str) -> bool:
    line = lines[index].text
    if _TOC_DOTS_PATTERN.search(line):
        return True
    if lines[index].start <= int(len(body) * 0.15) and _looks_like_heading(line):
        previous_headings = sum(
            1
            for probe in lines[max(0, index - 8) : index]
            if _looks_like_heading(probe.text)
        )
        if previous_headings >= 4:
            return True
    following = lines[index + 1 : min(len(lines), index + 10)]
    if _looks_like_toc_heading_list_window([lines[index], *following]):
        return True
    if _looks_like_toc_page_number_window(following):
        return True
    if any(_looks_like_toc_page_reference(probe.text) for probe in following):
        prose_before_page_ref = False
        for probe in following:
            if _looks_like_toc_page_reference(probe.text):
                break
            if _word_count(probe.text) >= 6 and probe.text.endswith((".", "?", "!", "”", '"')):
                prose_before_page_ref = True
                break
        if not prose_before_page_ref:
            return True
    if lines[index].start > int(len(body) * 0.15):
        return False
    return _looks_like_toc_item_heading_cluster(lines[index : min(len(lines), index + 8)])


def _looks_like_toc_page_number_window(lines: list[_Line]) -> bool:
    page_numbers = 0
    short_labels = 0
    for probe in lines:
        text = probe.text.strip()
        if re.fullmatch(r"\d{1,3}", text):
            page_numbers += 1
            if page_numbers >= 2 and short_labels >= 2:
                return True
            continue
        if _word_count(text) >= 6 and text.endswith((".", "?", "!", "”", '"')):
            return False
        if text.endswith((".", "?", "!", ";", "”", '"')):
            continue
        if _looks_like_heading(text):
            short_labels += 1
            if page_numbers >= 2 and short_labels >= 2:
                return True
    return page_numbers >= 2 and short_labels >= 2


def _looks_like_toc_heading_list_window(lines: list[_Line]) -> bool:
    headings = 0
    for probe in lines:
        text = probe.text.strip()
        if _word_count(text) >= 6 and text.endswith((".", "?", "!", "”", '"')):
            return False
        if text.endswith((".", "?", "!", ";", "”", '"')):
            continue
        if _looks_like_heading(text):
            headings += 1
    return headings >= 5


def _looks_like_toc_item_heading_cluster(lines: list[_Line]) -> bool:
    item_headings = 0
    for probe in lines:
        text = probe.text.strip()
        if _word_count(text) >= 6 and text.endswith((".", "?", "!", "”", '"')):
            return False
        if text.endswith((".", "?", "!", ";", "”", '"')):
            continue
        if re.search(r"(?i)\bitem\s+\d+[a-z]?\b", text):
            item_headings += 1
    return item_headings >= 3


def _looks_like_toc_page_reference(text: str) -> bool:
    normalized = _WHITESPACE_PATTERN.sub(" ", text).strip()
    if re.fullmatch(r"(?i)pages?\s+\d{1,4}(?:\s*[-,]\s*\d{1,4})*(?:\s*,\s*\d{1,4}\s*[-,]\s*\d{1,4})*", normalized):
        return True
    return normalized.lower() == "none"


def _is_subheading(text: str) -> bool:
    if not text or _word_count(text) > 12 or len(text) > 120:
        return False
    if text.endswith((".", "?", "!", ";")):
        return False
    if _is_table_of_contents_heading(text):
        return False
    if _looks_like_financial_table_row(text):
        return False
    words = re.findall(r"[A-Za-z][A-Za-z&/\-']*", text)
    if not words:
        return False
    title_case = sum(1 for word in words if word[:1].isupper())
    return title_case >= max(1, len(words) // 2)


def _is_table_of_contents_heading(text: str) -> bool:
    normalized = _WHITESPACE_PATTERN.sub(" ", text).strip().lower()
    normalized = normalized.replace("conten t s", "contents")
    return normalized == "table of contents"


def _looks_like_financial_table_row(text: str) -> bool:
    numeric_tokens = re.findall(r"\b\d[\d,]*(?:\.\d+)?\b", text)
    if not numeric_tokens:
        return False
    words = re.findall(r"[A-Za-z][A-Za-z&/\-']*", text)
    has_financial_markers = "$" in text or "%" in text
    if has_financial_markers and len(words) <= 4:
        return True
    if len(numeric_tokens) < 3:
        return False
    if has_financial_markers:
        return len(words) <= 8 or len(numeric_tokens) >= len(words)
    return len(numeric_tokens) >= max(4, len(words) * 2)


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
