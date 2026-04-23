from __future__ import annotations

from arrow.ingest.sec.qualitative import build_chunks, extract_sections, normalize_filing_body


def test_extract_10q_sections_is_part_aware_and_skips_toc() -> None:
    html = b"""
    <html><body>
      <div>TABLE OF CONTENTS</div>
      <div>Item 2. Management's Discussion and Analysis ........ 12</div>
      <div>Item 3. Quantitative and Qualitative Disclosures About Market Risk ........ 42</div>
      <div>Part I</div>
      <h2>Item 2. Management's Discussion and Analysis of Financial Condition and Results of Operations</h2>
      <p>Quarterly discussion text. Revenue improved meaningfully and margins expanded.</p>
      <h2>Item 3. Quantitative and Qualitative Disclosures About Market Risk</h2>
      <p>Market risk text.</p>
      <div>Part II</div>
      <h2>Item 1A. Risk Factors</h2>
      <p>Risk factor text.</p>
    </body></html>
    """
    normalized = normalize_filing_body(html, "text/html")

    sections = extract_sections("10-Q", normalized)
    keys = [section.section_key for section in sections]

    assert keys == [
        "part1_item2_mda",
        "part1_item3_market_risk",
        "part2_item1a_risk_factors",
    ]
    assert sections[0].part_label == "Part I"
    assert "TABLE OF CONTENTS" not in sections[0].text


def test_extract_unparsed_body_when_no_valid_headings_found() -> None:
    normalized = normalize_filing_body(
        b"<html><body><p>Plain filing body with no recognizable SEC item headings.</p></body></html>",
        "text/html",
    )

    sections = extract_sections("10-K", normalized)

    assert len(sections) == 1
    section = sections[0]
    assert section.section_key == "unparsed_body"
    assert section.extraction_method == "unparsed_fallback"
    assert section.confidence == 0.0


def test_chunking_preserves_heading_path_and_sentence_overlap() -> None:
    normalized = normalize_filing_body(
        (
            "<html><body>"
            "<h2>Item 1A. Risk Factors</h2>"
            "<p>Regulatory Risks</p>"
            "<p>" + " ".join(
                f"Sentence {i}. This is additional context for the filing narrative."
                for i in range(1, 220)
            ) + "</p>"
            "<p>Data Privacy</p>"
            "<p>" + " ".join(
                f"Privacy sentence {i}. This is more narrative text for chunking."
                for i in range(1, 220)
            ) + "</p>"
            "</body></html>"
        ).encode(),
        "text/html",
    )
    section = extract_sections("10-K", normalized)[0]

    chunks = build_chunks(section)

    assert len(chunks) >= 2
    assert chunks[0].heading_path[0] == "Item 1A. Risk Factors"
    assert any("Regulatory Risks" in path for path in chunks[0].heading_path)
    tail_sentence = chunks[0].text.split(".")[-2].strip()
    assert tail_sentence
    assert tail_sentence in chunks[1].text
