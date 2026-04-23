from __future__ import annotations

from arrow.ingest.sec.qualitative import (
    ExtractedSection,
    _is_subheading,
    build_chunks,
    extract_sections,
    normalize_filing_body,
)


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


def test_extract_10q_stops_part1_before_part2_title_line() -> None:
    html = b"""
    <html><body>
      <div>Part I</div>
      <h2>Item 2. Management's Discussion and Analysis of Financial Condition and Results of Operations</h2>
      <p>MD&A text that should stay inside the section.</p>
      <div>PART II. OTHER INFORMATION</div>
      <h2>Item 1A. Risk Factors</h2>
      <p>Risk factor text.</p>
    </body></html>
    """
    normalized = normalize_filing_body(html, "text/html")

    sections = extract_sections("10-Q", normalized)
    mda = next(section for section in sections if section.section_key == "part1_item2_mda")

    assert "MD&A text that should stay inside the section." in mda.text
    assert "PART II. OTHER INFORMATION" not in mda.text


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


def test_normalize_filing_body_strips_standalone_page_numbers() -> None:
    normalized = normalize_filing_body(
        b"""
        <html><body>
          <p>For a description of our operating lease obligations, refer to Note 3.</p>
          <div>33</div>
          <h3>Climate Change</h3>
          <p>There has been no material impact from climate-related business trends.</p>
          <table><tr><td>2024</td><td>100</td></tr></table>
        </body></html>
        """,
        "text/html",
    )

    assert "\n33\n" not in normalized
    assert "Note 3.\n\nClimate Change" in normalized
    assert "2024 100" in normalized


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


def test_chunking_splits_embedded_mda_subheading_and_updates_heading_path() -> None:
    section = ExtractedSection(
        section_key="part1_item2_mda",
        section_title="Part I Item 2. Management's Discussion and Analysis",
        part_label="Part I",
        item_label="Item 2",
        text=(
            "Material Cash Requirements and Other Obligations\n\n"
            "33 To date, there has been no material impact to our results of operations "
            "associated with global sustainability regulations, compliance, costs from "
            "sourcing renewable energy or climate-related business trends. Adoption of "
            "New and Recently Issued Accounting Pronouncements There has been no adoption "
            "of any new and recently issued accounting pronouncements."
        ),
        start_offset=0,
        end_offset=357,
        confidence=1.0,
        extraction_method="deterministic",
    )

    chunks = build_chunks(section)

    material_chunk = next(
        chunk for chunk in chunks if "global sustainability regulations" in chunk.text
    )
    accounting_chunk = next(
        chunk
        for chunk in chunks
        if "There has been no adoption of any new and recently issued accounting pronouncements"
        in chunk.text
    )

    assert material_chunk.heading_path == [
        "Part I Item 2. Management's Discussion and Analysis",
        "Material Cash Requirements and Other Obligations",
    ]
    assert accounting_chunk.heading_path == [
        "Part I Item 2. Management's Discussion and Analysis",
        "Adoption of New and Recently Issued Accounting Pronouncements",
    ]
    assert "global sustainability regulations" not in accounting_chunk.text


def test_chunking_does_not_treat_financial_table_rows_as_subheadings() -> None:
    leading_context = " ".join(
        f"Operational sentence {i}. Demand remained broad across product categories."
        for i in range(1, 150)
    )
    section = ExtractedSection(
        section_key="part1_item2_mda",
        section_title="Part I Item 2. Management's Discussion and Analysis",
        part_label="Part I",
        item_label="Item 2",
        text=(
            "Results of Operations\n\n"
            f"{leading_context}\n\n"
            "Net income $ 16,599 $ 14,881 $ 6,188 12 % 168 %\n\n"
            "Sequentially, gross margin decreased primarily driven by inventory "
            "provisions and a higher mix of new products."
        ),
        start_offset=0,
        end_offset=12000,
        confidence=1.0,
        extraction_method="deterministic",
    )

    chunks = build_chunks(section)

    assert not any("Net income $" in " > ".join(chunk.heading_path) for chunk in chunks)
    assert any(
        chunk.heading_path == [
            "Part I Item 2. Management's Discussion and Analysis",
            "Results of Operations",
        ]
        and "Sequentially, gross margin decreased" in chunk.text
        for chunk in chunks
    )


def test_subheading_detection_rejects_financial_table_rows() -> None:
    assert not _is_subheading("Net income $ 16,599 $ 14,881 $ 6,188 12 % 168 %")
    assert not _is_subheading("Net income $ 4,757 $ 2,114")
    assert not _is_subheading("Net income $ 4,757")
    assert not _is_subheading("Revenue $ 30,040 $ 26,974 $ 13,507 11 % 122 %")
    assert not _is_subheading("Gross margin 75.1 % 78.4 % 70.1 %")

    assert _is_subheading("Critical Accounting Policies and Estimates")
    assert _is_subheading("Market Platform Highlights")
    assert _is_subheading("Liquidity and Capital Resources")
