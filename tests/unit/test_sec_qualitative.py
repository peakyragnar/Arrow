from __future__ import annotations

from arrow.ingest.sec.qualitative import (
    ExtractedSection,
    _is_subheading,
    build_chunks,
    build_text_unit_chunks,
    extract_sections,
    extract_press_release_units,
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


def test_normalize_filing_body_removes_nul_bytes() -> None:
    normalized = normalize_filing_body(b"<html><body>Revenue\x00 grew.</body></html>", "text/html")

    assert "\x00" not in normalized
    assert normalized == "Revenue grew."


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


def test_extract_10q_risk_factors_stop_before_unextracted_item6_exhibits() -> None:
    html = b"""
    <html><body>
      <div>Part II</div>
      <h2>Item 1A. Risk Factors</h2>
      <p>Risk factor text that should stay inside the risk factor section.</p>
      <h2>ITEM 6. EXHIBITS</h2>
      <p>Exhibit index text should not be part of risk factors.</p>
      <p>SIGNATURE Pursuant to the requirements of the Securities Exchange Act of 1934.</p>
    </body></html>
    """
    normalized = normalize_filing_body(html, "text/html")

    sections = extract_sections("10-Q", normalized)
    risk_factors = next(
        section for section in sections if section.section_key == "part2_item1a_risk_factors"
    )

    assert "Risk factor text that should stay inside" in risk_factors.text
    assert "ITEM 6. EXHIBITS" not in risk_factors.text
    assert "SIGNATURE" not in risk_factors.text


def test_extract_10k_risk_factors_stop_before_unextracted_item1b() -> None:
    html = b"""
    <html><body>
      <h2>Item 1A. Risk Factors</h2>
      <p>Risk factor text that should stay inside the risk factor section.</p>
      <h2>ITEM 1B. UNRESOLVED STAFF COMMENTS</h2>
      <p>None.</p>
      <h2>Item 3. Legal Proceedings</h2>
      <p>Legal proceedings text.</p>
    </body></html>
    """
    normalized = normalize_filing_body(html, "text/html")

    sections = extract_sections("10-K", normalized)
    risk_factors = next(
        section for section in sections if section.section_key == "item_1a_risk_factors"
    )

    assert "Risk factor text that should stay inside" in risk_factors.text
    assert "ITEM 1B. UNRESOLVED STAFF COMMENTS" not in risk_factors.text
    assert "None." not in risk_factors.text


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


def test_normalize_filing_body_strips_final_page_number_after_signature() -> None:
    normalized = normalize_filing_body(
        b"""
        <html><body>
          <p>Title: Executive Vice President, Chief Financial Officer and Treasurer</p>
          <p>Signing on behalf of the Registrant as the Principal Financial Officer</p>
          <div>62</div>
        </body></html>
        """,
        "text/html",
    )

    assert normalized.endswith("Principal Financial Officer")
    assert "\n62" not in normalized


def test_normalize_filing_body_strips_table_of_contents_furniture() -> None:
    normalized = normalize_filing_body(
        b"""
        <html><body>
          <p>Financial statement discussion that should remain. Table of Contents</p>
          <div>Table of Conten t s</div>
          <p>Risk discussion that should also remain.</p>
        </body></html>
        """,
        "text/html",
    )

    assert "Financial statement discussion that should remain." in normalized
    assert "Risk discussion that should also remain." in normalized
    assert "Table of Contents" not in normalized
    assert "Table of Conten t s" not in normalized


def test_normalize_filing_body_strips_conservative_filing_tail_page_numbers() -> None:
    normalized = normalize_filing_body(
        b"""
        <html><body>
          <p>The Inline XBRL tags are embedded within the Inline XBRL document
          * Management contracts and compensatory plans or arrangements. 62</p>
          <p>Signing on behalf of the Registrant as the Principal Financial Officer 57</p>
          <p>The company had no customer over 10%. 2024</p>
        </body></html>
        """,
        "text/html",
    )

    assert "arrangements. 62" not in normalized
    assert "arrangements." in normalized
    assert "Principal Financial Officer 57" not in normalized
    assert "Principal Financial Officer" in normalized
    assert "The company had no customer over 10%. 2024" in normalized


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


def test_subheading_detection_rejects_table_of_contents_labels() -> None:
    assert not _is_subheading("Table of Contents")
    assert not _is_subheading("TABLE OF CONTENTS")
    assert not _is_subheading("Table of Conten t s")

    assert _is_subheading("Legal and Regulatory Risks")


def test_press_release_extraction_creates_headline_and_body_units() -> None:
    normalized = normalize_filing_body(
        b"""
        <html><body>
          <h1>Intel Reports First-Quarter 2026 Financial Results</h1>
          <p>Revenue was $12.7 billion and gross margin improved sequentially.</p>
          <p>The company provided second-quarter guidance.</p>
        </body></html>
        """,
        "text/html",
    )

    units = extract_press_release_units(normalized)

    assert [unit.unit_key for unit in units] == ["headline", "release_body"]
    assert units[0].text == "Intel Reports First-Quarter 2026 Financial Results"
    assert "gross margin improved" in units[1].text
    assert units[0].confidence == 1.0


def test_press_release_extraction_skips_exhibit_boilerplate_and_splits_units() -> None:
    normalized = normalize_filing_body(
        b"""
        <html><body>
          <div>EX-99.1</div>
          <div>2</div>
          <div>q126earningsrelease.htm</div>
          <div>EX-99.1</div>
          <div>Document</div>
          <div>Exhibit 99.1</div>
          <div>Intel Corporation</div>
          <div>2200 Mission College Blvd.</div>
          <div>Santa Clara, CA 95054-1549</div>
          <div>News Release</div>
          <h1>Intel Reports First-Quarter 2026 Financial Results</h1>
          <h2>News Summary</h2>
          <p>First-quarter revenue was $13.6 billion, up 7% year-over-year.</p>
          <h2>Business Outlook</h2>
          <p>Forecasting second-quarter 2026 revenue of $13.8 billion to $14.8 billion.</p>
          <h2>Forward-Looking Statements</h2>
          <p>This release contains forward-looking statements.</p>
          <h2>About Intel</h2>
          <p>Intel designs and manufactures advanced semiconductors.</p>
          <div>Intel/Page 6</div>
          <div>Intel Corporation</div>
          <h2>Supplemental Reconciliations of GAAP Actuals to Non-GAAP Actuals</h2>
          <p>GAAP gross margin was 39.4% and non-GAAP gross margin was 41.0%.</p>
        </body></html>
        """,
        "text/html",
    )

    units = extract_press_release_units(normalized)

    assert [unit.unit_key for unit in units] == [
        "headline",
        "news_summary",
        "business_outlook",
        "forward_looking_statements",
        "about_company",
        "non_gaap_reconciliations",
    ]
    assert units[0].text == "Intel Reports First-Quarter 2026 Financial Results"
    assert units[1].text.startswith("News Summary")
    assert "EX-99.1" not in units[1].text
    assert "q126earningsrelease.htm" not in units[1].text
    assert "second-quarter 2026 revenue" in units[2].text
    assert "Intel/Page 6" not in units[4].text
    assert not units[4].text.endswith("Intel Corporation")


def test_press_release_chunks_are_searchable_retrieval_units() -> None:
    normalized = normalize_filing_body(
        b"""
        <html><body>
          <h1>Intel Reports First-Quarter 2026 Financial Results</h1>
          <p>Revenue was $12.7 billion and gross margin improved sequentially.</p>
        </body></html>
        """,
        "text/html",
    )
    body_unit = extract_press_release_units(normalized)[1]

    chunks = build_text_unit_chunks(body_unit)

    assert len(chunks) == 1
    assert chunks[0].heading_path == ["Release Body"]
    assert "gross margin" in chunks[0].search_text


def test_intel_style_standalone_headings_beat_toc_page_lists() -> None:
    html = b"""
    <html><body>
      <p>Financial Statements Notes to Financial Statements</p>
      <p>24</p>
      <h2>Management's Discussion and Analysis</h2>
      <p>This report should be read in conjunction with our annual report.</p>
      <p>Revenue increased because client demand improved and costs declined.</p>
      <p>MD&A</p>
      <p>25</p>
      <h2>Quantitative and Qualitative Disclosures About Market Risk</h2>
      <p>There were no material changes in market risk.</p>
      <h2>Controls and Procedures</h2>
      <p>Disclosure controls and procedures were effective.</p>
      <h2>Risk Factors</h2>
      <p>Risk factors could materially affect the business.</p>
      <h2>Part I - Financial Information</h2>
      <p>Item 1. Financial Statements</p>
      <p>Pages 3 - 24</p>
      <p>Item 2.</p>
      <p>Management's Discussion and Analysis of Financial Condition and Results of Operations</p>
      <p>Liquidity and capital resources</p>
      <p>Pages 33 - 34</p>
      <p>Item 3. Quantitative and Qualitative Disclosures About Market Risk</p>
      <p>Page 35</p>
      <h2>Part II - Other Information</h2>
      <p>Item 1A. Risk Factors</p>
      <p>Page 36</p>
    </body></html>
    """

    sections = extract_sections("10-Q", normalize_filing_body(html, "text/html"))
    by_key = {section.section_key: section for section in sections}

    assert "part1_item2_mda" in by_key
    assert "Revenue increased because client demand improved" in by_key["part1_item2_mda"].text
    assert "Pages 33 - 34" not in by_key["part1_item2_mda"].text
    assert "part1_item3_market_risk" in by_key
    assert "part1_item4_controls" in by_key
    assert "part2_item1a_risk_factors" in by_key
