# SEC Section Keys

Authoritative reference for the section keys, extraction contract, and chunking rules used by the SEC qualitative layer defined in [../architecture/sec_qualitative_layer.md](../architecture/sec_qualitative_layer.md).

## Scope

This phase covers:
- `10-K`
- `10-Q`
- extracted full sections in `artifact_sections`
- standardized retrieval chunks in `artifact_section_chunks`

Out of scope here:
- transcripts
- exhibits
- `8-K` body extraction
- table parsing inside narrative sections

## Filing Section Keys

### `10-K`

| Section key | Item |
|---|---|
| `item_1_business` | `Item 1. Business` |
| `item_1a_risk_factors` | `Item 1A. Risk Factors` |
| `item_1c_cybersecurity` | `Item 1C. Cybersecurity` |
| `item_3_legal_proceedings` | `Item 3. Legal Proceedings` |
| `item_7_mda` | `Item 7. Management's Discussion and Analysis` |
| `item_7a_market_risk` | `Item 7A. Quantitative and Qualitative Disclosures About Market Risk` |
| `item_9a_controls` | `Item 9A. Controls and Procedures` |
| `item_9b_other_information` | `Item 9B. Other Information` |

### `10-Q`

| Section key | Item |
|---|---|
| `part1_item2_mda` | `Part I Item 2. Management's Discussion and Analysis` |
| `part1_item3_market_risk` | `Part I Item 3. Quantitative and Qualitative Disclosures About Market Risk` |
| `part1_item4_controls` | `Part I Item 4. Controls and Procedures` |
| `part2_item1_legal_proceedings` | `Part II Item 1. Legal Proceedings` |
| `part2_item1a_risk_factors` | `Part II Item 1A. Risk Factors` |
| `part2_item5_other_information` | `Part II Item 5. Other Information` |

Fallback:
- `unparsed_body`

## Extraction Method Contract

`extraction_method` values:
- `deterministic`
- `repair`
- `unparsed_fallback`

`confidence` is a float in `[0.0, 1.0]`.

Rules:
- `deterministic` requires `confidence >= 0.85`
- `repair` covers `(0.0, 0.85)`
- `unparsed_fallback` writes `0.0`

If no valid section heading is found:
- write one `artifact_sections` row with `section_key = 'unparsed_body'`
- store the full normalized filing body in `text`
- chunk it normally so FTS coverage is preserved

## 10-Q Part-Aware Rule

10-Q extraction must be Part-aware.

Rules:
- track the current `Part I` / `Part II` context while scanning the filing body
- resolve `Item` numbers only against the current Part
- a bare `Item 1`, `Item 1A`, or `Item 2` with no Part context is not a valid mapped section

## Chunking Contract

Every extracted section is chunked.

Rules:
- chunk only within one section
- never cross section boundaries
- split on structure first:
  - subheadings
  - paragraph boundaries
  - sentence boundaries if needed
- size is a guardrail, not the primary split rule

Standard:
- target size: `1,000-1,500` words
- hard max: about `1,800` words
- overlap: `1-2` sentences
- overlap must be sentence-aligned
- target overlap range: about `10-15%`
- mid-sentence overlap is invalid output

Chunk fields of note:
- `text` = faithful chunk prose
- `search_text` = normalized FTS input
- `heading_path` = `TEXT[]` chain of headings above the chunk, e.g. `['Item 1A. Risk Factors', 'Regulatory Risks']`

`search_text` normalization:
- HTML entity decode
- unicode NFKC normalize
- lowercase
- whitespace collapse

No stemming or stopword removal is baked into `search_text`; that belongs to the FTS configuration.
