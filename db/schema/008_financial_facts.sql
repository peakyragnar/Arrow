-- financial_facts: canonical long/skinny financial fact store.
--
-- One row per (company, concept, period_end, period_type, source pull,
-- extraction version). PIT-correct: restatements append a new row and set
-- superseded_at on the prior row — values are never mutated in place.
--
-- Identity rule (foundation, irreversible):
--   internal id (bigserial)
--   business identity = (company_id, concept, period_end, period_type,
--                        source_raw_response_id, extraction_version)
--                       → idempotent re-extraction (UNIQUE)
--   "current value at most once" = same identity minus source_raw_response_id,
--                                  enforced via partial unique index on
--                                  superseded_at IS NULL
--
-- Provenance (foundation, irreversible):
--   source_raw_response_id NOT NULL → every fact traces to a vendor payload
--   source_artifact_id NULL → optional pointer to a parsed document; used
--     when facts are derived from a filing's text/XBRL rather than a vendor
--     normalized endpoint
--   extraction_version → bump when extraction logic changes; old rows are
--     preserved (regeneratable from raw_responses), new rows coexist until
--     ingest decides to mark old ones superseded
--
-- Time-aware columns (per docs/architecture/system.md § Time-Aware Model):
--   published_at NOT NULL — when the source released this value (filing
--     date for SEC-derived; FMP's reported filing date for FMP-derived).
--     PIT queries depend on this being correct, not the fetch time.
--   superseded_at NULL — when a later row replaced this; NULL = current
--   ingested_at — when Arrow fetched it (audit trail)
--
-- Two clocks (per docs/reference/periods.md):
--   fiscal truth: fiscal_year, fiscal_quarter, fiscal_period_label, period_type
--   calendar normalization: calendar_year, calendar_quarter, calendar_period_label
--   Both stored, both indexed.

CREATE TABLE financial_facts (
    id                      bigserial   PRIMARY KEY,
    ingest_run_id           bigint      REFERENCES ingest_runs(id) ON DELETE RESTRICT,

    -- Entity
    company_id              bigint      NOT NULL REFERENCES companies(id) ON DELETE RESTRICT,

    -- What is being measured
    statement               text        NOT NULL,
    concept                 text        NOT NULL,
    value                   numeric(28,4) NOT NULL,
    unit                    text        NOT NULL,

    -- Fiscal truth (per docs/reference/periods.md)
    fiscal_year             smallint    NOT NULL,
    fiscal_quarter          smallint,
    fiscal_period_label     text        NOT NULL,
    period_end              date        NOT NULL,
    period_type             text        NOT NULL,

    -- Calendar normalization (pure function of period_end per periods.md)
    calendar_year           smallint    NOT NULL,
    calendar_quarter        smallint    NOT NULL,
    calendar_period_label   text        NOT NULL,

    -- Time-aware / PIT
    published_at            timestamptz NOT NULL,
    effective_at            timestamptz,
    superseded_at           timestamptz,
    ingested_at             timestamptz NOT NULL DEFAULT now(),

    -- Provenance (every derived row traces back to source)
    source_raw_response_id  bigint      NOT NULL REFERENCES raw_responses(id) ON DELETE RESTRICT,
    source_artifact_id      bigint      REFERENCES artifacts(id) ON DELETE RESTRICT,
    extraction_version      text        NOT NULL,

    CONSTRAINT financial_facts_statement_check CHECK (statement IN (
        'income_statement', 'balance_sheet', 'cash_flow',
        'metrics', 'ratios', 'segment'
    )),
    CONSTRAINT financial_facts_period_type_check
        CHECK (period_type IN ('quarter', 'annual', 'stub')),
    CONSTRAINT financial_facts_period_type_quarter_iff
        CHECK (
            (period_type IN ('annual', 'stub') AND fiscal_quarter IS NULL)
         OR (period_type = 'quarter' AND fiscal_quarter IS NOT NULL)
        ),
    CONSTRAINT financial_facts_fiscal_quarter_range
        CHECK (fiscal_quarter IS NULL OR fiscal_quarter BETWEEN 1 AND 4),
    CONSTRAINT financial_facts_calendar_quarter_range
        CHECK (calendar_quarter BETWEEN 1 AND 4),
    CONSTRAINT financial_facts_fiscal_period_label_format
        CHECK (fiscal_period_label ~ '^FY[0-9]{4}( Q[1-4])?$'),
    CONSTRAINT financial_facts_calendar_period_label_format
        CHECK (calendar_period_label ~ '^CY[0-9]{4} Q[1-4]$'),

    -- Idempotent re-extraction: re-parsing the same payload with the same
    -- extraction version must produce no new rows.
    CONSTRAINT financial_facts_unique_extraction
        UNIQUE (company_id, concept, period_end, period_type,
                source_raw_response_id, extraction_version)
);

-- At most one CURRENT row per (company, concept, period, period_type, extraction_version).
-- The supersession discipline becomes a DB-enforced invariant, not a hope.
CREATE UNIQUE INDEX financial_facts_one_current_idx
    ON financial_facts (company_id, concept, period_end, period_type, extraction_version)
    WHERE superseded_at IS NULL;

-- Primary analyst lookup: "give me NVDA's revenue series"
CREATE INDEX financial_facts_company_concept_period_idx
    ON financial_facts (company_id, concept, period_end DESC);

-- Default "current values only" version of the above
CREATE INDEX financial_facts_current_idx
    ON financial_facts (company_id, concept, period_end DESC)
    WHERE superseded_at IS NULL;

-- Cross-company comparison by calendar quarter
CREATE INDEX financial_facts_calendar_period_concept_idx
    ON financial_facts (calendar_year, calendar_quarter, concept);

-- Statement-scoped scans: "all income-statement items for company"
CREATE INDEX financial_facts_statement_company_idx
    ON financial_facts (statement, company_id, period_end DESC);

-- Provenance traversal: "what did this raw_response produce"
CREATE INDEX financial_facts_source_raw_response_idx
    ON financial_facts (source_raw_response_id);

-- Optional: artifact-derived facts (parsed filings) — only meaningful when populated
CREATE INDEX financial_facts_source_artifact_idx
    ON financial_facts (source_artifact_id)
    WHERE source_artifact_id IS NOT NULL;
