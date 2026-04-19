-- companies: financial reporting unit, one row per CIK.
--
-- A "company" here is the entity that reports financial statements.
-- Securities (shares, share classes) are a separate concept and will get
-- their own table later — GOOG and GOOGL both point at one Alphabet row.
--
-- Identity rule (foundation, irreversible):
--   internal id (bigserial) for FK references everywhere
--   cik (SEC Central Index Key) as the canonical external anchor —
--     stable across renames, ticker changes, share-class additions
--   ticker as a queryable display field (NOT a primary key — tickers
--     change, get reused, and aren't unique cross-time)
--
-- Fiscal calendar:
--   fiscal_year_end_md (MM-DD text, per docs/reference/periods.md) anchors
--   every fiscal/calendar derivation. NVDA="01-26", MSFT="06-30",
--   calendar-year filers="12-31". Stored as MM-DD, not (month, day) ints,
--   to keep the periods spec's wording and the column literal in sync.
--   Drift across fiscal-calendar changes is handled later via a
--   companies_fiscal_history sidecar table — out of scope for v1.

CREATE TABLE companies (
    id                      bigserial   PRIMARY KEY,

    -- Canonical external anchor
    cik                     bigint      NOT NULL UNIQUE,

    -- Display / lookup
    ticker                  text        NOT NULL,
    name                    text        NOT NULL,

    -- Fiscal calendar anchor (drives period derivation)
    fiscal_year_end_md      text        NOT NULL,

    -- Status
    status                  text        NOT NULL DEFAULT 'active',

    -- Bookkeeping
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT companies_status_check
        CHECK (status IN ('active', 'delisted', 'merged', 'acquired', 'private')),
    CONSTRAINT companies_fiscal_year_end_md_format
        CHECK (fiscal_year_end_md ~ '^(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$'),
    CONSTRAINT companies_cik_positive
        CHECK (cik > 0)
);

-- Lookup by current ticker (the analyst's most common entry point)
CREATE INDEX companies_ticker_idx ON companies (ticker);

-- Active universe scan
CREATE INDEX companies_active_idx
    ON companies (ticker)
    WHERE status = 'active';
