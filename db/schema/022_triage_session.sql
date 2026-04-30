-- triage_session: structured capture of chat-driven data-quality work.
--
-- Every meaningful triage activity in chat (Claude Code, Codex, ...)
-- leaves a row here so the autonomous-agent path has training data:
-- what surfaced, what was investigated, the operator's reasoning, the
-- actions taken, the outcome.
--
-- This is the V1 substrate for the future autonomous data-quality
-- operator agent (see docs/architecture/steward.md § LLM Trajectory).
-- The agent will read these rows via SQL+FTS — same retrieval pattern
-- as the analyst layer — to recognize patterns it has seen the
-- operator approve before.
--
-- Distinct from data_quality_findings.history (per-row state-change
-- audit) and ingest_runs.counts (per-script execution metadata). This
-- is per-OPERATOR-SESSION: a higher-level record that ties findings,
-- investigations, and actions into the loop the operator and AI
-- worked through together.

CREATE TABLE triage_session (
    id                bigserial   PRIMARY KEY,
    started_at        timestamptz NOT NULL DEFAULT now(),
    finished_at       timestamptz,
    harness           text        NOT NULL,
    intent            text        NOT NULL,
    finding_ids       bigint[]    NOT NULL DEFAULT '{}',
    operator_quotes   jsonb       NOT NULL DEFAULT '[]'::jsonb,
    investigations    jsonb       NOT NULL DEFAULT '[]'::jsonb,
    actions_taken     jsonb       NOT NULL DEFAULT '[]'::jsonb,
    outcomes          jsonb       NOT NULL DEFAULT '{}'::jsonb,
    captured_pattern  text,
    session_ref       text,
    created_by        text        NOT NULL,

    CONSTRAINT triage_session_intent_nonempty
        CHECK (length(trim(intent)) > 0),
    CONSTRAINT triage_session_harness_known
        CHECK (harness IN ('claude_code', 'codex', 'human_only', 'other')),
    CONSTRAINT triage_session_created_by_nonempty
        CHECK (length(trim(created_by)) > 0)
);

CREATE INDEX triage_session_finding_ids_idx
    ON triage_session USING GIN (finding_ids);

CREATE INDEX triage_session_started_at_idx
    ON triage_session (started_at DESC);

CREATE INDEX triage_session_intent_tsv_idx
    ON triage_session USING GIN (to_tsvector('english', intent));

CREATE INDEX triage_session_pattern_tsv_idx
    ON triage_session USING GIN (to_tsvector('english', coalesce(captured_pattern, '')));

COMMENT ON TABLE triage_session IS
  'Per-operator-session capture of chat-driven triage work. Training corpus for the future autonomous data-quality operator agent. See docs/architecture/steward.md.';

COMMENT ON COLUMN triage_session.intent IS
  'What the session set out to do. One sentence — used for FTS retrieval.';

COMMENT ON COLUMN triage_session.harness IS
  'AI surface the operator was using: claude_code | codex | human_only | other.';

COMMENT ON COLUMN triage_session.finding_ids IS
  'Findings touched in this session. May be empty for sessions that uncovered new issues not yet flagged by a steward check.';

COMMENT ON COLUMN triage_session.operator_quotes IS
  'jsonb array of strings — verbatim operator words at decision points. The training signal: these are what the future agent should learn to predict / propose.';

COMMENT ON COLUMN triage_session.investigations IS
  'jsonb array of {action, target, result_summary} — what the AI ran/read/queried for verification before proposing action.';

COMMENT ON COLUMN triage_session.actions_taken IS
  'jsonb array of {kind, target, identifier, summary} — concrete writes (script invocations, commits, supersessions, etc.).';

COMMENT ON COLUMN triage_session.outcomes IS
  'jsonb of finding-state transitions and data deltas. e.g. {findings_closed: [...], data_changed: {...}}.';

COMMENT ON COLUMN triage_session.captured_pattern IS
  'Extractable rule for future-agent retrieval. Optional but high-leverage: one sentence the agent can read to recognize when this resolution applies.';

COMMENT ON COLUMN triage_session.created_by IS
  'Honest actor label: human:michael (operator-reasoned), claude:assistant_via_michael (AI-investigated, operator-approved), claude:assistant_triage (AI-driven, low operator review), system:auto (no human).';
