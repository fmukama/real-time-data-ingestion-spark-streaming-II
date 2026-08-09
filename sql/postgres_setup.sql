-- events: the clean, validated stream. Every row here already survived
-- transforms.add_rejection_reason (Phase 6) before Spark ever writes it, so
-- the NOT NULL / CHECK constraints below aren't speculative hardening -- each
-- one documents a specific defect that Phase 4 deliberately generates and
-- Phase 6 deliberately quarantines, meaning it is structurally guaranteed to
-- already be false by the time a row lands here:
--   - null product_id            -> quarantined, so product_id is NOT NULL here
--   - negative price              -> quarantined, so CHECK (price >= 0) holds
--   - quantity of zero            -> quarantined, so CHECK (quantity > 0) holds
--   - unparseable event_time      -> quarantined, so this is a real TIMESTAMPTZ
-- These constraints are a second, independent line of defense (the DB refuses
-- bad data even if it arrives by some path other than this pipeline, e.g. a
-- stray manual INSERT) -- not a claim that Spark's own checks are untrusted.
CREATE TABLE events (
    event_id      TEXT PRIMARY KEY,                 -- producer-generated UUID; the ON CONFLICT target in Phase 7
    event_time    TIMESTAMPTZ NOT NULL,              -- when the user action happened
    generated_at  TIMESTAMPTZ NOT NULL,              -- producer's wall clock at emit
    user_id       TEXT NOT NULL,
    product_id    TEXT NOT NULL,
    category      TEXT,                              -- nullable: an uncategorized product is a real case, not a defect
    event_type    TEXT NOT NULL CHECK (event_type IN ('view', 'add_to_cart', 'purchase')),
    price         NUMERIC(10, 2) NOT NULL CHECK (price >= 0),
    quantity      INTEGER NOT NULL CHECK (quantity > 0),
    revenue       NUMERIC(12, 2),                    -- price * quantity for 'purchase' rows only; NULL otherwise (Phase 6)
    ingested_at   TIMESTAMPTZ NOT NULL,               -- Spark's wall clock when the batch processed this row
    latency_ms    BIGINT NOT NULL                     -- ingested_at - generated_at, in ms; the perf report's real number
);

CREATE INDEX idx_events_event_time ON events (event_time);
CREATE INDEX idx_events_user_id ON events (user_id);


-- 
-- events_quarantine: every row Phase 6 rejects, kept for inspection rather
-- than silently dropped.
--
-- Every column is TEXT, deliberately, unlike `events`: a row lands here
-- precisely because something about it didn't survive validation, and for a
-- structurally corrupt CSV line (wrong field count, triggering Spark's
-- columnNameOfCorruptRecord) even event_id may not have parsed correctly. A
-- typed column or a NOT NULL event_id would either reject the write outright
-- or force lossy coercion of the very value you're trying to preserve for
-- inspection -- the opposite of what a quarantine table is for.
--
-- No PRIMARY KEY on event_id and no idempotent upsert here, unlike `events`.
-- This is a deliberate, narrower scope than Phase 7's guarantee for the valid
-- stream: quarantine is a diagnostic aid, not the financial record, so an
-- occasional duplicate row on the rare crash-and-replay path is an accepted
-- cost of keeping this table simple -- and it can't reliably key on event_id
-- anyway, per the paragraph above. `id` is a synthetic surrogate key purely so
-- individual rows are addressable.
-- 
CREATE TABLE events_quarantine (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id          TEXT,                          -- nullable: may be unrecoverable from a structurally corrupt line
    event_time        TEXT,                           -- kept raw: this may be the exact unparseable value under test
    generated_at      TEXT,
    user_id           TEXT,
    product_id        TEXT,
    category          TEXT,
    event_type        TEXT,
    price             TEXT,
    quantity          TEXT,
    rejection_reason  TEXT NOT NULL,                  -- the reason this table exists; always populated
    raw_line          TEXT,                            -- the original CSV line, when Spark's corrupt-record capture caught it
    quarantined_at    TIMESTAMPTZ NOT NULL
);


-- 
-- event_metrics: the optional windowed aggregate (Phase 9, stretch goal).
-- Table created now so the schema is complete in this one file; the streaming
-- query that populates it doesn't exist until Phase 9.
--
-- Composite PK on the window's identity plus category: Structured Streaming's
-- `update` output mode re-emits a window's row every time late data arrives
-- within the watermark, so the write path is an upsert (Phase 9's
-- ON CONFLICT ... DO UPDATE), never a plain append -- the PK is what makes
-- "the same window again" a well-defined, conflictable target.
-- 
CREATE TABLE event_metrics (
    window_start   TIMESTAMPTZ NOT NULL,
    window_end     TIMESTAMPTZ NOT NULL,
    category       TEXT NOT NULL,
    total_revenue  NUMERIC(14, 2) NOT NULL DEFAULT 0,
    event_count    BIGINT NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ NOT NULL,               -- last time this window's row was (re)computed
    PRIMARY KEY (window_start, window_end, category)
);
