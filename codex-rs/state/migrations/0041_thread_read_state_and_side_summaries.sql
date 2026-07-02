ALTER TABLE threads ADD COLUMN read_at_ms INTEGER NOT NULL DEFAULT 0;

-- Existing rows predate local read-state tracking. Treat their current
-- persisted update as already read so upgrades do not light up every old
-- session as unread.
UPDATE threads
SET read_at_ms = updated_at_ms
WHERE read_at_ms = 0;

CREATE TABLE thread_side_summaries (
    id TEXT PRIMARY KEY,
    created_at_ms INTEGER NOT NULL,
    read_at_ms INTEGER NOT NULL,
    scope_json TEXT NOT NULL,
    thread_count INTEGER NOT NULL,
    unread_count INTEGER NOT NULL,
    summary_json TEXT NOT NULL,
    summary_markdown TEXT NOT NULL
);

CREATE INDEX idx_thread_side_summaries_created_at_ms
    ON thread_side_summaries(created_at_ms DESC, id DESC);
