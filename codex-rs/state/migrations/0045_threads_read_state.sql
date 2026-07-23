ALTER TABLE threads ADD COLUMN read_at_ms INTEGER NOT NULL DEFAULT 0;

-- This is intentionally a one-time upgrade backfill. Rows that already exist
-- are considered read through their migration-time update snapshot; rows
-- inserted later keep the 0 ("never read") default and start unread.
--
-- Preserve 0 exclusively as the sentinel, including for a legacy row whose
-- update timestamp is exactly the Unix epoch.
UPDATE threads
SET read_at_ms = CASE
    WHEN updated_at_ms = 0 THEN 1
    ELSE updated_at_ms
END;
