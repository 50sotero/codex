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
    -- Values below the 2020 cutoff are legacy second-precision values. Read
    -- markers are always stored as strict milliseconds so their unit is
    -- unambiguous after the one-time backfill.
    WHEN updated_at_ms < 1577836800000 THEN updated_at_ms * 1000
    ELSE updated_at_ms
END;

-- A writer that explicitly assigns updated_at_ms represents a new persisted
-- thread snapshot even when its timestamp is equal to or older than the prior
-- value. Clear the marker after every such write so commit order, rather than
-- timestamp ordering, determines whether that snapshot remains unread.
CREATE TRIGGER threads_read_at_after_updated_at_ms
AFTER UPDATE OF updated_at_ms ON threads
BEGIN
    UPDATE threads
    SET read_at_ms = 0
    WHERE id = NEW.id;
END;
