ALTER TABLE threads ADD COLUMN read_at_ms INTEGER NOT NULL DEFAULT 0;
ALTER TABLE threads ADD COLUMN source_updated_at_ms INTEGER NOT NULL DEFAULT 0;
ALTER TABLE threads ADD COLUMN snapshot_revision INTEGER NOT NULL DEFAULT 0;
ALTER TABLE threads ADD COLUMN read_state_write_token INTEGER NOT NULL DEFAULT 0
    CHECK(read_state_write_token IN (0, 1));

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
END,
source_updated_at_ms = CASE
    WHEN updated_at_ms < 1577836800000 THEN updated_at_ms * 1000
    ELSE updated_at_ms
END;

-- Current writers advance snapshot_revision only for a new persisted snapshot.
-- Direct or mixed-version writers are also covered when they change a
-- snapshot-relevant value.
CREATE TRIGGER threads_read_at_after_snapshot_change
AFTER UPDATE ON threads
WHEN NEW.snapshot_revision IS NOT OLD.snapshot_revision
  OR NEW.updated_at_ms IS NOT OLD.updated_at_ms
  OR NEW.source_updated_at_ms IS NOT OLD.source_updated_at_ms
  OR NEW.history_mode IS NOT OLD.history_mode
  OR NEW.model IS NOT OLD.model
  OR NEW.reasoning_effort IS NOT OLD.reasoning_effort
  OR NEW.title IS NOT OLD.title
  OR NEW.preview IS NOT OLD.preview
  OR NEW.tokens_used IS NOT OLD.tokens_used
  OR NEW.first_user_message IS NOT OLD.first_user_message
BEGIN
    UPDATE threads
    SET read_at_ms = 0
    WHERE id = NEW.id;
END;

-- A legacy writer cannot advance read_state_write_token. Treat every explicit
-- assignment of its second-precision updated_at column as a new snapshot,
-- including an equal-second write that the millisecond compatibility trigger
-- cannot otherwise observe.
CREATE TRIGGER threads_read_at_after_legacy_updated_at_write
AFTER UPDATE OF updated_at ON threads
WHEN NEW.read_state_write_token IS OLD.read_state_write_token
BEGIN
    UPDATE threads
    SET read_at_ms = 0
    WHERE id = NEW.id;
END;
