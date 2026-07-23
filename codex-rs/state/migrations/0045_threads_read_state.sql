ALTER TABLE threads ADD COLUMN read_at_ms INTEGER NOT NULL DEFAULT 0;
-- NULL lets migrated/old-binary rows adopt an unknown raw timestamp without becoming unread.
ALTER TABLE threads ADD COLUMN source_updated_at_ms INTEGER;
ALTER TABLE threads ADD COLUMN snapshot_revision INTEGER NOT NULL DEFAULT 0;
ALTER TABLE threads ADD COLUMN read_state_write_token INTEGER NOT NULL DEFAULT 0
    CHECK(read_state_write_token IN (0, 1));

-- Existing rows are read through this migration snapshot; later rows default unread.
-- Preserve 0 as the never-read sentinel, including for a row at the Unix epoch.
UPDATE threads
SET read_at_ms = CASE
    WHEN updated_at_ms = 0 THEN 1
    -- Values below the 2020 cutoff are legacy second-precision values. Read
    -- markers are always stored as strict milliseconds so their unit is
    -- unambiguous after the one-time backfill.
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
  OR (OLD.source_updated_at_ms IS NOT NULL
      AND NEW.source_updated_at_ms IS NOT OLD.source_updated_at_ms)
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

-- Legacy writers cannot toggle the token, so every explicit updated_at assignment
-- is a snapshot, including equal-second writes invisible to the millis trigger.
CREATE TRIGGER threads_read_at_after_legacy_updated_at_write
AFTER UPDATE OF updated_at ON threads
WHEN NEW.read_state_write_token IS OLD.read_state_write_token
BEGIN
    UPDATE threads
    SET read_at_ms = 0
    WHERE id = NEW.id;
END;
