-- live sync trigger (flat -> modern) for rows inside the migration window.
-- Runs in the SAME transaction as the application's write, so both tables commit together.
CREATE OR REPLACE FUNCTION sync_logs_to_modern()
RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    w timestamptz;
BEGIN
    SELECT window_start INTO w FROM migration_config WHERE id = 1;

    -- UPDATE/DELETE: remove the old version (handles a changed created_at too)
    IF TG_OP IN ('UPDATE', 'DELETE') THEN
        DELETE FROM transaction_logs_modern
        WHERE id = OLD.id AND created_at = OLD.created_at;
    END IF;

    -- INSERT/UPDATE: upsert the new version if it falls inside the window.
    -- Upsert (not DO NOTHING) so a concurrent backfill can never leave a stale version behind.
    IF TG_OP IN ('INSERT', 'UPDATE') AND NEW.created_at >= w THEN
        INSERT INTO transaction_logs_modern (id, user_id, amount, status, created_at)
        VALUES (NEW.id, NEW.user_id, NEW.amount, NEW.status, NEW.created_at)
        ON CONFLICT (id, created_at) DO UPDATE
            SET user_id = EXCLUDED.user_id,
                amount  = EXCLUDED.amount,
                status  = EXCLUDED.status;
    END IF;

    RETURN NULL;   -- AFTER trigger: return value is ignored
END $$;

-- Attaching a trigger briefly needs a SHARE ROW EXCLUSIVE lock on the flat table.
-- lock_timeout makes it give up (and be retried) rather than queue behind long
-- transactions and block application writes behind it.
BEGIN;
SET LOCAL lock_timeout = '3s';
CREATE OR REPLACE TRIGGER trg_sync_logs_to_modern
    AFTER INSERT OR UPDATE OR DELETE ON transaction_logs_flat
    FOR EACH ROW EXECUTE FUNCTION sync_logs_to_modern();
COMMIT;
