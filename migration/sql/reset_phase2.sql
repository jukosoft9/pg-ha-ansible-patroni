-- Resets env so the whole migration can be demonstrated again from scratch.
-- Safe in any state: before migration, mid-backfill, after cutover or after purge.
\set ON_ERROR_STOP on
DO $$
DECLARE k "char";
BEGIN
    SELECT relkind INTO k FROM pg_class WHERE oid = to_regclass('transaction_logs_flat');
    IF k = 'v' THEN
        EXECUTE 'DROP VIEW transaction_logs_flat';            -- post-cutover compatibility view
    ELSIF k = 'r' THEN
        EXECUTE 'DROP TABLE transaction_logs_flat';           -- legacy table (drops its trigger too)
    END IF;
END $$;
DROP TABLE IF EXISTS transaction_logs_flat_retired;
DROP TABLE IF EXISTS transaction_logs_modern CASCADE;      -- partitions + owned id sequence
DROP SEQUENCE IF EXISTS transaction_logs_flat_id_seq;
DROP TABLE IF EXISTS migration_checkpoint, migration_config;
DROP FUNCTION IF EXISTS sync_logs_to_modern();
DROP FUNCTION IF EXISTS create_logs_partitions(timestamptz, int);
\echo 'Reset complete.'
