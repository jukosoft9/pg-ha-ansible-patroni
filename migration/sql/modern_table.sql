-- target partitioned table + monthly partitions. Re-runnable.

-- Target table
CREATE TABLE IF NOT EXISTS transaction_logs_modern (
    id         BIGINT NOT NULL,
    user_id    INT NOT NULL,
    amount     NUMERIC(10, 2) NOT NULL,
    status     VARCHAR(20) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

-- index created on the parent so every partition inherits it.
CREATE INDEX IF NOT EXISTS idx_logs_modern_created_at ON transaction_logs_modern (created_at);

-- Migration window, computed ONCE and shared by the sync trigger and the backfill script.
--    "Last 3 months" = from the 1st of the month three months back (UTC).
CREATE TABLE IF NOT EXISTS migration_config (
    id           INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),   -- single-row table
    window_start TIMESTAMPTZ NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO migration_config (window_start)
VALUES (date_trunc('month', (now() AT TIME ZONE 'UTC') - interval '3 months') AT TIME ZONE 'UTC')
ON CONFLICT (id) DO NOTHING;

-- Reusable partition maintenance: monthly partitions from p_from up to p_months_ahead
-- months after the current month. Idempotent; returns the partitions it created.
CREATE OR REPLACE FUNCTION create_logs_partitions(p_from timestamptz, p_months_ahead int DEFAULT 3)
RETURNS SETOF text
LANGUAGE plpgsql AS $$
DECLARE
    m    date;
    lo   timestamptz;
    hi   timestamptz;
    part text;
BEGIN
    FOR m IN
        SELECT d::date
        FROM generate_series(date_trunc('month', p_from AT TIME ZONE 'UTC'),
                             date_trunc('month', now() AT TIME ZONE 'UTC') + make_interval(months => p_months_ahead),
                             interval '1 month') AS d
    LOOP
        part := format('transaction_logs_modern_y%sm%s', to_char(m, 'YYYY'), to_char(m, 'MM'));
        IF to_regclass(part) IS NULL THEN
            lo := m::timestamp AT TIME ZONE 'UTC';
            hi := (m + interval '1 month')::timestamp AT TIME ZONE 'UTC';
            EXECUTE format('CREATE TABLE %I PARTITION OF transaction_logs_modern FOR VALUES FROM (%L) TO (%L)',
                           part, lo, hi);
            RETURN NEXT part;
        END IF;
    END LOOP;
END $$;

-- Window months + 3 future months
SELECT create_logs_partitions((SELECT window_start FROM migration_config), 3) AS created_partition;

-- Safety net: a row outside every range lands here instead of failing the insert.
-- Keep it empty in practice by always creating future partitions ahead of time (monitor its size).
CREATE TABLE IF NOT EXISTS transaction_logs_modern_default PARTITION OF transaction_logs_modern DEFAULT;

-- Cutover preparation: the new table takes over the legacy id sequence, so that
-- inserts after cutover keep the same id series, and dropping the flat table
-- later does not drop the sequence. Safe now: transaction_logs_modern has no traffic yet.
ALTER TABLE transaction_logs_modern ALTER COLUMN id SET DEFAULT nextval('transaction_logs_flat_id_seq');
ALTER TABLE transaction_logs_modern ALTER COLUMN created_at SET DEFAULT now();
ALTER SEQUENCE transaction_logs_flat_id_seq OWNED BY transaction_logs_modern.id;
