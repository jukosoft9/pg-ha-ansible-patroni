-- legacy, unpartitioned table
-- IF NOT EXISTS keeps the script safely re-runnable.
CREATE TABLE IF NOT EXISTS transaction_logs_flat (
    id         BIGSERIAL PRIMARY KEY,
    user_id    INT NOT NULL,
    amount     NUMERIC(10, 2) NOT NULL,
    status     VARCHAR(20) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index to simulate production workload
CREATE INDEX IF NOT EXISTS idx_logs_flat_created_at ON transaction_logs_flat (created_at);
