-- Proves the sync trigger mirrors INSERT, UPDATE and DELETE. Rolled back - leaves no data behind.
\set ON_ERROR_STOP on
BEGIN;
INSERT INTO transaction_logs_flat (user_id, amount, status) VALUES (42, 9.99, 'pending') RETURNING id AS test_id \gset
SELECT 'after INSERT' AS step, count(*) AS rows_in_modern, max(status) AS status FROM transaction_logs_modern WHERE id = :test_id;
UPDATE transaction_logs_flat SET status = 'completed' WHERE id = :test_id;
SELECT 'after UPDATE' AS step, count(*) AS rows_in_modern, max(status) AS status FROM transaction_logs_modern WHERE id = :test_id;
DELETE FROM transaction_logs_flat WHERE id = :test_id;
SELECT 'after DELETE' AS step, count(*) AS rows_in_modern, max(status) AS status FROM transaction_logs_modern WHERE id = :test_id;
ROLLBACK;
