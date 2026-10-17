-- populate transaction_logs_flat with 2,000,000 rows spanning the past 12 months.
--  * Rows are generated in time order, so ids increase with created_at
--  * Loaded in 8 separately-committed chunks of 250k (via \gexec): no single huge transaction,
--    and WAL is shipped to the replicas steadily instead of in one burst.
--  * Skips seeding if the table already has data (re-runnable).
\set total 2000000
\set chunk 250000
\timing on

SELECT count(*) = 0 AS is_empty FROM transaction_logs_flat \gset
\if :is_empty
  \echo 'Seeding' :total 'rows in chunks of' :chunk '...'
  SELECT format($sql$
      INSERT INTO transaction_logs_flat (user_id, amount, status, created_at)
      SELECT 1 + floor(random() * 100000)::int,
             round((1 + random() * 999)::numeric, 2),
             (ARRAY['completed','completed','completed','completed','pending','failed','refunded'])[1 + floor(random() * 7)::int],
             now() - interval '365 days' + g * (interval '365 days' / %1$s) + random() * interval '10 seconds'
      FROM generate_series(%2$s, %3$s) AS g
      ORDER BY g
  $sql$, :total, s, LEAST(s + :chunk - 1, :total))
  FROM generate_series(1, :total, :chunk) AS s
  \gexec
  VACUUM (ANALYZE) transaction_logs_flat;
\else
  \echo 'transaction_logs_flat already has data - skipping seed.'
\endif
