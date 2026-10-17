#!/usr/bin/env bash
# Writes one row per second through PgBouncer and prints which node accepted it.
# Leader discovery: libpq multi-host + target_session_attrs=read-write
# Usage: export PGPASSWORD=<app_user password>; scripts/failover-probe.sh
set -u
: "${PGPASSWORD:?export PGPASSWORD (app_user) first}"
CA="${CA:-$(cd "$(dirname "$0")/.." && pwd)/ansible/pki/ca.crt}"
HOSTS="${HOSTS:-192.168.56.11,192.168.56.12,192.168.56.13}"
DSN="host=$HOSTS port=6432 dbname=appdb user=app_user sslmode=verify-full sslrootcert=$CA target_session_attrs=read-write connect_timeout=2"

psql "$DSN" -qc "CREATE TABLE IF NOT EXISTS failover_probe (id bigserial PRIMARY KEY, ts timestamptz NOT NULL DEFAULT now(), node text NOT NULL)" || exit 1

while true; do
  out=$(psql "$DSN" -Atc "INSERT INTO failover_probe (node) VALUES (split_part(current_setting('listen_addresses'), ',', 1)) RETURNING 'OK id=' || id || ' leader=' || node" 2>&1 | head -1)
  printf '%s  %s\n' "$(date +%T)" "$out"
  sleep 1
done
