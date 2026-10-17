#!/usr/bin/env bash
# Patroni callback (on_start / on_role_change). Args: <action> <role> <scope>
# PgBouncer caches server-reported parameters (in_hot_standby, default_transaction_read_only)
# per pool and replays them to new clients. After a role change those values are stale, so
# libpq's target_session_attrs=read-write picks the wrong node. Restarting the local PgBouncer
# clears the cache so clients see this node's real role.
set -uo pipefail
action="${1:-}"; role="${2:-}"; scope="${3:-}"
systemctl cat pgbouncer >/dev/null 2>&1 || exit 0   # PgBouncer not installed yet (first bootstrap)
logger -t patroni-callback "action=${action} role=${role} scope=${scope}: restarting pgbouncer"
sudo -n /usr/bin/systemctl restart pgbouncer
