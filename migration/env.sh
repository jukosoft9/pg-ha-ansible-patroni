# Source me:  . migration/env.sh
# libpq settings shared by psql and the Python scripts (psycopg reads the same PG* variables).
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PGHOST="192.168.56.11,192.168.56.12,192.168.56.13"   # all PgBouncers
export PGPORT=6432
export PGDATABASE=appdb
export PGUSER=app_user
export PGSSLMODE=verify-full
export PGSSLROOTCERT="$REPO/ansible/pki/ca.crt"
export PGTARGETSESSIONATTRS=read-write                      # libpq picks the current leader
export PGCONNECT_TIMEOUT=3
if [ -z "${PGPASSWORD:-}" ]; then
  PGPASSWORD="$(ansible-vault view "$REPO/ansible/group_vars/all/vault.yml" \
      --vault-password-file ~/.vault_pass_pg-ha | awk -F'"' '/^vault_app_user_password/{print $2}')"
  export PGPASSWORD
fi
echo "libpq env set: user=$PGUSER db=$PGDATABASE hosts=$PGHOST (TLS verify-full, read-write)"
