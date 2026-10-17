#!/usr/bin/env python3
"""Online, throttled, checkpointed migration: transaction_logs_flat -> transaction_logs_modern.

Design
  * sql/sync_trigger.sql installs a trigger that mirrors every in-window change on the flat table
    into the partitioned table, in the same transaction as the application's write.
  * `run` backfills historical rows in keyset batches (by id) up to a high mark that is
    captured once, after the trigger is verified. Rows above the mark are covered by the trigger.
  * Each batch (INSERT ... ON CONFLICT DO NOTHING) and its checkpoint UPDATE commit in ONE
    transaction: a crash loses at most the uncommitted batch, which is simply redone on restart -
    no duplicates, no primary-key conflicts.
  * I/O throttling: a configurable sleep between batches, plus adaptive back-off while replicas lag.
  * Connections use libpq env vars (migration/env.sh): multi-host + target_session_attrs=read-write,
    so after a Patroni failover the script reconnects to the new leader and resumes.

Usage:  . migration/env.sh && python migration/migrate.py {run|status|verify|cutover|purge} --help
"""
import argparse
import logging
import signal
import sys
import time

import psycopg
from psycopg.rows import dict_row

JOB = "flat_to_modern"
log = logging.getLogger("migrate")
_stop_requested = False

CHECKPOINT_DDL = """
CREATE TABLE IF NOT EXISTS migration_checkpoint (
    job_name       text PRIMARY KEY,
    window_start   timestamptz NOT NULL,
    high_water_id  bigint      NOT NULL,
    start_id       bigint      NOT NULL,
    last_id        bigint      NOT NULL,
    rows_estimated bigint      NOT NULL,
    rows_copied    bigint      NOT NULL DEFAULT 0,
    batches_done   bigint      NOT NULL DEFAULT 0,
    status         text        NOT NULL DEFAULT 'running'
                   CHECK (status IN ('running', 'backfilled', 'cutover_done', 'purged')),
    started_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    finished_at    timestamptz
)
"""

TRIGGER_CHECK = """
SELECT 1 FROM pg_trigger
WHERE tgrelid = 'transaction_logs_flat'::regclass
  AND tgname = 'trg_sync_logs_to_modern' AND tgenabled <> 'D'
"""

# One statement: read the next keyset batch, copy it (idempotently), report what happened.
BATCH_SQL = """
WITH batch AS (
    SELECT id, user_id, amount, status, created_at
    FROM transaction_logs_flat
    WHERE id > %(last_id)s AND id <= %(hwm)s AND created_at >= %(window_start)s
    ORDER BY id
    LIMIT %(batch_size)s
), ins AS (
    INSERT INTO transaction_logs_modern (id, user_id, amount, status, created_at)
    SELECT id, user_id, amount, status, created_at FROM batch
    ON CONFLICT (id, created_at) DO NOTHING
    RETURNING 1
)
SELECT count(*), max(id), (SELECT count(*) FROM ins) FROM batch
"""

MISSING_SQL = """
SELECT count(*) FROM (
    SELECT id, user_id, amount, status, created_at FROM transaction_logs_flat WHERE created_at >= %s
    EXCEPT ALL
    SELECT id, user_id, amount, status, created_at FROM transaction_logs_modern
) d
"""

EXTRA_SQL = """
SELECT count(*) FROM (
    SELECT id, user_id, amount, status, created_at FROM transaction_logs_modern
    EXCEPT ALL
    SELECT id, user_id, amount, status, created_at FROM transaction_logs_flat WHERE created_at >= %s
) d
"""

FIX_SQL = [
    # rows missing from the target
    """INSERT INTO transaction_logs_modern (id, user_id, amount, status, created_at)
       SELECT f.id, f.user_id, f.amount, f.status, f.created_at FROM transaction_logs_flat f
       WHERE f.created_at >= %(w)s AND NOT EXISTS (
           SELECT 1 FROM transaction_logs_modern m WHERE m.id = f.id AND m.created_at = f.created_at)
       ON CONFLICT (id, created_at) DO NOTHING""",
    # rows whose content differs
    """UPDATE transaction_logs_modern m
       SET user_id = f.user_id, amount = f.amount, status = f.status
       FROM transaction_logs_flat f
       WHERE f.id = m.id AND f.created_at = m.created_at
         AND (m.user_id, m.amount, m.status) IS DISTINCT FROM (f.user_id, f.amount, f.status)""",
    # rows in the target that no longer exist in the source window
    """DELETE FROM transaction_logs_modern m
       WHERE NOT EXISTS (SELECT 1 FROM transaction_logs_flat f
                         WHERE f.id = m.id AND f.created_at = m.created_at AND f.created_at >= %(w)s)""",
]

CUTOVER_SQL = [
    "DROP TRIGGER IF EXISTS trg_sync_logs_to_modern ON transaction_logs_flat",
    "ALTER TABLE transaction_logs_flat RENAME TO transaction_logs_flat_retired",
    # Compatibility view: the application keeps using the old name, writes land in the new table.
    "CREATE VIEW transaction_logs_flat AS "
    "SELECT id, user_id, amount, status, created_at FROM transaction_logs_modern",
    "ALTER VIEW transaction_logs_flat ALTER COLUMN id SET DEFAULT nextval('transaction_logs_flat_id_seq')",
    "ALTER VIEW transaction_logs_flat ALTER COLUMN created_at SET DEFAULT now()",
    "COMMENT ON VIEW transaction_logs_flat IS "
    "'Post-cutover compatibility view: reads/writes go to transaction_logs_modern'",
]


# --------------------------------------------------------------------------- helpers
def first_line(exc):
    text = str(exc).strip()
    return text.splitlines()[0] if text else type(exc).__name__


def nap(seconds):
    """Sleep that wakes up promptly on Ctrl+C or SIGTERM."""
    end = time.monotonic() + seconds
    while not _stop_requested:
        left = end - time.monotonic()
        if left <= 0:
            break
        time.sleep(min(0.2, left))


def on_signal(signum, _frame):
    global _stop_requested
    _stop_requested = True
    log.warning("signal %s received - stopping after the current batch (checkpoint stays consistent)", signum)


class Db:
    """Connection holder: (re)connects through libpq multi-host and retries transient failures."""

    def __init__(self, retries):
        self.retries = retries
        self.conn = None

    def get(self):
        if self.conn is None or self.conn.closed or self.conn.broken:
            self.conn = self._connect()
        return self.conn

    def reset(self):
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                pass
        self.conn = None

    def _connect(self):
        delay = 1.0
        for attempt in range(1, self.retries + 1):
            try:
                conn = psycopg.connect(autocommit=True, application_name="pg-ha-migrate")
                log.info("connected to the primary via PgBouncer at %s", conn.info.host)
                return conn
            except psycopg.OperationalError as exc:
                log.warning("connect attempt %d/%d failed: %s", attempt, self.retries, first_line(exc))
                time.sleep(delay)
                delay = min(delay * 2, 10)
        raise SystemExit("could not reach a read-write node")

    def run(self, fn, what):
        """Run fn(conn); on connection loss, failover, timeout or lock wait: reconnect and retry."""
        delay = 1.0
        for attempt in range(1, self.retries + 1):
            try:
                return fn(self.get())
            except (psycopg.OperationalError, psycopg.errors.ReadOnlySqlTransaction,
                    psycopg.errors.SerializationFailure, psycopg.errors.DeadlockDetected) as exc:
                log.warning("%s failed (%s: %s) - retry %d/%d in %.0fs",
                            what, type(exc).__name__, first_line(exc), attempt, self.retries, delay)
                self.reset()
                time.sleep(delay)
                delay = min(delay * 2, 15)
        raise SystemExit(f"{what}: giving up after {self.retries} attempts")


def relkind(conn, name):
    row = conn.execute("SELECT relkind FROM pg_class WHERE oid = to_regclass(%s)", (name,)).fetchone()
    return row[0] if row else None


def get_job(conn):
    cur = conn.cursor(row_factory=dict_row)
    if relkind(conn, "migration_checkpoint") is None:
        return None
    return cur.execute("SELECT * FROM migration_checkpoint WHERE job_name = %s", (JOB,)).fetchone()


def replica_lag_mb(conn):
    """Max WAL replay lag across replicas in MB; None if not visible (needs pg_monitor)."""
    n, visible, lag = conn.execute(
        "SELECT count(*), count(replay_lsn), "
        "COALESCE(max(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)), 0) "
        "FROM pg_stat_replication").fetchone()
    if n and not visible:
        return None
    return float(lag) / 1048576


# --------------------------------------------------------------------------- run (backfill)
def init_job(conn):
    with conn.transaction():
        conn.execute(CHECKPOINT_DDL)
        job = get_job(conn)
        if job:
            log.info("checkpoint found - resuming job '%s' (status=%s, last_id=%s)",
                     JOB, job["status"], job["last_id"])
            return job
        if not conn.execute(TRIGGER_CHECK).fetchone():
            raise SystemExit("sync trigger not found/enabled - run migration/sql/sync_trigger.sql first")
        w = conn.execute("SELECT window_start FROM migration_config WHERE id = 1").fetchone()[0]
        # High mark captured AFTER the trigger is verified: anything committed later is mirrored.
        hwm = conn.execute("SELECT COALESCE(max(id), 0) FROM transaction_logs_flat").fetchone()[0]
        first = conn.execute("SELECT min(id) FROM transaction_logs_flat WHERE created_at >= %s",
                             (w,)).fetchone()[0]
        start_id = first - 1 if first is not None else hwm
        est = conn.execute("SELECT count(*) FROM transaction_logs_flat WHERE created_at >= %s AND id <= %s",
                           (w, hwm)).fetchone()[0]
        job = conn.cursor(row_factory=dict_row).execute(
            "INSERT INTO migration_checkpoint "
            "(job_name, window_start, high_water_id, start_id, last_id, rows_estimated) "
            "VALUES (%s, %s, %s, %s, %s, %s) RETURNING *",
            (JOB, w, hwm, start_id, start_id, est)).fetchone()
        log.info("new job '%s': window_start=%s high_water_id=%s start_id=%s rows_in_scope=%s",
                 JOB, w, hwm, start_id, est)
        return job


def cmd_run(args, db):
    if not 0.5 <= args.sleep <= 2.0:
        log.warning("--sleep %.2fs is outside the recommended 0.5-2.0s range", args.sleep)
    job = db.run(init_job, "initialise job")
    if job["status"] != "running":
        log.info("backfill already complete (status=%s) - nothing to do", job["status"])
        return 0

    t_start, batches, lag_warned = time.monotonic(), 0, False
    while not _stop_requested:
        def do_batch(conn):
            with conn.transaction():
                conn.execute(f"SET LOCAL statement_timeout = '{int(args.statement_timeout)}s'")
                conn.execute("SET LOCAL lock_timeout = '5s'")
                # FOR UPDATE: single source of truth + prevents two concurrent runs of this job
                last_id, hwm, w, start_id = conn.execute(
                    "SELECT last_id, high_water_id, window_start, start_id FROM migration_checkpoint "
                    "WHERE job_name = %s FOR UPDATE", (JOB,)).fetchone()
                scanned, max_id, inserted = conn.execute(BATCH_SQL, {
                    "last_id": last_id, "hwm": hwm, "window_start": w,
                    "batch_size": args.batch_size}).fetchone()
                if scanned == 0:
                    conn.execute("UPDATE migration_checkpoint SET status = 'backfilled', "
                                 "finished_at = now(), updated_at = now() WHERE job_name = %s", (JOB,))
                    return None
                copied, done = conn.execute(
                    "UPDATE migration_checkpoint SET last_id = %s, rows_copied = rows_copied + %s, "
                    "batches_done = batches_done + 1, updated_at = now() WHERE job_name = %s "
                    "RETURNING rows_copied, batches_done", (max_id, inserted, JOB)).fetchone()
                pct = 100.0 * (max_id - start_id) / max(1, hwm - start_id)
                return last_id, max_id, scanned, inserted, copied, done, pct

        t0 = time.monotonic()
        res = db.run(do_batch, "batch")
        ms = (time.monotonic() - t0) * 1000
        if res is None:
            log.info("BACKFILL COMPLETE in %.0fs - status set to 'backfilled'", time.monotonic() - t_start)
            return 0
        last_id, max_id, scanned, inserted, copied, done, pct = res
        batches += 1

        lag = db.run(replica_lag_mb, "lag check")
        lag_txt = "n/a" if lag is None else f"{lag:.1f}MB"
        log.info("batch %4d | ids %d..%d | scanned %5d inserted %5d | total %7d | %5.1f%% | %4.0f ms | "
                 "replica lag %s | sleep %.1fs",
                 done, last_id + 1, max_id, scanned, inserted, copied, pct, ms, lag_txt, args.sleep)

        if args.max_batches and batches >= args.max_batches:
            log.info("--max-batches %d reached - stopping (resume any time with 'run')", args.max_batches)
            return 0

        nap(args.sleep)  # I/O throttle between batches

        # Adaptive back-off: never let the backfill push replicas behind
        if args.max_lag_mb > 0:
            waited = 0.0
            while not _stop_requested:
                lag = db.run(replica_lag_mb, "lag check")
                if lag is None:
                    if not lag_warned:
                        log.warning("replica lag not visible (grant pg_monitor) - lag back-off disabled")
                        lag_warned = True
                    break
                if lag <= args.max_lag_mb or waited >= 300:
                    break
                log.warning("replica lag %.1fMB > %.1fMB - backing off", lag, args.max_lag_mb)
                nap(args.sleep * 2)
                waited += args.sleep * 2

    log.info("stopped by signal after %d batch(es) - re-run 'run' to resume from the checkpoint", batches)
    return 0


# --------------------------------------------------------------------------- status / verify
def cmd_status(args, db):
    def show(conn):
        job = get_job(conn)
        if not job:
            print("no migration job yet (run 'migrate.py run')")
            return 0
        span = max(1, job["high_water_id"] - job["start_id"])
        pct = 100.0 * (job["last_id"] - job["start_id"]) / span
        target = conn.execute("SELECT count(*) FROM transaction_logs_modern").fetchone()[0]
        for k in ("job_name", "status", "window_start", "start_id", "high_water_id", "last_id",
                  "rows_estimated", "rows_copied", "batches_done", "started_at", "updated_at", "finished_at"):
            print(f"{k:>15}: {job[k]}")
        print(f"{'progress':>15}: {pct:.1f}% of id range")
        print(f"{'rows in target':>15}: {target} (backfilled + mirrored by trigger)")
        return 0
    return db.run(show, "status")


def verify(db, fix=False):
    def check(conn):
        if relkind(conn, "transaction_logs_flat") == "v":
            log.info("transaction_logs_flat is already the post-cutover view - nothing to compare")
            return None
        with conn.transaction():
            # One snapshot for both tables, so concurrent writes can't produce false differences
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            w = conn.execute("SELECT window_start FROM migration_config WHERE id = 1").fetchone()[0]
            src = conn.execute("SELECT count(*) FROM transaction_logs_flat WHERE created_at >= %s",
                               (w,)).fetchone()[0]
            dst = conn.execute("SELECT count(*) FROM transaction_logs_modern").fetchone()[0]
            missing = conn.execute(MISSING_SQL, (w,)).fetchone()[0]
            extra = conn.execute(EXTRA_SQL, (w,)).fetchone()[0]
        return w, src, dst, missing, extra

    res = db.run(check, "verify")
    if res is None:
        return 0
    w, src, dst, missing, extra = res
    log.info("verify @ one snapshot: source window rows=%d target rows=%d missing/different=%d extra=%d",
             src, dst, missing, extra)
    if missing == 0 and extra == 0:
        log.info("VERIFY OK - target matches the source window exactly")
        return 0
    if not fix:
        log.error("VERIFY FAILED - re-run with --fix to reconcile")
        return 1

    def repair(conn):
        with conn.transaction():
            for sql in FIX_SQL:
                conn.execute(sql, {"w": w})
    db.run(repair, "reconcile")
    log.info("reconciliation applied - verifying again")
    return verify(db, fix=False)


def cmd_verify(args, db):
    return verify(db, args.fix)


# --------------------------------------------------------------------------- cutover / purge
def cmd_cutover(args, db):
    conn = db.get()
    if relkind(conn, "transaction_logs_flat") == "v":
        log.info("already cut over (transaction_logs_flat is the compatibility view)")
        return 0
    job = get_job(conn)
    if not job or job["status"] != "backfilled":
        log.error("backfill not complete (status=%s) - run 'migrate.py run' first",
                  job["status"] if job else None)
        return 1
    if not args.skip_verify and verify(db) != 0:
        return 1

    for attempt in range(1, args.attempts + 1):
        try:
            conn = db.get()
            with conn.transaction():
                conn.execute(f"SET LOCAL lock_timeout = '{int(args.lock_timeout_ms)}ms'")
                t_req = time.monotonic()
                conn.execute("LOCK TABLE transaction_logs_flat IN ACCESS EXCLUSIVE MODE")
                t_lock = time.monotonic()
                for sql in CUTOVER_SQL:
                    conn.execute(sql)
                conn.execute("UPDATE migration_checkpoint SET status = 'cutover_done', updated_at = now() "
                             "WHERE job_name = %s", (JOB,))
            t_done = time.monotonic()
            log.info("CUTOVER COMMITTED on attempt %d: waited %.0f ms for the lock, held it %.0f ms",
                     attempt, (t_lock - t_req) * 1000, (t_done - t_lock) * 1000)
            log.info("transaction_logs_flat is now a view over transaction_logs_modern; "
                     "legacy table renamed to transaction_logs_flat_retired")
            return 0
        except psycopg.errors.LockNotAvailable:
            log.warning("attempt %d/%d: lock not granted within %d ms (busy) - backing off",
                        attempt, args.attempts, args.lock_timeout_ms)
            time.sleep(min(2 * attempt, 10))
        except psycopg.OperationalError as exc:
            log.warning("attempt %d/%d: %s - reconnecting", attempt, args.attempts, first_line(exc))
            db.reset()
            time.sleep(2)
    log.error("cutover not done after %d attempts - nothing changed, safe to retry", args.attempts)
    return 1


def cmd_purge(args, db):
    def purge(conn):
        job = get_job(conn)
        status = job["status"] if job else None
        if status == "purged":
            log.info("already purged")
            return 0
        if status != "cutover_done":
            log.error("cutover not done (status=%s) - refusing to drop anything", status)
            return 1
        with conn.transaction():
            conn.execute("SET LOCAL lock_timeout = '5s'")
            size = conn.execute("SELECT pg_size_pretty(pg_total_relation_size(to_regclass("
                                "'transaction_logs_flat_retired')))").fetchone()[0]
            conn.execute("DROP TABLE IF EXISTS transaction_logs_flat_retired")
            conn.execute("DROP FUNCTION IF EXISTS sync_logs_to_modern()")
            conn.execute("UPDATE migration_checkpoint SET status = 'purged', updated_at = now() "
                         "WHERE job_name = %s", (JOB,))
        log.info("PURGED: dropped the legacy flat table (%s) incl. rows older than the window, "
                 "and the sync trigger function", size)
        return 0
    return db.run(purge, "purge")


# --------------------------------------------------------------------------- main
def main():
    p = argparse.ArgumentParser(description="Online migration of transaction_logs_flat to the "
                                            "monthly-partitioned transaction_logs_modern.")
    p.add_argument("--retries", type=int, default=20, help="reconnect/retry attempts (default 20)")
    sub = p.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("run", help="throttled, resumable backfill")
    r.add_argument("--batch-size", type=int, default=25000, help="rows per batch (default 25000)")
    r.add_argument("--sleep", type=float, default=1.0, help="seconds between batches (default 1.0)")
    r.add_argument("--max-lag-mb", type=float, default=64,
                   help="back off while replica replay lag exceeds this (0 = off, default 64)")
    r.add_argument("--max-batches", type=int, default=0, help="stop after N batches (demo; 0 = run to end)")
    r.add_argument("--statement-timeout", type=int, default=120, help="per-batch timeout in seconds")

    sub.add_parser("status", help="show checkpoint/progress")
    v = sub.add_parser("verify", help="compare source window vs target at one snapshot")
    v.add_argument("--fix", action="store_true", help="reconcile differences, then re-verify")
    c = sub.add_parser("cutover", help="swap in the new table (short lock, retried)")
    c.add_argument("--lock-timeout-ms", type=int, default=1000, help="max wait for the lock per attempt")
    c.add_argument("--attempts", type=int, default=10)
    c.add_argument("--skip-verify", action="store_true")
    sub.add_parser("purge", help="drop the retired legacy table (archive/purge)")

    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s",
                        datefmt="%H:%M:%S")
    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)
    db = Db(args.retries)
    handlers = {"run": cmd_run, "status": cmd_status, "verify": cmd_verify,
                "cutover": cmd_cutover, "purge": cmd_purge}
    return handlers[args.cmd](args, db)


if __name__ == "__main__":
    sys.exit(main())
