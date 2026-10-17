#!/usr/bin/env python3
"""Simulated application write load on transaction_logs_flat.

Mix: ~90% INSERT, ~8% UPDATE, ~2% DELETE (updates/deletes hit recent rows, exercising every
sync-trigger path). Prints throughput and latency percentiles every --report seconds - the
evidence that the migration never blocks application writes. Reconnects automatically
(libpq multi-host + target_session_attrs=read-write), so it also rides through a failover.

Usage:  . migration/env.sh && python migration/load_generator.py --rate 50
"""
import argparse
import random
import re
import signal
import sys
import time
from datetime import datetime

import psycopg

_stop = False
STATUSES = ["completed", "pending", "failed", "refunded"]


def on_signal(_signum, _frame):
    global _stop
    _stop = True


def ts():
    return datetime.now().strftime("%H:%M:%S")


def first_line(exc):
    text = str(exc).strip()
    return text.splitlines()[0] if text else type(exc).__name__


def pct(sorted_vals, p):
    if not sorted_vals:
        return 0.0
    return sorted_vals[min(len(sorted_vals) - 1, int(round(p / 100.0 * (len(sorted_vals) - 1))))]


def connect():
    while not _stop:
        try:
            conn = psycopg.connect(autocommit=True, application_name="pg-ha-loadgen")
            print(f"{ts()} connected via PgBouncer at {conn.info.host}", flush=True)
            return conn
        except psycopg.OperationalError as exc:
            print(f"{ts()} connect failed: {first_line(exc)} - retrying", flush=True)
            time.sleep(1)
    return None


def main():
    p = argparse.ArgumentParser(description="Simulated application write load.")
    p.add_argument("--rate", type=float, default=50, help="target operations per second (default 50)")
    p.add_argument("--table", default="transaction_logs_flat", help="table or view to write to")
    p.add_argument("--report", type=float, default=5, help="report interval in seconds (default 5)")
    p.add_argument("--duration", type=float, default=0, help="stop after N seconds (0 = until Ctrl+C)")
    args = p.parse_args()
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", args.table):
        sys.exit("invalid table name")

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    t = args.table
    sql_ins = f"INSERT INTO {t} (user_id, amount, status) VALUES (%s, %s, %s) RETURNING id"
    sql_upd = f"UPDATE {t} SET status = %s WHERE id = %s"
    sql_del = f"DELETE FROM {t} WHERE id = %s"

    conn = connect()
    if conn is None:
        return 1
    max_id = conn.execute(f"SELECT COALESCE(max(id), 0) FROM {t}").fetchone()[0]

    interval = 1.0 / args.rate
    lat, kinds = [], {"insert": 0, "update": 0, "delete": 0}
    total = errors = 0
    worst = 0.0
    start = win_start = next_t = time.monotonic()
    print(f"{ts()} load started: {args.rate:.0f} ops/s on {t} (Ctrl+C to stop)", flush=True)

    while not _stop:
        r = random.random()
        t0 = time.perf_counter()
        try:
            if r < 0.90 or max_id == 0:
                max_id = conn.execute(sql_ins, (random.randint(1, 100000),
                                                round(random.uniform(1, 1000), 2),
                                                "pending")).fetchone()[0]
                kind = "insert"
            elif r < 0.98:
                conn.execute(sql_upd, (random.choice(STATUSES), random.randint(max(1, max_id - 5000), max_id)))
                kind = "update"
            else:
                conn.execute(sql_del, (random.randint(max(1, max_id - 5000), max_id),))
                kind = "delete"
            ms = (time.perf_counter() - t0) * 1000
            lat.append(ms)
            worst = max(worst, ms)
            kinds[kind] += 1
            total += 1
        except psycopg.Error as exc:
            errors += 1
            print(f"{ts()} ERROR {type(exc).__name__}: {first_line(exc)} - reconnecting", flush=True)
            try:
                conn.close()
            except Exception:
                pass
            conn = connect()
            if conn is None:
                break

        now = time.monotonic()
        if now - win_start >= args.report:
            s = sorted(lat)
            print(f"{ts()} {len(s) / (now - win_start):6.1f} ops/s | latency ms p50 {pct(s, 50):6.1f} "
                  f"p95 {pct(s, 95):6.1f} p99 {pct(s, 99):6.1f} max {(s[-1] if s else 0):7.1f} | "
                  f"ins {kinds['insert']} upd {kinds['update']} del {kinds['delete']} | errors {errors}",
                  flush=True)
            lat, win_start = [], now
        if args.duration and now - start >= args.duration:
            break

        next_t += interval
        delay = next_t - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        else:
            next_t = time.monotonic()   # running behind - don't try to "catch up" in a burst

    elapsed = time.monotonic() - start
    print(f"{ts()} SUMMARY: {total} ops in {elapsed:.0f}s ({total / max(elapsed, 1):.1f} ops/s), "
          f"worst latency {worst:.1f} ms, errors {errors}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
