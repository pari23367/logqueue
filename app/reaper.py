import os
import time

import psycopg
from psycopg.rows import dict_row

DATABASE_URL = os.environ["DATABASE_URL"]

REAP_INTERVAL_SECONDS = 10
LEASE_TIMEOUT_SECONDS = 30
MAX_ATTEMPTS = 5


def log_reclaim(task_id: int, dead_worker_id: str) -> None:
    print(
        f"reaper task_id={task_id} dead_worker_id={dead_worker_id} action=reclaimed",
        flush=True,
    )


def reap_once(conn: psycopg.Connection) -> None:
    rows = conn.execute(
        """
        WITH stale AS (
            SELECT id, locked_by AS dead_worker, attempts
            FROM tasks
            WHERE status = 'in_progress'
              AND locked_at < now() - interval '30 seconds'
            FOR UPDATE SKIP LOCKED
        )
        UPDATE tasks
        SET status = CASE WHEN stale.attempts + 1 >= 5 THEN 'dead_letter' ELSE 'pending' END,
            attempts = stale.attempts + 1,
            locked_by = NULL,
            locked_at = NULL,
            run_after = now()
        FROM stale
        WHERE tasks.id = stale.id
        RETURNING tasks.id, stale.dead_worker
        """
    ).fetchall()
    conn.commit()
    for row in rows:
        log_reclaim(row["id"], row["dead_worker"])


def run() -> None:
    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row, autocommit=False)
    print("reaper started", flush=True)
    try:
        while True:
            try:
                reap_once(conn)
            except Exception as exc:
                conn.rollback()
                print(f"reaper action=error detail={type(exc).__name__}:{exc}", flush=True)
            time.sleep(REAP_INTERVAL_SECONDS)
    finally:
        conn.close()


if __name__ == "__main__":
    run()
