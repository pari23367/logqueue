import os
import random
import re
import socket
import threading
import time
import uuid
from datetime import datetime, timezone

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

DATABASE_URL = os.environ["DATABASE_URL"]
WORKER_ID = f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
FAILURE_RATE = float(os.environ.get("FAILURE_RATE", "0"))
MAX_ATTEMPTS = 5
HEARTBEAT_INTERVAL_SECONDS = 5
WORKER_SLEEP_MIN = float(os.environ.get("WORKER_SLEEP_MIN", "0.1"))
WORKER_SLEEP_MAX = float(os.environ.get("WORKER_SLEEP_MAX", "0.5"))

LOG_LINE_RE = re.compile(r"^(?P<timestamp>\S+)\s+(?P<level>\w+)\s+(?P<message>.*)$")


def log_event(action: str, task_id: int | str) -> None:
    print(
        f"worker_id={WORKER_ID} task_id={task_id} action={action} "
        f"timestamp={datetime.now(timezone.utc).isoformat()}",
        flush=True,
    )


def parse_log_line(line: str) -> dict:
    match = LOG_LINE_RE.match(line)
    if not match:
        return {"level": "UNKNOWN", "timestamp": None, "message": line}
    return match.groupdict()


def maybe_inject_failure() -> None:
    if random.random() < FAILURE_RATE:
        raise RuntimeError("injected failure (FAILURE_RATE)")


def claim_task(conn: psycopg.Connection) -> dict | None:
    row = conn.execute(
        """
        WITH next AS (
            SELECT id FROM tasks
            WHERE status = 'pending' AND run_after <= now()
            ORDER BY created_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE tasks
        SET status = 'in_progress', locked_by = %s, locked_at = now()
        FROM next
        WHERE tasks.id = next.id
        RETURNING tasks.id, tasks.payload
        """,
        [WORKER_ID],
    ).fetchone()
    if row is None:
        conn.rollback()
        log_event("claim_attempt_empty", "-")
        return None
    conn.execute(
        "INSERT INTO task_executions (task_id, worker_id) VALUES (%s, %s)",
        [row["id"], WORKER_ID],
    )
    conn.commit()
    log_event("claim_attempt_succeeded", row["id"])
    return row


def complete_task(conn: psycopg.Connection, task_id: int, parsed: dict) -> None:
    conn.execute(
        "UPDATE tasks SET status = 'completed', payload = payload || %s, "
        "completed_at = now() WHERE id = %s",
        [Jsonb({"parsed": parsed}), task_id],
    )
    conn.commit()


def fail_task(conn: psycopg.Connection, task_id: int) -> None:
    conn.execute(
        """
        UPDATE tasks
        SET attempts = attempts + 1,
            status = CASE WHEN attempts + 1 >= %s THEN 'dead_letter' ELSE 'pending' END,
            run_after = now() + (INTERVAL '1 second' * LEAST(power(2, attempts + 1), 300)),
            locked_by = NULL,
            locked_at = NULL
        WHERE id = %s
        """,
        [MAX_ATTEMPTS, task_id],
    )
    conn.commit()


def heartbeat_loop(conn: psycopg.Connection, stop_event: threading.Event, task_id: int) -> None:
    while not stop_event.wait(HEARTBEAT_INTERVAL_SECONDS):
        conn.execute(
            "UPDATE tasks SET locked_at = now() WHERE id = %s AND locked_by = %s",
            [task_id, WORKER_ID],
        )


def run() -> None:
    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row, autocommit=False)
    heartbeat_conn = psycopg.connect(DATABASE_URL, autocommit=True)
    log_event("worker_started", "-")
    try:
        while True:
            task = claim_task(conn)
            if task is None:
                time.sleep(0.5)
                continue
            log_event("claimed", task["id"])

            stop_heartbeat = threading.Event()
            hb_thread = threading.Thread(
                target=heartbeat_loop,
                args=(heartbeat_conn, stop_heartbeat, task["id"]),
                daemon=True,
            )
            hb_thread.start()
            try:
                time.sleep(random.uniform(WORKER_SLEEP_MIN, WORKER_SLEEP_MAX))
                maybe_inject_failure()
                parsed = parse_log_line(task["payload"]["log_line"])
                complete_task(conn, task["id"], parsed)
                log_event("completed", task["id"])
            except Exception as exc:
                fail_task(conn, task["id"])
                log_event(f"failed:{type(exc).__name__}", task["id"])
            finally:
                stop_heartbeat.set()
                hb_thread.join()
    finally:
        conn.close()
        heartbeat_conn.close()


if __name__ == "__main__":
    run()
