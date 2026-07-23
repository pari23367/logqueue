import time

from psycopg.types.json import Jsonb

from tests.helpers import spawn_workers, stop_workers

TASK_COUNT = 500
WORKER_COUNT = 4
TIMEOUT_SECONDS = 30


def test_exactly_once(database_url, db_conn):
    for _ in range(TASK_COUNT):
        db_conn.execute(
            "INSERT INTO tasks (payload) VALUES (%s)",
            [Jsonb({"log_line": "2026-01-01T00:00:00 INFO test"})],
        )
    db_conn.commit()

    processes = spawn_workers(database_url, WORKER_COUNT)
    try:
        deadline = time.monotonic() + TIMEOUT_SECONDS
        remaining = TASK_COUNT
        while time.monotonic() < deadline:
            remaining = db_conn.execute(
                "SELECT count(*) AS n FROM tasks WHERE status != 'completed'"
            ).fetchone()["n"]
            if remaining == 0:
                break
            time.sleep(0.2)
        assert remaining == 0, "tasks did not complete within timeout"
    finally:
        stop_workers(processes)

    total = db_conn.execute("SELECT count(*) AS n FROM task_executions").fetchone()["n"]
    distinct = db_conn.execute(
        "SELECT count(DISTINCT task_id) AS n FROM task_executions"
    ).fetchone()["n"]

    assert total == TASK_COUNT
    assert distinct == TASK_COUNT
