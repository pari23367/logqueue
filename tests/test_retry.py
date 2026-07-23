from psycopg.types.json import Jsonb

from app.worker import MAX_ATTEMPTS, fail_task


def _insert_task(db_conn):
    row = db_conn.execute(
        "INSERT INTO tasks (payload) VALUES (%s) RETURNING id",
        [Jsonb({"log_line": "2026-01-01T00:00:00 INFO test"})],
    ).fetchone()
    db_conn.commit()
    return row["id"]


def _fetch(db_conn, task_id):
    row = db_conn.execute(
        "SELECT status, attempts, run_after FROM tasks WHERE id = %s",
        [task_id],
    ).fetchone()
    db_conn.commit()
    return row


def test_retry_backoff(db_conn):
    task_id = _insert_task(db_conn)

    previous_gap = None
    for expected_attempts in range(1, MAX_ATTEMPTS + 1):
        before = db_conn.execute("SELECT now() AS n").fetchone()["n"]
        fail_task(db_conn, task_id)
        row = _fetch(db_conn, task_id)

        assert row["attempts"] == expected_attempts

        if expected_attempts < MAX_ATTEMPTS:
            assert row["status"] == "pending"
            gap = (row["run_after"] - before).total_seconds()
            assert gap > 0
            if previous_gap is not None:
                assert gap > previous_gap
            previous_gap = gap
        else:
            assert row["status"] == "dead_letter"
