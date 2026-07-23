from psycopg.types.json import Jsonb

from app.reaper import LEASE_TIMEOUT_SECONDS, MAX_ATTEMPTS, reap_once


def _insert_task(db_conn):
    row = db_conn.execute(
        "INSERT INTO tasks (payload) VALUES (%s) RETURNING id",
        [Jsonb({"log_line": "2026-01-01T00:00:00 INFO test"})],
    ).fetchone()
    db_conn.commit()
    return row["id"]


def _simulate_dead_worker(db_conn, task_id, worker_id):
    db_conn.execute(
        "UPDATE tasks SET status = 'in_progress', locked_by = %s, "
        "locked_at = now() - (interval '1 second' * %s) WHERE id = %s",
        [worker_id, LEASE_TIMEOUT_SECONDS + 10, task_id],
    )
    db_conn.commit()


def _fetch(db_conn, task_id):
    row = db_conn.execute(
        "SELECT status, attempts, locked_by, locked_at FROM tasks WHERE id = %s",
        [task_id],
    ).fetchone()
    db_conn.commit()
    return row


def test_crash_recovery(db_conn):
    task_id = _insert_task(db_conn)
    _simulate_dead_worker(db_conn, task_id, "dead-worker-1")

    reap_once(db_conn)

    row = _fetch(db_conn, task_id)
    assert row["status"] == "pending"
    assert row["attempts"] == 1
    assert row["locked_by"] is None
    assert row["locked_at"] is None


def test_reaper_dlq(db_conn):
    task_id = _insert_task(db_conn)
    row = None

    for i in range(MAX_ATTEMPTS):
        _simulate_dead_worker(db_conn, task_id, f"dead-worker-{i}")
        reap_once(db_conn)
        row = _fetch(db_conn, task_id)
        if i < MAX_ATTEMPTS - 1:
            assert row["status"] == "pending"
        else:
            assert row["status"] == "dead_letter"

    assert row["attempts"] == MAX_ATTEMPTS
