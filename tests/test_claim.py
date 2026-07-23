from datetime import datetime, timedelta, timezone

from psycopg.types.json import Jsonb

from app.worker import claim_task


def _insert_task(db_conn, run_after=None):
    row = db_conn.execute(
        "INSERT INTO tasks (payload, run_after) VALUES (%s, COALESCE(%s, now())) RETURNING id",
        [Jsonb({"log_line": "2026-01-01T00:00:00 INFO test"}), run_after],
    ).fetchone()
    db_conn.commit()
    return row["id"]


def test_claim_skips_future_run_after(db_conn):
    future_id = _insert_task(
        db_conn, run_after=datetime.now(timezone.utc) + timedelta(hours=1)
    )
    ready_id = _insert_task(db_conn)

    claimed = claim_task(db_conn)

    assert claimed is not None
    assert claimed["id"] == ready_id
    assert claimed["id"] != future_id
