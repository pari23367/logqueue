from typing import Any

from fastapi import FastAPI, HTTPException
from psycopg.types.json import Jsonb
from pydantic import BaseModel

from app.db import pool
from app.reaper import LEASE_TIMEOUT_SECONDS

app = FastAPI()


class TaskCreate(BaseModel):
    log_line: str


@app.post("/tasks", status_code=201)
def create_task(body: TaskCreate) -> dict[str, Any]:
    with pool.connection() as conn:
        row = conn.execute(
            "INSERT INTO tasks (payload) VALUES (%s) RETURNING id",
            [Jsonb({"log_line": body.log_line})],
        ).fetchone()
    return {"id": row["id"]}


@app.get("/tasks/{task_id}")
def get_task(task_id: int) -> dict[str, Any]:
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT id, payload, status, attempts, run_after, locked_by, "
            "locked_at, created_at, completed_at FROM tasks WHERE id = %s",
            [task_id],
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="task not found")
    return row


@app.get("/stats")
def stats() -> dict[str, int]:
    with pool.connection() as conn:
        by_status_rows = conn.execute(
            "SELECT status, count(*) AS count FROM tasks GROUP BY status"
        ).fetchall()
        retrying = conn.execute(
            "SELECT count(*) AS n FROM tasks "
            "WHERE status = 'pending' AND run_after > now()"
        ).fetchone()
        stale_lease = conn.execute(
            "SELECT count(*) AS n FROM tasks WHERE status = 'in_progress' "
            "AND locked_at < now() - (INTERVAL '1 second' * %s)",
            [LEASE_TIMEOUT_SECONDS],
        ).fetchone()

    by_status = {row["status"]: row["count"] for row in by_status_rows}
    return {
        **by_status,
        "dead_letter_count": by_status.get("dead_letter", 0),
        "retrying_count": retrying["n"],
        "stale_lease_count": stale_lease["n"],
    }


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
