from typing import Any

from fastapi import FastAPI, HTTPException
from psycopg.types.json import Jsonb
from pydantic import BaseModel

from app.db import pool

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
        rows = conn.execute(
            "SELECT status, count(*) AS count FROM tasks GROUP BY status"
        ).fetchall()
    return {row["status"]: row["count"] for row in rows}


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
