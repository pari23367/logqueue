import os
import pathlib
import random
import subprocess
import sys
import time
from datetime import datetime, timezone

import psycopg
import requests
from psycopg.rows import dict_row

REPO_ROOT = pathlib.Path(__file__).parent.parent

API_URL = os.environ.get("API_URL", "http://localhost:8000")
DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://taskqueue:change_me@localhost:5432/taskqueue"
)

TASK_COUNT = 1000
CHAOS_DURATION_SECONDS = 90
KILL_INTERVAL_MIN = 3
KILL_INTERVAL_MAX = 5
RESTART_DELAY_SECONDS = 2
DRAIN_TIMEOUT_SECONDS = 300


def log_chaos_event(action: str, container: str) -> None:
    print(
        f"chaos action={action} container={container} "
        f"timestamp={datetime.now(timezone.utc).isoformat()}",
        flush=True,
    )


def enqueue_tasks(count: int) -> None:
    for i in range(count):
        response = requests.post(
            f"{API_URL}/tasks",
            json={"log_line": f"2026-01-01T00:00:00 INFO chaos-{i}"},
        )
        response.raise_for_status()
        if (i + 1) % 100 == 0:
            print(f"enqueued {i + 1}/{count}", flush=True)


def list_worker_containers() -> list[str]:
    result = subprocess.run(
        ["docker", "ps", "--filter", "name=logqueue-worker", "--format", "{{.Names}}"],
        capture_output=True,
        text=True,
        check=True,
    )
    return [name for name in result.stdout.splitlines() if name.strip()]


def kill_random_worker() -> None:
    workers = list_worker_containers()
    if not workers:
        return
    target = random.choice(workers)
    subprocess.run(["docker", "kill", target], capture_output=True, check=True)
    log_chaos_event("kill", target)


def restart_stack() -> None:
    subprocess.run(
        ["docker", "compose", "up", "-d"],
        cwd=REPO_ROOT,
        capture_output=True,
        check=True,
    )


def run_chaos_loop() -> int:
    kills = 0
    deadline = time.monotonic() + CHAOS_DURATION_SECONDS
    while time.monotonic() < deadline:
        time.sleep(random.uniform(KILL_INTERVAL_MIN, KILL_INTERVAL_MAX))
        kill_random_worker()
        kills += 1
        time.sleep(RESTART_DELAY_SECONDS)
        restart_stack()
    return kills


def wait_for_drain(conn: psycopg.Connection) -> None:
    deadline = time.monotonic() + DRAIN_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        remaining = conn.execute(
            "SELECT count(*) AS n FROM tasks WHERE status IN ('pending', 'in_progress')"
        ).fetchone()["n"]
        if remaining == 0:
            return
        print(f"draining: {remaining} tasks still pending/in_progress", flush=True)
        time.sleep(3)
    raise TimeoutError(f"tasks did not drain within {DRAIN_TIMEOUT_SECONDS}s")


def check_invariants(conn: psycopg.Connection):
    rows = conn.execute(
        """
        SELECT t.id, t.status, t.attempts, COALESCE(e.exec_count, 0) AS exec_count
        FROM tasks t
        LEFT JOIN (
            SELECT task_id, count(*) AS exec_count
            FROM task_executions
            GROUP BY task_id
        ) e ON e.task_id = t.id
        """
    ).fetchall()

    stuck = [r for r in rows if r["status"] not in ("completed", "dead_letter")]

    unexplained = []
    for r in rows:
        if r["status"] == "completed":
            expected = r["attempts"] + 1
        elif r["status"] == "dead_letter":
            expected = r["attempts"]
        else:
            continue
        if r["exec_count"] > expected:
            unexplained.append(r)

    return rows, stuck, unexplained


def print_summary(rows, stuck, unexplained, kills: int) -> None:
    completed = sum(1 for r in rows if r["status"] == "completed")
    dead_letter = sum(1 for r in rows if r["status"] == "dead_letter")
    total_executions = sum(r["exec_count"] for r in rows)
    retried = sum(1 for r in rows if r["attempts"] > 0)

    print()
    print("=== Chaos test summary ===")
    print(f"{'total tasks':<24} {len(rows)}")
    print(f"{'completed':<24} {completed}")
    print(f"{'dead_letter':<24} {dead_letter}")
    print(f"{'total executions':<24} {total_executions}")
    print(f"{'tasks that needed retry':<24} {retried}")
    print(f"{'duplicate executions':<24} {len(unexplained)}")
    print(f"{'stuck in_progress':<24} {len(stuck)}")
    print(f"{'workers killed':<24} {kills}")


def main() -> int:
    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row, autocommit=True)

    print(f"enqueuing {TASK_COUNT} tasks...", flush=True)
    enqueue_tasks(TASK_COUNT)

    print(f"running chaos for {CHAOS_DURATION_SECONDS}s...", flush=True)
    kills = run_chaos_loop()

    print("chaos period done, waiting for queue to drain...", flush=True)
    wait_for_drain(conn)

    rows, stuck, unexplained = check_invariants(conn)
    print_summary(rows, stuck, unexplained, kills)

    if stuck or unexplained:
        print()
        print("FAILED")
        if stuck:
            print(f"  {len(stuck)} tasks stuck outside completed/dead_letter")
        if unexplained:
            print(f"  {len(unexplained)} tasks with unexplained extra executions")
        return 1

    print()
    print("PASSED: zero task loss, zero unexplained duplicate executions")
    return 0


if __name__ == "__main__":
    sys.exit(main())
