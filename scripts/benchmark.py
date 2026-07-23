import os
import pathlib
import time

import psycopg
import requests
from psycopg.rows import dict_row

REPO_ROOT = pathlib.Path(__file__).parent.parent

API_URL = os.environ.get("API_URL", "http://localhost:8000")
DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://taskqueue:change_me@localhost:5432/taskqueue"
)

WORKER_COUNTS = [1, 2, 4, 8]
TASK_COUNT = 200
SETTLE_SECONDS = 3
DRAIN_TIMEOUT_SECONDS = 120


def scale_workers(count: int) -> None:
    import subprocess

    env = os.environ.copy()
    env["FAILURE_RATE"] = "0"
    subprocess.run(
        ["docker", "compose", "up", "-d", "--scale", f"worker={count}"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        check=True,
    )
    time.sleep(SETTLE_SECONDS)


def reset_tables(conn: psycopg.Connection) -> None:
    conn.execute("TRUNCATE task_executions, tasks RESTART IDENTITY CASCADE")


def enqueue_tasks(count: int) -> None:
    for i in range(count):
        response = requests.post(
            f"{API_URL}/tasks",
            json={"log_line": f"2026-01-01T00:00:00 INFO bench-{i}"},
        )
        response.raise_for_status()


def wait_for_drain(conn: psycopg.Connection, count: int) -> None:
    deadline = time.monotonic() + DRAIN_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        remaining = conn.execute(
            "SELECT count(*) AS n FROM tasks WHERE status != 'completed'"
        ).fetchone()["n"]
        if remaining == 0:
            return
        time.sleep(0.2)
    raise TimeoutError(f"tasks did not drain within {DRAIN_TIMEOUT_SECONDS}s")


def percentile(data: list[float], pct: float) -> float:
    if not data:
        return 0.0
    data = sorted(data)
    k = (len(data) - 1) * (pct / 100)
    f = int(k)
    c = min(f + 1, len(data) - 1)
    if f == c:
        return data[f]
    return data[f] + (data[c] - data[f]) * (k - f)


def measure_latencies(conn: psycopg.Connection) -> list[float]:
    rows = conn.execute(
        """
        SELECT t.created_at, e.first_claimed_at
        FROM tasks t
        JOIN (
            SELECT task_id, MIN(started_at) AS first_claimed_at
            FROM task_executions
            GROUP BY task_id
        ) e ON e.task_id = t.id
        """
    ).fetchall()
    return [(r["first_claimed_at"] - r["created_at"]).total_seconds() for r in rows]


def run_for_worker_count(conn: psycopg.Connection, count: int) -> dict:
    print(f"scaling to {count} worker(s)...", flush=True)
    scale_workers(count)
    reset_tables(conn)

    start = time.monotonic()
    enqueue_tasks(TASK_COUNT)
    wait_for_drain(conn, TASK_COUNT)
    elapsed = time.monotonic() - start

    latencies = measure_latencies(conn)
    throughput = TASK_COUNT / elapsed

    return {
        "workers": count,
        "throughput": throughput,
        "p50": percentile(latencies, 50),
        "p95": percentile(latencies, 95),
        "p99": percentile(latencies, 99),
    }


def print_markdown_table(results: list[dict]) -> None:
    print()
    print("| Workers | Throughput (tasks/sec) | p50 latency (s) | p95 latency (s) | p99 latency (s) |")
    print("|---|---|---|---|---|")
    for r in results:
        print(
            f"| {r['workers']} | {r['throughput']:.2f} | "
            f"{r['p50']:.3f} | {r['p95']:.3f} | {r['p99']:.3f} |"
        )


def main() -> None:
    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row, autocommit=True)

    results = []
    for count in WORKER_COUNTS:
        result = run_for_worker_count(conn, count)
        results.append(result)
        print(
            f"workers={count} throughput={result['throughput']:.2f}/s "
            f"p50={result['p50']:.3f}s p95={result['p95']:.3f}s p99={result['p99']:.3f}s",
            flush=True,
        )

    print("restoring default worker count (4)...", flush=True)
    scale_workers(4)

    print_markdown_table(results)


if __name__ == "__main__":
    main()
