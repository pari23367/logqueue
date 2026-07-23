import os
import pathlib
import subprocess
import sys

REPO_ROOT = pathlib.Path(__file__).parent.parent


def spawn_workers(
    database_url: str,
    count: int,
    sleep_min: str = "0",
    sleep_max: str = "0.01",
) -> list[subprocess.Popen]:
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    env["WORKER_SLEEP_MIN"] = sleep_min
    env["WORKER_SLEEP_MAX"] = sleep_max
    env["FAILURE_RATE"] = "0"
    env["PYTHONUNBUFFERED"] = "1"
    return [
        subprocess.Popen(
            [sys.executable, "-m", "app.worker"],
            cwd=REPO_ROOT,
            env=env,
        )
        for _ in range(count)
    ]


def stop_workers(processes: list[subprocess.Popen]) -> None:
    for proc in processes:
        proc.terminate()
    for proc in processes:
        proc.wait(timeout=5)
