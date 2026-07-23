import os
import random
import sys

import requests

API_URL = os.environ.get("API_URL", "http://localhost:8000")
LEVELS = ["INFO", "WARN", "ERROR", "DEBUG"]


def make_log_line() -> str:
    level = random.choice(LEVELS)
    return f"2026-07-23T00:00:00 {level} sample log message {random.randint(1, 100000)}"


def main(count: int) -> None:
    for i in range(1, count + 1):
        response = requests.post(f"{API_URL}/tasks", json={"log_line": make_log_line()})
        response.raise_for_status()
        if i % 20 == 0 or i == count:
            print(f"seeded {i}/{count}")


if __name__ == "__main__":
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 200
    main(count)
