import os

import psycopg
from psycopg.rows import dict_row

DATABASE_URL = os.environ["DATABASE_URL"]


def main() -> None:
    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    try:
        total_tasks = conn.execute(
            "SELECT COUNT(DISTINCT task_id) AS n FROM task_executions"
        ).fetchone()["n"]
        total_executions = conn.execute(
            "SELECT COUNT(*) AS n FROM task_executions"
        ).fetchone()["n"]
        duplicates = conn.execute(
            "SELECT task_id, COUNT(*) AS count FROM task_executions "
            "GROUP BY task_id HAVING COUNT(*) > 1 ORDER BY task_id"
        ).fetchall()
    finally:
        conn.close()

    print(f"total tasks executed: {total_tasks}")
    print(f"total executions: {total_executions}")
    print(f"tasks that ran more than once: {len(duplicates)}")
    for row in duplicates:
        print(f"  task_id={row['task_id']} executions={row['count']}")


if __name__ == "__main__":
    main()
