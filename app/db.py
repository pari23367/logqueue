import os
import pathlib

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

DATABASE_URL = os.environ["DATABASE_URL"]
SCHEMA_PATH = pathlib.Path(__file__).parent.parent / "schema.sql"

pool = ConnectionPool(
    DATABASE_URL,
    min_size=2,
    max_size=10,
    kwargs={"row_factory": dict_row},
    open=True,
)


def ensure_schema() -> None:
    # Docker Compose applies schema.sql via docker-entrypoint-initdb.d on
    # first boot; Neon doesn't run that, so apply it ourselves if the
    # tables aren't there yet. Harmless no-op when they already exist.
    conn = psycopg.connect(DATABASE_URL, autocommit=True)
    try:
        exists = conn.execute(
            "SELECT to_regclass('public.tasks') IS NOT NULL AS exists"
        ).fetchone()[0]
        if not exists:
            conn.execute(SCHEMA_PATH.read_text())
    finally:
        conn.close()
