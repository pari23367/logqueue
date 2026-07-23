import os
import pathlib

# app.worker / app.reaper read DATABASE_URL at import time; test modules
# import functions from them during collection, before the database_url
# fixture runs. Capture whatever was genuinely provided (if anything)
# before injecting a placeholder to satisfy that import -- the fixture
# below must key off this captured value, not a re-read of os.environ,
# or it can't tell a real URL from the placeholder.
_REAL_DATABASE_URL = os.environ.get("DATABASE_URL")
os.environ.setdefault("DATABASE_URL", "postgresql://placeholder/placeholder")

import psycopg
import pytest
from psycopg.rows import dict_row

SCHEMA_PATH = pathlib.Path(__file__).parent.parent / "schema.sql"


def _apply_schema(url: str) -> None:
    conn = psycopg.connect(url, autocommit=True)
    try:
        conn.execute(SCHEMA_PATH.read_text())
    finally:
        conn.close()


@pytest.fixture(scope="session")
def database_url():
    if _REAL_DATABASE_URL:
        yield _REAL_DATABASE_URL
        return

    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16") as postgres:
        url = postgres.get_connection_url(driver=None)
        _apply_schema(url)
        yield url


@pytest.fixture(autouse=True)
def clean_tables(database_url):
    conn = psycopg.connect(database_url, autocommit=True)
    try:
        conn.execute("TRUNCATE task_executions, tasks RESTART IDENTITY CASCADE")
    finally:
        conn.close()


@pytest.fixture
def db_conn(database_url):
    conn = psycopg.connect(database_url, row_factory=dict_row, autocommit=False)
    yield conn
    conn.close()
