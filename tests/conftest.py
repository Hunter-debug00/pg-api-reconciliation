"""Pytest fixtures and configuration for PostgreSQL reconciliation tests."""

import os
from pathlib import Path
import httpx
import psycopg
import pytest

DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/reconcile_bench",
)

ROOT_DIR = Path(__file__).resolve().parent.parent
SCHEMA_PATH = ROOT_DIR / "sql" / "schema.sql"


@pytest.fixture(scope="module")
def db_conn():
    """Module-scoped connection to the PostgreSQL test database."""
    with psycopg.connect(DB_URL) as conn:
        yield conn


def canned_transport(pages: list) -> httpx.MockTransport:
    """Mock transport delivering canned JSON responses for edge case testing."""
    state = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        i = min(state["n"], len(pages) - 1)
        state["n"] += 1
        return httpx.Response(200, json=pages[i])

    return httpx.MockTransport(handler)
