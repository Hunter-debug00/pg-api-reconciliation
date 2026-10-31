"""Pytest fixtures and configuration for PostgreSQL reconciliation tests.

Provides database connection fixtures, mock HTTP transports, test dataset reset,
deterministic mutation triggers, and correctness evaluation oracle.
"""

import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import psycopg
import pytest

from pg_reconcile import (
    SweepExecutionStats,
    assert_disposable_database,
    ingest_subscription_transactional,
    run_strategy_incremental,
    run_strategy_naive_offset,
    run_strategy_snapshot_cursor,
    run_strategy_staged_keyset,
    run_strategy_two_pass_quarantine,
)
from pg_reconcile.simulator import STATE, app

DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/reconcile_bench",
)

ROOT_DIR = Path(__file__).resolve().parent.parent
SCHEMA_PATH = ROOT_DIR / "sql" / "schema.sql"

PAGE_SIZE = 100
BASE_TIME = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
SWEEP_START = BASE_TIME + timedelta(seconds=2)
MID_SWEEP_TIME = BASE_TIME + timedelta(seconds=15)
BACKDATED_SOURCE_TIME = BASE_TIME - timedelta(minutes=30)


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


def reset_environment(conn: psycopg.Connection) -> int:
    """Recreate tables and seed initial active subscriptions."""
    assert_disposable_database(conn)
    conn.rollback()
    with open(SCHEMA_PATH, encoding="utf-8") as f:
        schema_sql = f.read()

    with conn.cursor() as cur:
        cur.execute(schema_sql)

    STATE.seed_dataset(BASE_TIME)
    STATE.set_transient_omission(tenant_id="acme", record_id=3000, sweeps=1)

    with conn.cursor() as cur:
        # Disable trigger during initial bulk seed; re-enabled before any sweep or mutation runs.
        cur.execute(
            "ALTER TABLE destination_subscriptions DISABLE TRIGGER trg_enforce_tenant_epoch"
        )
        with cur.copy(
            """
            COPY destination_subscriptions
                (tenant_id, id, mrr_cents, is_deleted, source_created_at, updated_at,
                 ingested_at, ingestion_seq, ingestion_epoch, missing_streak)
            FROM STDIN
            """
        ) as copy:
            seq = 0
            for r in STATE.records.values():
                seq += 1
                copy.write_row(
                    (
                        r.tenant_id,
                        r.id,
                        r.mrr_cents,
                        r.is_deleted,
                        r.source_created_at,
                        r.updated_at,
                        BASE_TIME,
                        seq,
                        1,
                        0,
                    )
                )
        cur.execute("ALTER TABLE destination_subscriptions ENABLE TRIGGER trg_enforce_tenant_epoch")
        cur.execute("SELECT setval('dest_ingestion_seq', %s)", (seq,))
        cur.execute("SELECT nextval('dest_ingestion_seq')")
        preallocated_seq = int(cur.fetchone()[0])
    conn.commit()
    return preallocated_seq


def trigger_deterministic_mutations(page_num: int, conn: psycopg.Connection, preallocated_seq: int):
    """Inject concurrent mutations during sweep pagination of tenant acme."""
    if page_num == 1:
        STATE.hard_delete("acme", 420)
        STATE.hard_delete("acme", 9500)
        STATE.soft_delete("acme", 1500, MID_SWEEP_TIME)
    elif page_num == 2:
        rec_backdated = STATE.insert_new(
            tenant_id="acme",
            record_id=455,
            source_created_at=BACKDATED_SOURCE_TIME,
            updated_at=MID_SWEEP_TIME,
            mrr_cents=10000,
        )
        rec_late_commit = STATE.insert_new(
            tenant_id="acme",
            record_id=485,
            source_created_at=MID_SWEEP_TIME,
            updated_at=MID_SWEEP_TIME,
            mrr_cents=5000,
        )
        rec_ahead = STATE.insert_new(
            tenant_id="acme",
            record_id=25005,
            source_created_at=MID_SWEEP_TIME,
            updated_at=MID_SWEEP_TIME,
            mrr_cents=10000,
        )
        ingest_subscription_transactional(
            conn=conn,
            tenant_id=rec_backdated.tenant_id,
            record_id=rec_backdated.id,
            mrr_cents=rec_backdated.mrr_cents,
            is_deleted=False,
            source_created_at=rec_backdated.source_created_at,
            updated_at=rec_backdated.updated_at,
            ingested_at=MID_SWEEP_TIME,
        )
        ingest_subscription_transactional(
            conn=conn,
            tenant_id=rec_late_commit.tenant_id,
            record_id=rec_late_commit.id,
            mrr_cents=rec_late_commit.mrr_cents,
            is_deleted=False,
            source_created_at=rec_late_commit.source_created_at,
            updated_at=rec_late_commit.updated_at,
            ingested_at=MID_SWEEP_TIME,
            preallocated_seq=preallocated_seq,
        )
        ingest_subscription_transactional(
            conn=conn,
            tenant_id=rec_ahead.tenant_id,
            record_id=rec_ahead.id,
            mrr_cents=rec_ahead.mrr_cents,
            is_deleted=False,
            source_created_at=rec_ahead.source_created_at,
            updated_at=rec_ahead.updated_at,
            ingested_at=MID_SWEEP_TIME,
        )
        conn.commit()


def sync_soft_delete_tombstone(conn: psycopg.Connection):
    """Apply the soft-delete tombstone for record 1500 to PostgreSQL destination."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE destination_subscriptions
            SET is_deleted = TRUE, updated_at = %s
            WHERE tenant_id = 'acme' AND id = 1500
            """,
            (MID_SWEEP_TIME,),
        )
    conn.commit()


def evaluate_correctness(conn: psycopg.Connection) -> dict[str, int]:
    """Compare destination tables against upstream ground truth."""
    source_acme = {
        r.id: r for r in STATE.records.values() if r.tenant_id == "acme" and not r.is_deleted
    }
    source_globex = {
        r.id: r for r in STATE.records.values() if r.tenant_id == "globex" and not r.is_deleted
    }

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, mrr_cents
            FROM destination_subscriptions
            WHERE tenant_id = 'acme' AND is_deleted = FALSE
            """
        )
        dest_acme = {row[0]: row[1] for row in cur.fetchall()}

        cur.execute(
            """
            SELECT id
            FROM destination_subscriptions
            WHERE tenant_id = 'globex' AND is_deleted = FALSE
            """
        )
        dest_globex_ids = {row[0] for row in cur.fetchall()}

    source_acme_ids = set(source_acme.keys())
    dest_acme_ids = set(dest_acme.keys())

    ghosts = dest_acme_ids - source_acme_ids
    hard_delete_ghosts = {gid for gid in ghosts if gid in (420, 9500)}
    soft_delete_missed = 1 if 1500 in ghosts else 0

    false_deletions = source_acme_ids - dest_acme_ids
    globex_wiped = len(set(source_globex.keys()) - dest_globex_ids)

    true_source_mrr = sum(r.mrr_cents for r in source_acme.values())
    dest_mrr = sum(dest_acme.values())
    net_mrr_distortion = abs(dest_mrr - true_source_mrr)

    all_acme_ids = source_acme_ids | dest_acme_ids
    gross_mrr_mismatch = sum(
        abs(dest_acme.get(x, 0) - (source_acme[x].mrr_cents if x in source_acme else 0))
        for x in all_acme_ids
    )

    return {
        "hard_delete_ghosts_remaining": len(hard_delete_ghosts),
        "soft_delete_tombstones_missed": soft_delete_missed,
        "false_deletions_boundary_shift": 1 if 1010 in false_deletions else 0,
        "false_deletions_mid_sweep_backdated": 1 if 455 in false_deletions else 0,
        "false_deletions_uncommitted_seq": 1 if 485 in false_deletions else 0,
        "false_deletions_transient_lag": 1 if 3000 in false_deletions else 0,
        "total_false_deletions": len(false_deletions),
        "globex_rows_wiped": globex_wiped,
        "net_mrr_distortion_cents": net_mrr_distortion,
        "gross_mrr_mismatch_cents": gross_mrr_mismatch,
    }


async def run_single_trial(
    strategy_key: str, conn: psycopg.Connection
) -> tuple[SweepExecutionStats, dict[str, int], float]:
    """Execute a single reconciliation pass under concurrent mutations."""
    preallocated_seq = reset_environment(conn)
    transport = httpx.ASGITransport(app=app)
    t_start = time.perf_counter()

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        if strategy_key == "s0_incremental":
            trigger_deterministic_mutations(1, conn, preallocated_seq)
            trigger_deterministic_mutations(2, conn, preallocated_seq)
            stats = await run_strategy_incremental(
                client, conn, "acme", BASE_TIME, MID_SWEEP_TIME, page_size=PAGE_SIZE
            )

        elif strategy_key == "s1_naive_offset":
            sync_soft_delete_tombstone(conn)
            stats = await run_strategy_naive_offset(
                client,
                conn,
                "acme",
                page_size=PAGE_SIZE,
                on_page_fetched=lambda p: trigger_deterministic_mutations(
                    p, conn, preallocated_seq
                ),
                scope_to_tenant=True,
            )

        elif strategy_key == "s2a_keyset_source_created_hwm":
            sync_soft_delete_tombstone(conn)
            stats = await run_strategy_staged_keyset(
                client,
                conn,
                "acme",
                SWEEP_START,
                page_size=PAGE_SIZE,
                on_page_fetched=lambda p: trigger_deterministic_mutations(
                    p, conn, preallocated_seq
                ),
                hwm_mode="source_created_at",
            )

        elif strategy_key == "s2b_keyset_non_tx_seq_hwm":
            sync_soft_delete_tombstone(conn)
            stats = await run_strategy_staged_keyset(
                client,
                conn,
                "acme",
                SWEEP_START,
                page_size=PAGE_SIZE,
                on_page_fetched=lambda p: trigger_deterministic_mutations(
                    p, conn, preallocated_seq
                ),
                hwm_mode="ingestion_seq",
            )

        elif strategy_key == "s2c_keyset_sealed_epoch_hwm":
            sync_soft_delete_tombstone(conn)
            stats = await run_strategy_staged_keyset(
                client,
                conn,
                "acme",
                SWEEP_START,
                page_size=PAGE_SIZE,
                on_page_fetched=lambda p: trigger_deterministic_mutations(
                    p, conn, preallocated_seq
                ),
                hwm_mode="sealed_epoch",
            )

        elif strategy_key == "s3_two_pass_quarantine":
            sync_soft_delete_tombstone(conn)
            stats = await run_strategy_two_pass_quarantine(
                client,
                conn,
                "acme",
                SWEEP_START,
                page_size=PAGE_SIZE,
                on_page_fetched_sweep_1=lambda p: trigger_deterministic_mutations(
                    p, conn, preallocated_seq
                ),
                after_sweep_1_hook=lambda: STATE.decrement_transient_omissions("acme"),
            )

        elif strategy_key == "s4_snapshot_cursor":
            sync_soft_delete_tombstone(conn)
            stats = await run_strategy_snapshot_cursor(
                client,
                conn,
                "acme",
                SWEEP_START,
                page_size=PAGE_SIZE,
                on_page_fetched=lambda p: trigger_deterministic_mutations(
                    p, conn, preallocated_seq
                ),
            )
        else:
            raise ValueError(f"Unknown strategy key: {strategy_key}")

    elapsed_ms = (time.perf_counter() - t_start) * 1000.0
    corr = evaluate_correctness(conn)
    return stats, corr, elapsed_ms
