"""Tests validating correctness and failure boundaries across reconciliation strategies."""

import httpx
import pytest

from benchmarks.run_benchmark import (
    BASE_TIME,
    MID_SWEEP_TIME,
    PAGE_SIZE,
    SWEEP_START,
    evaluate_correctness,
    reset_environment,
    run_single_trial,
)
from pg_reconcile import (
    run_strategy_incremental,
    run_strategy_two_pass_quarantine,
)
from pg_reconcile.simulator import STATE, app


@pytest.mark.asyncio
async def test_incremental_sync_upserts_tombstones(db_conn):
    """Incremental sync applies soft-delete tombstones but cannot discover hard deletes."""
    reset_environment(db_conn)
    for i in range(1, 26):
        STATE.soft_delete("acme", i * 10, MID_SWEEP_TIME)
    STATE.hard_delete("acme", 9500)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        stats = await run_strategy_incremental(
            client, db_conn, "acme", BASE_TIME, MID_SWEEP_TIME, page_size=10
        )

    assert stats.api_requests == 3
    corr = evaluate_correctness(db_conn)
    assert corr["soft_delete_tombstones_missed"] == 0
    assert corr["hard_delete_ghosts_remaining"] == 1


@pytest.mark.asyncio
async def test_offset_pagination_causes_boundary_slip_and_false_deletions(db_conn):
    """Hard-deleting a record shifts subsequent records backward, causing false deletions."""
    _, corr, _ = await run_single_trial("s1_naive_offset", db_conn)
    assert corr["false_deletions_boundary_shift"] == 1
    assert corr["false_deletions_mid_sweep_backdated"] == 1
    assert corr["false_deletions_uncommitted_seq"] == 1
    assert corr["false_deletions_transient_lag"] == 1
    assert corr["total_false_deletions"] == 4
    assert corr["globex_rows_wiped"] == 0


@pytest.mark.asyncio
async def test_source_created_at_watermark_deletes_backdated_inserts(db_conn):
    """Relying on upstream source_created_at falsely deletes records arriving via webhooks."""
    _, corr, _ = await run_single_trial("s2a_keyset_source_created_hwm", db_conn)
    assert corr["false_deletions_boundary_shift"] == 0
    assert corr["false_deletions_mid_sweep_backdated"] == 1


@pytest.mark.asyncio
async def test_two_pass_quarantine_tolerates_temporary_replica_lag(db_conn):
    """Records omitted in a single sweep due to replication lag are spared from deletion."""
    _, corr, _ = await run_single_trial("s3_two_pass_quarantine", db_conn)
    assert corr["total_false_deletions"] == 0
    assert corr["hard_delete_ghosts_remaining"] == 1


@pytest.mark.asyncio
async def test_two_pass_quarantine_purges_after_consecutive_omissions(db_conn):
    """Records missing across two consecutive completed sweeps are purged as genuine hard deletes."""
    reset_environment(db_conn)
    STATE.set_transient_omission("acme", 3000, sweeps=2)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        await run_strategy_two_pass_quarantine(
            client, db_conn, "acme", SWEEP_START, page_size=PAGE_SIZE
        )

    corr = evaluate_correctness(db_conn)
    assert corr["false_deletions_transient_lag"] == 1


@pytest.mark.asyncio
async def test_snapshot_cursor_vulnerable_to_replica_lag_at_snapshot_time(db_conn):
    """Snapshot cursors isolate in-flight mutations but capture any replica lag present at creation."""
    _, corr, _ = await run_single_trial("s4_snapshot_cursor", db_conn)
    assert corr["false_deletions_boundary_shift"] == 0
    assert corr["false_deletions_mid_sweep_backdated"] == 0
    assert corr["false_deletions_uncommitted_seq"] == 0
    assert corr["false_deletions_transient_lag"] == 1
