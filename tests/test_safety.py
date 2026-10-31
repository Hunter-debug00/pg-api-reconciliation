"""Tests for mass-deletion safety guards, network fault recovery, and epoch timeouts."""

import httpx
import psycopg
import pytest

from pg_reconcile import (
    IncompleteSweepError,
    MassDeletionAbortedError,
    assert_disposable_database,
    run_strategy_snapshot_cursor,
    run_strategy_staged_keyset,
    run_strategy_two_pass_quarantine,
    tenant_advisory_lock_id,
)
from pg_reconcile.simulator import STATE, app
from tests.conftest import (
    DB_URL,
    PAGE_SIZE,
    SWEEP_START,
    canned_transport,
    evaluate_correctness,
    reset_environment,
)


@pytest.mark.asyncio
async def test_upstream_http_500_and_network_timeout_aborts_cleanly(db_conn):
    """Network dropouts or 5xx responses abort the sweep without executing deletions."""
    reset_environment(db_conn)
    STATE.fail_on_page = 25

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        with pytest.raises(IncompleteSweepError, match="HTTP 500"):
            await run_strategy_staged_keyset(
                client, db_conn, "acme", SWEEP_START, page_size=PAGE_SIZE
            )

        STATE.reset_faults()
        STATE.timeout_on_page = 12
        with pytest.raises(IncompleteSweepError, match="timeout"):
            await run_strategy_staged_keyset(
                client, db_conn, "acme", SWEEP_START, page_size=PAGE_SIZE
            )

    corr = evaluate_correctness(db_conn)
    assert corr["total_false_deletions"] == 0


@pytest.mark.asyncio
async def test_truncated_empty_page_with_has_more_aborts(db_conn):
    """Premature empty batch when has_more is true indicates an API stream failure."""
    reset_environment(db_conn)
    STATE.return_truncated_page = 10

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        with pytest.raises(IncompleteSweepError):
            await run_strategy_staged_keyset(
                client, db_conn, "acme", SWEEP_START, page_size=PAGE_SIZE
            )

    corr = evaluate_correctness(db_conn)
    assert corr["total_false_deletions"] == 0


@pytest.mark.asyncio
async def test_malformed_cursor_and_unsorted_batch_aborts(db_conn):
    """Out-of-order batches or non-advancing cursors trigger immediate fail-closed aborts."""
    reset_environment(db_conn)
    STATE.return_malformed_cursor_page = 5

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        with pytest.raises(IncompleteSweepError, match="Malformed cursor"):
            await run_strategy_staged_keyset(
                client, db_conn, "acme", SWEEP_START, page_size=PAGE_SIZE
            )

        STATE.reset_faults()
        STATE.return_unsorted_batch_page = 5
        with pytest.raises(IncompleteSweepError, match="Unsorted or duplicate ID"):
            await run_strategy_staged_keyset(
                client, db_conn, "acme", SWEEP_START, page_size=PAGE_SIZE
            )

    corr = evaluate_correctness(db_conn)
    assert corr["total_false_deletions"] == 0


@pytest.mark.asyncio
async def test_database_error_rolls_back_and_releases_lock(db_conn):
    """Database failures across staging, streak updating, and deletion stages roll back cleanly."""
    reset_environment(db_conn)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:

        async def run_with_failure(strategy_func, query_substring=None, fail_commit=False):
            orig_cursor = db_conn.cursor
            orig_commit = db_conn.commit
            ready_to_fail_commit = False
            commit_failed = False

            class FaultCursor:
                def __init__(self, real_cur):
                    self.real = real_cur

                def __enter__(self):
                    return self

                def __exit__(self, et, ev, tb):
                    return self.real.__exit__(et, ev, tb)

                def execute(self, query, params=None):
                    nonlocal ready_to_fail_commit
                    if "status = 'COMPLETED'" in str(query):
                        ready_to_fail_commit = True
                    if query_substring and query_substring in str(query):
                        raise psycopg.OperationalError(f"Simulated fault: {query_substring}")
                    return self.real.execute(query, params)

                def copy(self, query, params=None):
                    if query_substring and query_substring in str(query):
                        raise psycopg.OperationalError(f"Simulated fault: {query_substring}")
                    return self.real.copy(query, params)

                def fetchone(self):
                    return self.real.fetchone()

                def fetchall(self):
                    return self.real.fetchall()

            def patched_cursor(*args, **kwargs):
                return FaultCursor(orig_cursor(*args, **kwargs))

            def patched_commit():
                nonlocal commit_failed
                if fail_commit and ready_to_fail_commit and not commit_failed:
                    commit_failed = True
                    raise psycopg.OperationalError("Simulated commit failure")
                return orig_commit()

            db_conn.cursor = patched_cursor
            if fail_commit:
                db_conn.commit = patched_commit

            try:
                with pytest.raises(IncompleteSweepError, match="rolled back"):
                    await strategy_func()
            finally:
                db_conn.cursor = orig_cursor
                db_conn.commit = orig_commit

            with db_conn.cursor() as cur:
                cur.execute(
                    "SELECT status FROM reconciliation_sweeps ORDER BY sweep_seq DESC LIMIT 1"
                )
                assert cur.fetchone()[0] == "ABORTED"
                cur.execute("SELECT COUNT(*) FROM unlogged_seen_ids")
                assert cur.fetchone()[0] == 0

            lock_id = tenant_advisory_lock_id("acme")
            with psycopg.connect(DB_URL) as check_conn:
                with check_conn.cursor() as cur:
                    cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_id,))
                    assert cur.fetchone()[0] is True
                    cur.execute("SELECT pg_advisory_unlock(%s)", (lock_id,))
                check_conn.commit()

        # 1. Staging error in Strategy 2
        await run_with_failure(
            lambda: run_strategy_staged_keyset(
                client, db_conn, "acme", SWEEP_START, page_size=PAGE_SIZE
            ),
            query_substring="COPY unlogged_seen_ids",
        )
        # 2. Deletion error in Strategy 2
        await run_with_failure(
            lambda: run_strategy_staged_keyset(
                client, db_conn, "acme", SWEEP_START, page_size=PAGE_SIZE
            ),
            query_substring="DELETE FROM destination_subscriptions",
        )
        # 3. Commit error in Strategy 2
        await run_with_failure(
            lambda: run_strategy_staged_keyset(
                client, db_conn, "acme", SWEEP_START, page_size=PAGE_SIZE
            ),
            query_substring=None,
            fail_commit=True,
        )
        # 4. Streak update error in Strategy 3
        await run_with_failure(
            lambda: run_strategy_two_pass_quarantine(
                client, db_conn, "acme", SWEEP_START, page_size=PAGE_SIZE
            ),
            query_substring="missing_streak = missing_streak + 1",
        )
        # 5. Deletion error in Strategy 3
        await run_with_failure(
            lambda: run_strategy_two_pass_quarantine(
                client, db_conn, "acme", SWEEP_START, page_size=PAGE_SIZE
            ),
            query_substring="DELETE FROM destination_subscriptions",
        )
        # 6. Commit error in Strategy 4
        await run_with_failure(
            lambda: run_strategy_snapshot_cursor(
                client, db_conn, "acme", SWEEP_START, page_size=PAGE_SIZE
            ),
            query_substring=None,
            fail_commit=True,
        )


@pytest.mark.asyncio
async def test_snapshot_expiration_mid_scan_aborts_cleanly(db_conn):
    """Snapshot expiration (HTTP 404) halts the sweep without purging database rows."""
    reset_environment(db_conn)
    STATE.expire_snapshot_on_page = 15

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        with pytest.raises(IncompleteSweepError, match="HTTP 404"):
            await run_strategy_snapshot_cursor(
                client, db_conn, "acme", SWEEP_START, page_size=PAGE_SIZE
            )

    corr = evaluate_correctness(db_conn)
    assert corr["total_false_deletions"] == 0


@pytest.mark.asyncio
async def test_abandoned_sweep_recovered_on_next_run(db_conn):
    """Orphaned STARTED or STAGED records left by crashed processes are cleaned up automatically."""
    reset_environment(db_conn)
    STATE.set_transient_omission("acme", 3000, sweeps=0)
    dangling_id = "00000000-0000-0000-0000-000000000999"

    with db_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO reconciliation_sweeps
                (sweep_id, tenant_id, strategy_name, status, sweep_started_at, hwm_ingestion_seq, sealed_epoch)
            VALUES (%s, 'acme', 'crashed_worker', 'STAGED', %s, 10000, 1)
            """,
            (dangling_id, SWEEP_START),
        )
        cur.execute(
            "INSERT INTO unlogged_seen_ids (sweep_id, tenant_id, id) VALUES (%s, 'acme', 10)",
            (dangling_id,),
        )
    db_conn.commit()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        await run_strategy_staged_keyset(client, db_conn, "acme", SWEEP_START, page_size=PAGE_SIZE)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM reconciliation_sweeps WHERE sweep_id = %s",
            (dangling_id,),
        )
        assert cur.fetchone()[0] == "ABORTED"
        cur.execute(
            "SELECT COUNT(*) FROM unlogged_seen_ids WHERE sweep_id = %s",
            (dangling_id,),
        )
        assert cur.fetchone()[0] == 0


@pytest.mark.asyncio
async def test_empty_scan_with_has_more_false_aborts_via_safety_guard(db_conn):
    """Empty upstream scans (HTTP 200 with empty list) trigger MassDeletionAbortedError before any deletion."""
    for strategy_label, runner in (
        (
            "Strategy 2c",
            lambda cl: run_strategy_staged_keyset(
                cl, db_conn, "acme", SWEEP_START, page_size=PAGE_SIZE, hwm_mode="sealed_epoch"
            ),
        ),
        (
            "Strategy 3",
            lambda cl: run_strategy_two_pass_quarantine(
                cl, db_conn, "acme", SWEEP_START, page_size=PAGE_SIZE
            ),
        ),
    ):
        reset_environment(db_conn)
        evaluate_correctness(db_conn)
        empty_page = {"ids": [], "next_after_id": None, "has_more": False}

        async with httpx.AsyncClient(
            transport=canned_transport([empty_page]), base_url="http://test"
        ) as client:
            with pytest.raises(MassDeletionAbortedError, match="Safety guard"):
                await runner(client)

        with db_conn.cursor() as cur:
            cur.execute("SELECT status FROM reconciliation_sweeps ORDER BY sweep_seq DESC LIMIT 1")
            assert cur.fetchone()[0] == "ABORTED"
            cur.execute("SELECT COUNT(*) FROM unlogged_seen_ids")
            assert cur.fetchone()[0] == 0

        after_count = evaluate_correctness(db_conn)
        assert (
            after_count["total_false_deletions"] == 0
        ), f"{strategy_label} deleted records on empty scan!"


@pytest.mark.asyncio
async def test_truncated_short_scan_exceeding_delete_ratio_aborts(db_conn):
    """Truncated pagination returning only a fraction of records aborts if deletions exceed threshold."""
    reset_environment(db_conn)
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM destination_subscriptions WHERE tenant_id='acme' ORDER BY id LIMIT 100"
        )
        staged_100 = [r[0] for r in cur.fetchall()]

    short_page = {"ids": staged_100, "next_after_id": staged_100[-1], "has_more": False}

    async with httpx.AsyncClient(
        transport=canned_transport([short_page]), base_url="http://test"
    ) as client:
        with pytest.raises(MassDeletionAbortedError, match="exceeding safety limit"):
            await run_strategy_staged_keyset(
                client,
                db_conn,
                "acme",
                SWEEP_START,
                page_size=PAGE_SIZE,
                hwm_mode="sealed_epoch",
                max_delete_ratio=0.20,
            )

    with db_conn.cursor() as cur:
        cur.execute("SELECT status FROM reconciliation_sweeps ORDER BY sweep_seq DESC LIMIT 1")
        assert cur.fetchone()[0] == "ABORTED"

    corr = evaluate_correctness(db_conn)
    assert corr["total_false_deletions"] == 0


@pytest.mark.asyncio
async def test_seal_epoch_lock_timeout_aborts_when_writer_stalls(db_conn):
    """Stalled writer transactions holding locks cause the epoch sealer to time out cleanly."""
    reset_environment(db_conn)
    with psycopg.connect(DB_URL) as writer_conn:
        with writer_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO destination_subscriptions
                    (tenant_id, id, mrr_cents, is_deleted, source_created_at, updated_at)
                VALUES ('acme', 990001, 100, FALSE, now(), now())
                """
            )
            # Transaction left open holding FOR SHARE via trigger

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            with pytest.raises(IncompleteSweepError, match="Timed out acquiring epoch seal lock"):
                await run_strategy_staged_keyset(
                    client,
                    db_conn,
                    "acme",
                    SWEEP_START,
                    page_size=PAGE_SIZE,
                    hwm_mode="sealed_epoch",
                    seal_lock_timeout="200ms",
                )

        lock_id = tenant_advisory_lock_id("acme")
        with psycopg.connect(DB_URL) as check_conn:
            with check_conn.cursor() as cur:
                cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_id,))
                assert cur.fetchone()[0] is True
                cur.execute("SELECT pg_advisory_unlock(%s)", (lock_id,))
            check_conn.commit()


@pytest.mark.asyncio
async def test_seal_epoch_statement_timeout_bounds_cumulative_wait(db_conn):
    """statement_timeout bounds total elapsed time even if lock_timeout is set high."""
    import time

    reset_environment(db_conn)
    with psycopg.connect(DB_URL) as writer_conn:
        with writer_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO destination_subscriptions
                    (tenant_id, id, mrr_cents, is_deleted, source_created_at, updated_at)
                VALUES ('acme', 990002, 100, FALSE, now(), now())
                """
            )
            # Transaction left open holding FOR SHARE on tenant_watermarks

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            t0 = time.perf_counter()
            with pytest.raises(IncompleteSweepError, match="Timed out acquiring epoch seal lock"):
                await run_strategy_staged_keyset(
                    client,
                    db_conn,
                    "acme",
                    SWEEP_START,
                    page_size=PAGE_SIZE,
                    hwm_mode="sealed_epoch",
                    seal_lock_timeout="10s",
                    seal_statement_timeout="250ms",
                )
            elapsed = time.perf_counter() - t0
            assert (
                elapsed < 1.5
            ), f"statement_timeout failed to bound wait time: elapsed {elapsed:.2f}s"


@pytest.mark.asyncio
async def test_legitimate_deletion_below_threshold_succeeds(db_conn):
    """Deletions within the allowable threshold (e.g. 5 records out of 5,000) succeed normally."""
    reset_environment(db_conn)
    STATE.set_transient_omission("acme", 3000, sweeps=0)
    for rid in (10, 20, 30, 40, 50):
        STATE.hard_delete("acme", rid)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        await run_strategy_staged_keyset(
            client,
            db_conn,
            "acme",
            SWEEP_START,
            page_size=PAGE_SIZE,
            hwm_mode="sealed_epoch",
            max_delete_ratio=0.20,
        )

    corr = evaluate_correctness(db_conn)
    assert corr["hard_delete_ghosts_remaining"] == 0
    assert corr["total_false_deletions"] == 0


def test_disposable_database_guard_rejects_unapproved_database(monkeypatch):
    """assert_disposable_database rejects unapproved production database names unless overridden."""

    class DummyConnInfo:
        def __init__(self, dbname):
            self.dbname = dbname

    class DummyConn:
        def __init__(self, dbname):
            self.info = DummyConnInfo(dbname)

    # Rejects unapproved/production database names
    for bad_name in ("company_db", "prod", "production_billing", "users", "my_app"):
        with pytest.raises(RuntimeError, match="Refusing to execute destructive DDL"):
            assert_disposable_database(DummyConn(bad_name))

    # Accepts explicitly allowlisted disposable names
    for good_name in ("reconcile_bench", "reconcile_test", "postgres_test", "test", "bench"):
        assert_disposable_database(DummyConn(good_name))

    # Accepts custom name when ALLOW_UNSAFE_DB_RESET=1 is explicitly set
    monkeypatch.setenv("ALLOW_UNSAFE_DB_RESET", "1")
    assert_disposable_database(DummyConn("company_db"))
