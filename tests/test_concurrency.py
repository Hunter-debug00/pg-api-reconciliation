"""Tests for advisory locking, sequence races, epoch barriers, and multi-tenant isolation."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
import random
import threading
import time
import httpx
import psycopg
import pytest

from benchmarks.run_benchmark import (
    BASE_TIME,
    DB_URL,
    MID_SWEEP_TIME,
    PAGE_SIZE,
    SWEEP_START,
    evaluate_correctness,
    reset_environment,
    run_single_trial,
)
from pg_reconcile import (
    ConcurrentSweepError,
    ingest_subscription_transactional,
    release_tenant_sweep_lock,
    run_strategy_naive_offset,
    run_strategy_staged_keyset,
    run_strategy_two_pass_quarantine,
    seal_tenant_ingestion_epoch,
    tenant_advisory_lock_id,
)
from pg_reconcile.simulator import STATE, app


@pytest.mark.asyncio
async def test_unscoped_delete_causes_cross_tenant_bleed(db_conn):
    """Omitting tenant_id from the anti-join query falsely purges other tenants' records."""
    reset_environment(db_conn)
    STATE.insert_new("globex", 50055, BASE_TIME, BASE_TIME, 5000)
    ingest_subscription_transactional(
        db_conn, "globex", 50055, 5000, False, BASE_TIME, BASE_TIME, BASE_TIME
    )
    db_conn.commit()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        await run_strategy_naive_offset(
            client, db_conn, "acme", page_size=PAGE_SIZE, scope_to_tenant=False
        )

    corr = evaluate_correctness(db_conn)
    assert corr["globex_rows_wiped"] >= 1


@pytest.mark.asyncio
async def test_uncommitted_sequence_race_vs_sealed_epoch(db_conn):
    """Non-transactional sequence watermarks delete uncommitted writes, whereas epoch barriers block."""
    reset_environment(db_conn)
    STATE.set_transient_omission("acme", 3000, sweeps=0)

    with psycopg.connect(DB_URL) as writer_conn:
        with writer_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO destination_subscriptions
                    (tenant_id, id, mrr_cents, is_deleted, source_created_at, updated_at,
                     ingested_at, ingestion_seq)
                VALUES ('acme', 485, 5000, FALSE, %s, %s, %s, nextval('dest_ingestion_seq'))
                """,
                (MID_SWEEP_TIME, MID_SWEEP_TIME, MID_SWEEP_TIME),
            )

        def commit_writer_on_page_2(page_num: int):
            if page_num == 2:
                STATE.insert_new("acme", 485, MID_SWEEP_TIME, MID_SWEEP_TIME, 5000)
                writer_conn.commit()

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            await run_strategy_staged_keyset(
                client,
                db_conn,
                "acme",
                SWEEP_START,
                page_size=PAGE_SIZE,
                on_page_fetched=commit_writer_on_page_2,
                hwm_mode="ingestion_seq",
            )

    corr_seq = evaluate_correctness(db_conn)
    assert corr_seq["false_deletions_uncommitted_seq"] == 1

    _, corr_epoch, _ = await run_single_trial("s2c_keyset_sealed_epoch_hwm", db_conn)
    assert corr_epoch["false_deletions_mid_sweep_backdated"] == 0
    assert corr_epoch["false_deletions_uncommitted_seq"] == 0


@pytest.mark.asyncio
async def test_epoch_barrier_verified_via_pg_stat_activity(db_conn):
    """The epoch sealer blocks on pg_stat_activity lock-wait until concurrent writers commit."""
    reset_environment(db_conn)
    STATE.set_transient_omission("acme", 3000, sweeps=0)
    sealer_started = threading.Event()
    events: list[str] = []

    with psycopg.connect(DB_URL) as writer_conn, psycopg.connect(DB_URL) as sweep_conn:
        sweep_backend_pid = sweep_conn.info.backend_pid

        with writer_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO destination_subscriptions
                    (tenant_id, id, mrr_cents, is_deleted, source_created_at, updated_at)
                VALUES ('acme', 485, 5000, FALSE, %s, %s)
                """,
                (MID_SWEEP_TIME, MID_SWEEP_TIME),
            )
        events.append("writer_raw_insert_uncommitted")

        def run_sealer_thread():
            sealer_started.set()
            sealed = seal_tenant_ingestion_epoch(sweep_conn, "acme")
            events.append(f"sealed_epoch_{sealed}")

        t = threading.Thread(target=run_sealer_thread)
        t.start()
        sealer_started.wait(timeout=2.0)

        locked_in_pg = False
        for _ in range(100):
            with db_conn.cursor() as cur:
                cur.execute(
                    "SELECT wait_event_type FROM pg_stat_activity WHERE pid = %s",
                    (sweep_backend_pid,),
                )
                row = cur.fetchone()
                if row and row[0] == "Lock":
                    locked_in_pg = True
                    break
            await asyncio.sleep(0.01)

        assert locked_in_pg is True
        assert events == ["writer_raw_insert_uncommitted"]

        STATE.insert_new("acme", 485, MID_SWEEP_TIME, MID_SWEEP_TIME, 5000)
        events.append("writer_committing")
        writer_conn.commit()
        t.join(timeout=2.0)
        assert events == [
            "writer_raw_insert_uncommitted",
            "writer_committing",
            "sealed_epoch_1",
        ]

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            await run_strategy_staged_keyset(
                client,
                sweep_conn,
                "acme",
                SWEEP_START,
                page_size=PAGE_SIZE,
                hwm_mode="sealed_epoch",
            )

    corr = evaluate_correctness(db_conn)
    assert corr["false_deletions_uncommitted_seq"] == 0
    assert corr["total_false_deletions"] == 0


@pytest.mark.asyncio
async def test_concurrent_sweep_rejected_by_advisory_lock(db_conn):
    """Concurrent sweeps targeting the same tenant raise ConcurrentSweepError immediately."""
    reset_environment(db_conn)
    lock_id = tenant_advisory_lock_id("acme")

    with psycopg.connect(DB_URL) as conn_1, psycopg.connect(DB_URL) as conn_2:
        with conn_1.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_id,))
            assert cur.fetchone()[0] is True
        conn_1.commit()

        try:
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                with pytest.raises(ConcurrentSweepError, match="already holds lock"):
                    await run_strategy_two_pass_quarantine(
                        client, conn_2, "acme", SWEEP_START, page_size=PAGE_SIZE
                    )
        finally:
            with conn_1.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s)", (lock_id,))
            conn_1.commit()


@pytest.mark.asyncio
async def test_multithreaded_cross_tenant_sweeps_overlap_cleanly(db_conn):
    """Sweeps for distinct tenants acquire distinct advisory locks and execute in parallel."""
    reset_environment(db_conn)
    STATE.set_transient_omission("acme", 3000, sweeps=0)
    STATE.hard_delete("acme", 9500)
    STATE.hard_delete("globex", 9500)
    barrier = threading.Barrier(2, timeout=5.0)

    def run_tenant_worker(tenant: str) -> tuple[float, float]:
        def sync_mid_scan(page_num: int):
            if page_num == 25:
                barrier.wait()

        async def _runner():
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                with psycopg.connect(DB_URL) as worker_conn:
                    t0 = time.perf_counter()
                    await run_strategy_staged_keyset(
                        client,
                        worker_conn,
                        tenant,
                        SWEEP_START,
                        page_size=PAGE_SIZE,
                        on_page_fetched=sync_mid_scan,
                    )
                    t1 = time.perf_counter()
                    return t0, t1

        return asyncio.run(_runner())

    with ThreadPoolExecutor(max_workers=2) as pool:
        f_acme = pool.submit(run_tenant_worker, "acme")
        f_globex = pool.submit(run_tenant_worker, "globex")
        acme_start, acme_end = f_acme.result(timeout=10.0)
        globex_start, globex_end = f_globex.result(timeout=10.0)

    assert max(acme_start, globex_start) < min(acme_end, globex_end)

    corr = evaluate_correctness(db_conn)
    assert corr["hard_delete_ghosts_remaining"] == 0
    assert corr["total_false_deletions"] == 0
    assert corr["globex_rows_wiped"] == 0


@pytest.mark.asyncio
async def test_randomized_mutation_stress_across_strategies(db_conn):
    """Validate that Strategy 3 survives arbitrary combinations of transient lag and mutation bursts."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        for seed in (101, 202, 303):
            rng = random.Random(seed)
            reset_environment(db_conn)
            STATE.set_transient_omission("acme", 3000, sweeps=0)

            for omit_id in rng.sample([i * 10 for i in range(100, 4900)], 5):
                STATE.set_transient_omission("acme", omit_id, sweeps=1)

            def random_mutations(page_num: int, _rng=rng):
                if page_num == 1:
                    for del_id in _rng.sample([i * 10 for i in range(1, 95)], 3):
                        STATE.hard_delete("acme", del_id)
                    for del_id in _rng.sample([i * 10 for i in range(200, 4800)], 5):
                        STATE.hard_delete("acme", del_id)
                elif page_num == 3:
                    for ins_offset in range(1, 6):
                        new_id = ins_offset * 10 + 5
                        rec = STATE.insert_new(
                            "acme",
                            new_id,
                            BASE_TIME - timedelta(hours=2),
                            MID_SWEEP_TIME,
                            7500,
                        )
                        ingest_subscription_transactional(
                            db_conn,
                            "acme",
                            rec.id,
                            rec.mrr_cents,
                            False,
                            rec.source_created_at,
                            rec.updated_at,
                            MID_SWEEP_TIME,
                        )
                    db_conn.commit()

            await run_strategy_two_pass_quarantine(
                client,
                db_conn,
                "acme",
                SWEEP_START,
                page_size=PAGE_SIZE,
                on_page_fetched_sweep_1=random_mutations,
                after_sweep_1_hook=lambda: STATE.decrement_transient_omissions("acme"),
            )
            corr_s3 = evaluate_correctness(db_conn)
            assert corr_s3["total_false_deletions"] == 0
            assert corr_s3["globex_rows_wiped"] == 0


@pytest.mark.asyncio
async def test_authoritative_reingestion_resets_missing_streak(db_conn):
    """An authoritative live write between sweeps resets missing_streak to zero."""
    reset_environment(db_conn)
    STATE.set_transient_omission("acme", 3000, sweeps=2)

    def authoritative_upsert_between_sweeps():
        STATE.decrement_transient_omissions("acme")
        ingest_subscription_transactional(
            conn=db_conn,
            tenant_id="acme",
            record_id=3000,
            mrr_cents=6500,
            is_deleted=False,
            source_created_at=BASE_TIME,
            updated_at=MID_SWEEP_TIME,
            ingested_at=MID_SWEEP_TIME,
        )
        db_conn.commit()

        with db_conn.cursor() as cur:
            cur.execute(
                "SELECT missing_streak, last_missed_sweep_seq FROM destination_subscriptions WHERE tenant_id = 'acme' AND id = 3000"
            )
            streak, last_seq = cur.fetchone()
            assert streak == 0
            assert last_seq == 0

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        await run_strategy_two_pass_quarantine(
            client,
            db_conn,
            "acme",
            SWEEP_START,
            page_size=PAGE_SIZE,
            after_sweep_1_hook=authoritative_upsert_between_sweeps,
        )

    STATE.set_transient_omission("acme", 3000, sweeps=0)
    STATE.records[("acme", 3000)].mrr_cents = 6500
    corr = evaluate_correctness(db_conn)
    assert corr["false_deletions_transient_lag"] == 0
    assert corr["total_false_deletions"] == 0


@pytest.mark.asyncio
async def test_database_trigger_enforces_epoch_and_rejects_manual_override(db_conn):
    """Direct SQL writes cannot bypass the trigger-enforced epoch assignment."""
    reset_environment(db_conn)
    seal_tenant_ingestion_epoch(db_conn, "acme")
    seal_tenant_ingestion_epoch(db_conn, "acme")

    with db_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO destination_subscriptions
                (tenant_id, id, mrr_cents, is_deleted, source_created_at, updated_at, ingestion_epoch, missing_streak)
            VALUES ('acme', 777, 5000, FALSE, %s, %s, 1, 5)
            """,
            (MID_SWEEP_TIME, MID_SWEEP_TIME),
        )
        cur.execute(
            "SELECT ingestion_epoch, missing_streak FROM destination_subscriptions WHERE tenant_id = 'acme' AND id = 777"
        )
        epoch, streak = cur.fetchone()
        assert epoch == 3
        assert streak == 0

        cur.execute(
            """
            INSERT INTO destination_subscriptions
                (tenant_id, id, mrr_cents, is_deleted, source_created_at, updated_at)
            VALUES ('initech', 10, 5000, FALSE, %s, %s)
            """,
            (MID_SWEEP_TIME, MID_SWEEP_TIME),
        )
        cur.execute("SELECT current_epoch FROM tenant_watermarks WHERE tenant_id = 'initech'")
        assert cur.fetchone()[0] == 1
    db_conn.commit()


@pytest.mark.asyncio
async def test_advisory_unlock_failure_closes_connection(db_conn):
    """Attempting to release an unheld lock closes the connection immediately to prevent leaks."""
    with psycopg.connect(DB_URL) as temp_conn:
        with pytest.raises(RuntimeError, match="pg_advisory_unlock returned False"):
            release_tenant_sweep_lock(temp_conn, "acme", active_exc=None)
        assert temp_conn.closed is True


@pytest.mark.asyncio
async def test_tenant_id_reassignment_rejected_by_trigger(db_conn):
    """Updates attempting to mutate tenant_id raise an Immutable column exception."""
    reset_environment(db_conn)
    with db_conn.cursor() as cur:
        with pytest.raises(
            psycopg.errors.RaiseException,
            match="Immutable column violation: tenant_id cannot be modified",
        ):
            cur.execute(
                """
                UPDATE destination_subscriptions
                SET tenant_id = 'globex'
                WHERE tenant_id = 'acme' AND id = 10
                """
            )
    db_conn.rollback()
