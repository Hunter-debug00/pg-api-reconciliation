"""Seven reconciliation strategies evaluating pagination safety and concurrency boundaries."""

import time
import uuid
from collections.abc import Callable
from datetime import datetime
from typing import Literal

import httpx
import psycopg

from .engine import (
    abort_sweep_on_db_error,
    acquire_tenant_sweep_lock,
    assert_deletion_safety_bound,
    capture_destination_hwm_seq,
    fetch_keyset_pages_fail_closed,
    finalize_completed_sweep,
    ingest_subscription_transactional,
    parse_explain_json,
    release_tenant_sweep_lock,
    seal_tenant_ingestion_epoch,
    stage_seen_ids,
)
from .models import SweepExecutionStats


async def run_strategy_incremental(
    client: httpx.AsyncClient,
    conn: psycopg.Connection,
    tenant_id: str,
    updated_after: datetime,
    ingested_at: datetime,
    page_size: int = 100,
) -> SweepExecutionStats:
    """Strategy 0: Multi-page incremental cursor sync with soft-delete tombstones."""
    api_reqs = 0
    body_bytes = 0
    cursor_time = updated_after.isoformat()
    after_id = 0
    all_records = []

    while True:
        resp = await client.get(
            "/records/incremental",
            params={
                "tenant_id": tenant_id,
                "updated_after": cursor_time,
                "after_id": after_id,
                "limit": page_size,
            },
        )
        resp.raise_for_status()
        api_reqs += 1
        body_bytes += len(resp.content)

        data = resp.json()
        batch = data["records"]
        all_records.extend(batch)

        if not data["has_more"] or not batch:
            break
        cursor_time = data["next_updated_after"]
        after_id = data["next_after_id"]

    t0 = time.perf_counter()
    for rec in all_records:
        ingest_subscription_transactional(
            conn=conn,
            tenant_id=rec["tenant_id"],
            record_id=rec["id"],
            mrr_cents=rec["mrr_cents"],
            is_deleted=rec["is_deleted"],
            source_created_at=rec["source_created_at"],
            updated_at=rec["updated_at"],
            ingested_at=ingested_at,
        )
    conn.commit()
    post_db_ms = (time.perf_counter() - t0) * 1000.0

    return SweepExecutionStats(
        api_requests=api_reqs,
        response_body_bytes=body_bytes,
        pre_scan_db_ms=0.0,
        post_scan_db_ms=post_db_ms,
        total_db_reconcile_ms=post_db_ms,
        delete_query_plan_ms=0.0,
        delete_plan_root_shared_hits=None,
    )


async def run_strategy_naive_offset(
    client: httpx.AsyncClient,
    conn: psycopg.Connection,
    tenant_id: str,
    page_size: int = 100,
    on_page_fetched: Callable[[int], None] | None = None,
    scope_to_tenant: bool = True,
) -> SweepExecutionStats:
    """Strategy 1: Naive OFFSET pagination with direct NOT (id = ANY(%s)) anti-join."""
    seen_ids: list[int] = []
    offset = 0
    page_num = 0
    api_reqs = 0
    body_bytes = 0

    while True:
        resp = await client.get(
            "/records/offset",
            params={"tenant_id": tenant_id, "offset": offset, "limit": page_size},
        )
        resp.raise_for_status()
        api_reqs += 1
        body_bytes += len(resp.content)

        data = resp.json()
        batch = data["ids"]
        if not batch:
            break

        seen_ids.extend(batch)
        page_num += 1
        if on_page_fetched:
            on_page_fetched(page_num)

        if not data["has_more"]:
            break
        offset += page_size

    t0 = time.perf_counter()
    with conn.cursor() as cur:
        if scope_to_tenant:
            cur.execute(
                """
                EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
                DELETE FROM destination_subscriptions
                WHERE tenant_id = %s
                  AND is_deleted = FALSE
                  AND NOT (id = ANY(%s))
                """,
                (tenant_id, seen_ids),
            )
        else:
            cur.execute(
                """
                EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
                DELETE FROM destination_subscriptions
                WHERE is_deleted = FALSE
                  AND NOT (id = ANY(%s))
                """,
                (seen_ids,),
            )
        explain_row = cur.fetchone()[0]
    conn.commit()
    post_db_ms = (time.perf_counter() - t0) * 1000.0
    del_ms, shared_hits = parse_explain_json(explain_row)

    return SweepExecutionStats(
        api_requests=api_reqs,
        response_body_bytes=body_bytes,
        pre_scan_db_ms=0.0,
        post_scan_db_ms=post_db_ms,
        total_db_reconcile_ms=post_db_ms,
        delete_query_plan_ms=del_ms,
        delete_plan_root_shared_hits=shared_hits,
    )


async def run_strategy_staged_keyset(
    client: httpx.AsyncClient,
    conn: psycopg.Connection,
    tenant_id: str,
    sweep_started_at: datetime,
    page_size: int = 100,
    on_page_fetched: Callable[[int], None] | None = None,
    hwm_mode: Literal[
        "source_created_at", "ingested_at", "ingestion_seq", "sealed_epoch"
    ] = "sealed_epoch",
    max_delete_ratio: float = 0.20,
    seal_lock_timeout: str = "5s",
    seal_statement_timeout: str | None = None,
) -> SweepExecutionStats:
    """Strategy 2: Keyset pagination with UNLOGGED staging and watermark barrier."""
    t_pre = time.perf_counter()
    acquire_tenant_sweep_lock(conn, tenant_id)
    active_exc: BaseException | None = None
    try:
        sweep_id = uuid.uuid4()
        # nextval isn't transactional. Kept so strategy 2b can show the race.
        hwm_seq = capture_destination_hwm_seq(conn)
        sealed_epoch = (
            seal_tenant_ingestion_epoch(
                conn,
                tenant_id,
                lock_timeout=seal_lock_timeout,
                statement_timeout=seal_statement_timeout,
            )
            if hwm_mode == "sealed_epoch"
            else 1
        )
        pre_scan_db_ms = (time.perf_counter() - t_pre) * 1000.0

        scan = await fetch_keyset_pages_fail_closed(
            client=client,
            conn=conn,
            sweep_id=sweep_id,
            tenant_id=tenant_id,
            strategy_name=f"strategy_2_{hwm_mode}",
            sweep_started_at=sweep_started_at,
            hwm_seq=hwm_seq,
            sealed_epoch=sealed_epoch,
            page_size=page_size,
            on_page_fetched=on_page_fetched,
        )
        pre_scan_db_ms += scan.init_db_ms

        t_post = time.perf_counter()
        try:
            with conn.cursor() as cur:
                stage_seen_ids(cur, sweep_id, tenant_id, scan.pages_fetched, scan.seen_ids)

                if hwm_mode == "sealed_epoch":
                    hwm_clause, hwm_val = "d.ingestion_epoch <= %s", sealed_epoch
                elif hwm_mode == "ingestion_seq":
                    hwm_clause, hwm_val = "d.ingestion_seq <= %s", hwm_seq
                else:
                    hwm_col = "ingested_at" if hwm_mode == "ingested_at" else "source_created_at"
                    hwm_clause, hwm_val = f"d.{hwm_col} < %s", sweep_started_at

                assert_deletion_safety_bound(
                    cur,
                    tenant_id,
                    sweep_id,
                    max_delete_ratio,
                    hwm_clause,
                    hwm_val,
                )

                cur.execute(
                    f"""
                    EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
                    DELETE FROM destination_subscriptions d
                    WHERE d.tenant_id = %s
                      AND d.is_deleted = FALSE
                      AND {hwm_clause}
                      AND NOT EXISTS (
                          SELECT 1 FROM unlogged_seen_ids s
                          WHERE s.sweep_id = %s
                            AND s.tenant_id = d.tenant_id
                            AND s.id = d.id
                      )
                    """,
                    (tenant_id, hwm_val, sweep_id),
                )
                explain_row = cur.fetchone()[0]
                finalize_completed_sweep(cur, sweep_id)

            conn.commit()
        except Exception as exc:
            abort_sweep_on_db_error(conn, sweep_id, exc)

        post_scan_db_ms = (time.perf_counter() - t_post) * 1000.0
        del_ms, shared_hits = parse_explain_json(explain_row)

        return SweepExecutionStats(
            api_requests=scan.api_requests,
            response_body_bytes=scan.response_body_bytes,
            pre_scan_db_ms=pre_scan_db_ms,
            post_scan_db_ms=post_scan_db_ms,
            total_db_reconcile_ms=pre_scan_db_ms + post_scan_db_ms,
            delete_query_plan_ms=del_ms,
            delete_plan_root_shared_hits=shared_hits,
        )
    except BaseException as exc:
        active_exc = exc
        raise
    finally:
        release_tenant_sweep_lock(conn, tenant_id, active_exc=active_exc)


async def run_strategy_two_pass_quarantine(
    client: httpx.AsyncClient,
    conn: psycopg.Connection,
    tenant_id: str,
    sweep_started_at: datetime,
    page_size: int = 100,
    on_page_fetched_sweep_1: Callable[[int], None] | None = None,
    after_sweep_1_hook: Callable[[], None] | None = None,
    max_delete_ratio: float = 0.20,
    seal_lock_timeout: str = "5s",
    seal_statement_timeout: str | None = None,
) -> SweepExecutionStats:
    """Strategy 3: Two-phase quarantine purging records missing across consecutive completed sweeps."""
    t_pre = time.perf_counter()
    acquire_tenant_sweep_lock(conn, tenant_id)
    active_exc: BaseException | None = None
    try:
        total_api_reqs = 0
        total_body_bytes = 0
        post_scan_db_ms = 0.0
        total_del_plan_ms = 0.0
        total_shared_hits = 0

        hwm_seq = capture_destination_hwm_seq(conn)
        sealed_epoch = seal_tenant_ingestion_epoch(
            conn,
            tenant_id,
            lock_timeout=seal_lock_timeout,
            statement_timeout=seal_statement_timeout,
        )
        pre_scan_db_ms = (time.perf_counter() - t_pre) * 1000.0

        for sweep_num in (1, 2):
            sweep_id = uuid.uuid4()
            scan = await fetch_keyset_pages_fail_closed(
                client=client,
                conn=conn,
                sweep_id=sweep_id,
                tenant_id=tenant_id,
                strategy_name=f"strategy_3_quarantine_pass_{sweep_num}",
                sweep_started_at=sweep_started_at,
                hwm_seq=hwm_seq,
                sealed_epoch=sealed_epoch,
                page_size=page_size,
                on_page_fetched=on_page_fetched_sweep_1 if sweep_num == 1 else None,
            )
            pre_scan_db_ms += scan.init_db_ms
            total_api_reqs += scan.api_requests
            total_body_bytes += scan.response_body_bytes

            t_post = time.perf_counter()
            try:
                with conn.cursor() as cur:
                    stage_seen_ids(cur, sweep_id, tenant_id, scan.pages_fetched, scan.seen_ids)

                    assert_deletion_safety_bound(
                        cur,
                        tenant_id,
                        sweep_id,
                        max_delete_ratio,
                        "d.ingestion_epoch <= %s",
                        sealed_epoch,
                    )

                    cur.execute(
                        """
                        UPDATE destination_subscriptions d
                        SET missing_streak = missing_streak + 1,
                            last_missed_sweep_seq = %s
                        WHERE d.tenant_id = %s
                          AND d.is_deleted = FALSE
                          AND d.ingestion_epoch <= %s
                          AND d.last_missed_sweep_seq < %s
                          AND NOT EXISTS (
                              SELECT 1 FROM unlogged_seen_ids s
                              WHERE s.sweep_id = %s
                                AND s.tenant_id = d.tenant_id
                                AND s.id = d.id
                          )
                        """,
                        (scan.sweep_seq, tenant_id, sealed_epoch, scan.sweep_seq, sweep_id),
                    )

                    cur.execute(
                        """
                        UPDATE destination_subscriptions d
                        SET missing_streak = 0,
                            last_missed_sweep_seq = 0
                        WHERE d.tenant_id = %s
                          AND d.missing_streak > 0
                          AND EXISTS (
                              SELECT 1 FROM unlogged_seen_ids s
                              WHERE s.sweep_id = %s
                                AND s.tenant_id = d.tenant_id
                                AND s.id = d.id
                          )
                        """,
                        (tenant_id, sweep_id),
                    )

                    cur.execute(
                        """
                        EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
                        DELETE FROM destination_subscriptions
                        WHERE tenant_id = %s
                          AND is_deleted = FALSE
                          AND missing_streak >= 2
                        """,
                        (tenant_id,),
                    )
                    explain_row = cur.fetchone()[0]
                    finalize_completed_sweep(cur, sweep_id)

                conn.commit()
            except Exception as exc:
                abort_sweep_on_db_error(conn, sweep_id, exc)

            post_scan_db_ms += (time.perf_counter() - t_post) * 1000.0
            del_ms, shared_hits = parse_explain_json(explain_row)
            total_del_plan_ms += del_ms
            total_shared_hits += shared_hits

            if sweep_num == 1 and after_sweep_1_hook:
                after_sweep_1_hook()

        return SweepExecutionStats(
            api_requests=total_api_reqs,
            response_body_bytes=total_body_bytes,
            pre_scan_db_ms=pre_scan_db_ms,
            post_scan_db_ms=post_scan_db_ms,
            total_db_reconcile_ms=pre_scan_db_ms + post_scan_db_ms,
            delete_query_plan_ms=total_del_plan_ms,
            delete_plan_root_shared_hits=total_shared_hits,
        )
    except BaseException as exc:
        active_exc = exc
        raise
    finally:
        release_tenant_sweep_lock(conn, tenant_id, active_exc=active_exc)


async def run_strategy_snapshot_cursor(
    client: httpx.AsyncClient,
    conn: psycopg.Connection,
    tenant_id: str,
    sweep_started_at: datetime,
    page_size: int = 100,
    on_page_fetched: Callable[[int], None] | None = None,
    max_delete_ratio: float = 0.20,
    seal_lock_timeout: str = "5s",
    seal_statement_timeout: str | None = None,
) -> SweepExecutionStats:
    """Strategy 4: Consistent snapshot pagination with UNLOGGED staging and epoch barrier."""
    t_pre = time.perf_counter()
    acquire_tenant_sweep_lock(conn, tenant_id)
    active_exc: BaseException | None = None
    try:
        hwm_seq = capture_destination_hwm_seq(conn)
        sealed_epoch = seal_tenant_ingestion_epoch(
            conn,
            tenant_id,
            lock_timeout=seal_lock_timeout,
            statement_timeout=seal_statement_timeout,
        )
        pre_scan_db_ms = (time.perf_counter() - t_pre) * 1000.0

        init_resp = await client.post("/records/snapshot/init", params={"tenant_id": tenant_id})
        init_resp.raise_for_status()
        snapshot_id = init_resp.json()["snapshot_id"]

        sweep_id = uuid.uuid4()
        scan = await fetch_keyset_pages_fail_closed(
            client=client,
            conn=conn,
            sweep_id=sweep_id,
            tenant_id=tenant_id,
            strategy_name="strategy_4_snapshot_cursor",
            sweep_started_at=sweep_started_at,
            hwm_seq=hwm_seq,
            sealed_epoch=sealed_epoch,
            page_size=page_size,
            on_page_fetched=on_page_fetched,
            snapshot_id=snapshot_id,
        )
        pre_scan_db_ms += scan.init_db_ms
        total_api_reqs = scan.api_requests + 1
        total_body_bytes = scan.response_body_bytes + len(init_resp.content)

        t_post = time.perf_counter()
        try:
            with conn.cursor() as cur:
                stage_seen_ids(cur, sweep_id, tenant_id, scan.pages_fetched, scan.seen_ids)

                assert_deletion_safety_bound(
                    cur,
                    tenant_id,
                    sweep_id,
                    max_delete_ratio,
                    "d.ingestion_epoch <= %s",
                    sealed_epoch,
                )

                cur.execute(
                    """
                    EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
                    DELETE FROM destination_subscriptions d
                    WHERE d.tenant_id = %s
                      AND d.is_deleted = FALSE
                      AND d.ingestion_epoch <= %s
                      AND NOT EXISTS (
                          SELECT 1 FROM unlogged_seen_ids s
                          WHERE s.sweep_id = %s
                            AND s.tenant_id = d.tenant_id
                            AND s.id = d.id
                      )
                    """,
                    (tenant_id, sealed_epoch, sweep_id),
                )
                explain_row = cur.fetchone()[0]
                finalize_completed_sweep(cur, sweep_id)

            conn.commit()
        except Exception as exc:
            abort_sweep_on_db_error(conn, sweep_id, exc)

        post_scan_db_ms = (time.perf_counter() - t_post) * 1000.0
        del_ms, shared_hits = parse_explain_json(explain_row)

        return SweepExecutionStats(
            api_requests=total_api_reqs,
            response_body_bytes=total_body_bytes,
            pre_scan_db_ms=pre_scan_db_ms,
            post_scan_db_ms=post_scan_db_ms,
            total_db_reconcile_ms=pre_scan_db_ms + post_scan_db_ms,
            delete_query_plan_ms=del_ms,
            delete_plan_root_shared_hits=shared_hits,
        )
    except BaseException as exc:
        active_exc = exc
        raise
    finally:
        release_tenant_sweep_lock(conn, tenant_id, active_exc=active_exc)
