"""PostgreSQL 16 API reconciliation primitives.

Handles advisory locking, epoch watermarks, unlogged table staging,
and deletion safety bounds.
"""

from datetime import datetime
import hashlib
import json
import logging
import time
from typing import Callable, List, Optional, Tuple
import uuid
import httpx
import psycopg
from psycopg.pq import TransactionStatus

logger = logging.getLogger(__name__)


class IncompleteSweepError(RuntimeError):
    """Raised when an API or database failure interrupts a sweep."""


class ConcurrentSweepError(RuntimeError):
    """Raised when another sweep already holds the tenant advisory lock."""


class MassDeletionAbortedError(IncompleteSweepError):
    """Raised when an empty or truncated scan would purge too many rows."""


def tenant_advisory_lock_id(tenant_id: str) -> int:
    """Map tenant string to a stable 64-bit lock key for Postgres."""
    digest = hashlib.sha256(f"reconcile_lock:{tenant_id}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=True)


def parse_explain_json(explain_row: object) -> Tuple[float, int]:
    """Parse execution time and shared buffer hits from EXPLAIN ANALYZE JSON."""
    data = json.loads(explain_row) if isinstance(explain_row, str) else explain_row
    top = data[0]
    exec_ms = float(top.get("Execution Time", 0.0))
    root_plan = top.get("Plan", {})
    shared_hits = int(root_plan.get("Shared Hit Blocks", 0))
    return exec_ms, shared_hits


def capture_destination_hwm_seq(conn: psycopg.Connection) -> int:
    """Read the current sequence counter value."""
    with conn.cursor() as cur:
        cur.execute("SELECT last_value FROM dest_ingestion_seq")
        val = int(cur.fetchone()[0])
    conn.commit()
    return val


def seal_tenant_ingestion_epoch(
    conn: psycopg.Connection, tenant_id: str, lock_timeout: str = "5s"
) -> int:
    """Advance the tenant epoch watermark. Blocks until in-flight writers commit."""
    with conn.cursor() as cur:
        cur.execute("SELECT set_config('lock_timeout', %s, true)", (lock_timeout,))
        try:
            cur.execute(
                """
                UPDATE tenant_watermarks
                SET current_epoch = current_epoch + 1
                WHERE tenant_id = %s
                RETURNING current_epoch - 1
                """,
                (tenant_id,),
            )
            sealed_epoch = int(cur.fetchone()[0])
        except (psycopg.errors.LockNotAvailable, psycopg.errors.QueryCanceled) as exc:
            conn.rollback()
            raise IncompleteSweepError(
                f"Timed out acquiring epoch seal lock on tenant '{tenant_id}' behind active ingestion writers"
            ) from exc
        finally:
            try:
                cur.execute("SET lock_timeout = DEFAULT")
            except Exception:
                pass
    conn.commit()
    return sealed_epoch


def ingest_subscription_transactional(
    conn: psycopg.Connection,
    tenant_id: str,
    record_id: int,
    mrr_cents: int,
    is_deleted: bool,
    source_created_at: datetime,
    updated_at: datetime,
    ingested_at: datetime,
    preallocated_seq: Optional[int] = None,
):
    """Upsert a subscription row and assign the current epoch via trigger."""
    with conn.cursor() as cur:
        if preallocated_seq is not None:
            cur.execute(
                """
                INSERT INTO destination_subscriptions
                    (tenant_id, id, mrr_cents, is_deleted, source_created_at, updated_at,
                     ingested_at, ingestion_seq)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (tenant_id, id) DO UPDATE SET
                    mrr_cents = EXCLUDED.mrr_cents,
                    is_deleted = EXCLUDED.is_deleted,
                    updated_at = EXCLUDED.updated_at,
                    ingested_at = EXCLUDED.ingested_at,
                    ingestion_seq = EXCLUDED.ingestion_seq
                """,
                (
                    tenant_id,
                    record_id,
                    mrr_cents,
                    is_deleted,
                    source_created_at,
                    updated_at,
                    ingested_at,
                    preallocated_seq,
                ),
            )
        else:
            cur.execute(
                """
                INSERT INTO destination_subscriptions
                    (tenant_id, id, mrr_cents, is_deleted, source_created_at, updated_at,
                     ingested_at, ingestion_seq)
                VALUES (%s, %s, %s, %s, %s, %s, %s, nextval('dest_ingestion_seq'))
                ON CONFLICT (tenant_id, id) DO UPDATE SET
                    mrr_cents = EXCLUDED.mrr_cents,
                    is_deleted = EXCLUDED.is_deleted,
                    updated_at = EXCLUDED.updated_at,
                    ingested_at = EXCLUDED.ingested_at,
                    ingestion_seq = nextval('dest_ingestion_seq')
                """,
                (
                    tenant_id,
                    record_id,
                    mrr_cents,
                    is_deleted,
                    source_created_at,
                    updated_at,
                    ingested_at,
                ),
            )


def acquire_tenant_sweep_lock(conn: psycopg.Connection, tenant_id: str):
    """Acquire the session advisory lock and clear any abandoned runs."""
    if conn.info.transaction_status == TransactionStatus.INERROR:
        conn.rollback()

    lock_id = tenant_advisory_lock_id(tenant_id)
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_id,))
        acquired = cur.fetchone()[0]
    conn.commit()
    if not acquired:
        raise ConcurrentSweepError(
            f"Active reconciliation sweep already holds lock for tenant '{tenant_id}'"
        )

    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM unlogged_seen_ids
            WHERE sweep_id IN (
                SELECT sweep_id FROM reconciliation_sweeps
                WHERE tenant_id = %s AND status IN ('STARTED', 'STAGED')
            )
            """,
            (tenant_id,),
        )
        cur.execute(
            """
            UPDATE reconciliation_sweeps
            SET status = 'ABORTED'
            WHERE tenant_id = %s AND status IN ('STARTED', 'STAGED')
            """,
            (tenant_id,),
        )
    conn.commit()


def release_tenant_sweep_lock(
    conn: psycopg.Connection, tenant_id: str, active_exc: Optional[BaseException] = None
):
    """Release the session advisory lock, closing the connection if release fails."""
    lock_id = tenant_advisory_lock_id(tenant_id)
    try:
        if conn.closed:
            if active_exc is None:
                raise RuntimeError(
                    f"Connection closed before releasing advisory lock for '{tenant_id}'"
                )
            return

        if conn.info.transaction_status != TransactionStatus.IDLE:
            conn.rollback()

        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (lock_id,))
            unlocked = bool(cur.fetchone()[0])
        conn.commit()

        if not unlocked:
            conn.close()
            if active_exc is None:
                raise RuntimeError(
                    f"pg_advisory_unlock returned False for tenant '{tenant_id}'; connection closed"
                )
    except Exception as unlock_exc:
        logger.error(
            "Failed to release advisory lock for tenant %s: %s", tenant_id, unlock_exc
        )
        if not conn.closed:
            conn.close()
        if active_exc is None:
            raise


def abort_sweep_on_db_error(
    conn: psycopg.Connection, sweep_id: uuid.UUID, exc: Exception
) -> None:
    """Mark the sweep aborted and clean up staging rows."""
    conn.rollback()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE reconciliation_sweeps SET status = 'ABORTED' WHERE sweep_id = %s",
            (sweep_id,),
        )
        cur.execute("DELETE FROM unlogged_seen_ids WHERE sweep_id = %s", (sweep_id,))
    conn.commit()
    if isinstance(exc, IncompleteSweepError):
        raise exc
    raise IncompleteSweepError(f"Database staging/reconcile rolled back: {exc}") from exc


def assert_deletion_safety_bound(
    cur: psycopg.Cursor,
    tenant_id: str,
    sweep_id: uuid.UUID,
    max_delete_ratio: float,
    hwm_condition_sql: str,
    hwm_param: object,
):
    """Check deletion count against safety bounds before running DELETE."""
    cur.execute(
        f"""
        SELECT COUNT(*) FROM destination_subscriptions d
        WHERE d.tenant_id = %s
          AND d.is_deleted = FALSE
          AND {hwm_condition_sql}
        """,
        (tenant_id, hwm_param),
    )
    eligible_count = int(cur.fetchone()[0])

    if eligible_count == 0:
        return

    cur.execute(
        f"""
        SELECT COUNT(*) FROM destination_subscriptions d
        WHERE d.tenant_id = %s
          AND d.is_deleted = FALSE
          AND {hwm_condition_sql}
          AND NOT EXISTS (
              SELECT 1 FROM unlogged_seen_ids s
              WHERE s.sweep_id = %s
                AND s.tenant_id = d.tenant_id
                AND s.id = d.id
          )
        """,
        (tenant_id, hwm_param, sweep_id),
    )
    candidates_to_delete = int(cur.fetchone()[0])

    cur.execute(
        "SELECT COUNT(*) FROM unlogged_seen_ids WHERE sweep_id = %s", (sweep_id,)
    )
    seen_staged = int(cur.fetchone()[0])

    if seen_staged == 0 and eligible_count > 0:
        raise MassDeletionAbortedError(
            f"Safety guard: scan returned 0 records for tenant '{tenant_id}', "
            f"which would wipe out all {eligible_count} active records. Sweep aborted."
        )

    delete_ratio = candidates_to_delete / eligible_count
    if delete_ratio > max_delete_ratio:
        raise MassDeletionAbortedError(
            f"Safety guard: sweep would delete {candidates_to_delete}/{eligible_count} "
            f"({delete_ratio:.1%}) records for tenant '{tenant_id}', exceeding safety limit of "
            f"{max_delete_ratio:.0%}. Sweep aborted."
        )


async def fetch_keyset_pages_fail_closed(
    client: httpx.AsyncClient,
    conn: psycopg.Connection,
    sweep_id: uuid.UUID,
    tenant_id: str,
    strategy_name: str,
    sweep_started_at: datetime,
    hwm_seq: int,
    sealed_epoch: int,
    page_size: int,
    on_page_fetched: Optional[Callable[[int], None]] = None,
    snapshot_id: Optional[str] = None,
) -> Tuple[List[int], int, int, int, int, float]:
    """Fetch paginated records, validating order and cursor progression."""
    t_init = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO reconciliation_sweeps
                (sweep_id, tenant_id, strategy_name, status, sweep_started_at, hwm_ingestion_seq, sealed_epoch)
            VALUES (%s, %s, %s, 'STARTED', %s, %s, %s)
            RETURNING sweep_seq
            """,
            (sweep_id, tenant_id, strategy_name, sweep_started_at, hwm_seq, sealed_epoch),
        )
        sweep_seq = int(cur.fetchone()[0])
    conn.commit()
    init_db_ms = (time.perf_counter() - t_init) * 1000.0

    seen_ids: List[int] = []
    after_id = 0
    page_num = 0
    api_reqs = 0
    body_bytes = 0

    try:
        while True:
            if snapshot_id:
                resp = await client.get(
                    "/records/snapshot",
                    params={
                        "snapshot_id": snapshot_id,
                        "after_id": after_id,
                        "limit": page_size,
                    },
                )
            else:
                resp = await client.get(
                    "/records/keyset",
                    params={
                        "tenant_id": tenant_id,
                        "after_id": after_id,
                        "limit": page_size,
                    },
                )
            api_reqs += 1
            body_bytes += len(resp.content)

            if resp.status_code != 200:
                raise IncompleteSweepError(
                    f"Upstream API error HTTP {resp.status_code} on page {page_num + 1}"
                )

            data = resp.json()
            batch: List[int] = data["ids"]
            has_more: bool = data["has_more"]
            next_cursor: Optional[int] = data["next_after_id"]

            if not batch and has_more:
                raise IncompleteSweepError(
                    "Truncated stream: received empty page while has_more=True"
                )

            if not batch:
                break

            prev = after_id
            for item_id in batch:
                if item_id <= prev:
                    raise IncompleteSweepError(
                        f"Unsorted or duplicate ID {item_id} after {prev} in page {page_num + 1}"
                    )
                prev = item_id

            if next_cursor is None or next_cursor != batch[-1]:
                raise IncompleteSweepError(
                    f"Malformed cursor: next_after_id={next_cursor} != batch[-1]={batch[-1]}"
                )

            seen_ids.extend(batch)
            page_num += 1
            if on_page_fetched:
                on_page_fetched(page_num)

            if not has_more:
                break
            after_id = next_cursor

    except Exception as exc:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE reconciliation_sweeps
                SET status = 'ABORTED', pages_fetched = %s, records_seen = %s
                WHERE sweep_id = %s
                """,
                (page_num, len(seen_ids), sweep_id),
            )
            cur.execute("DELETE FROM unlogged_seen_ids WHERE sweep_id = %s", (sweep_id,))
        conn.commit()
        if isinstance(exc, IncompleteSweepError):
            raise
        raise IncompleteSweepError(f"Sweep aborted due to error: {exc}") from exc

    return seen_ids, page_num, api_reqs, body_bytes, sweep_seq, init_db_ms
