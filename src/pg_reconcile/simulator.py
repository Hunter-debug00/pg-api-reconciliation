"""In-process FastAPI simulator mocking an upstream SaaS subscription API.

Simulates incremental syncs, offset pagination, keyset pagination,
and snapshot cursors, with fault injection hooks for edge cases.
"""

from copy import deepcopy
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from fastapi import FastAPI, HTTPException, Query
import httpx

from pg_reconcile.models import SubscriptionRecord

app = FastAPI(title="SaaS Subscription API Simulator")


class SourceState:
    """In-memory dataset of upstream tenant subscriptions."""

    def __init__(self):
        self.records: Dict[Tuple[str, int], SubscriptionRecord] = {}
        self.snapshots: Dict[str, List[SubscriptionRecord]] = {}
        self.fail_on_page: Optional[int] = None
        self.timeout_on_page: Optional[int] = None
        self.return_truncated_page: Optional[int] = None
        self.return_malformed_cursor_page: Optional[int] = None
        self.return_unsorted_batch_page: Optional[int] = None
        self.expire_snapshot_on_page: Optional[int] = None
        self.requests_served_in_sweep: int = 0

    def seed_dataset(
        self, base_time: Optional[datetime] = None, records_per_tenant: int = 5000
    ):
        """Seed active records across tenants."""
        self.records.clear()
        self.snapshots.clear()
        self.reset_faults()
        t0 = base_time or datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

        for tenant in ("acme", "globex"):
            for idx in range(1, records_per_tenant + 1):
                rec_id = idx * 10
                self.records[(tenant, rec_id)] = SubscriptionRecord(
                    tenant_id=tenant,
                    id=rec_id,
                    mrr_cents=5000,
                    is_deleted=False,
                    source_created_at=t0,
                    updated_at=t0,
                    transient_omit_sweeps_left=0,
                )

    def reset_faults(self):
        """Clear active fault hooks."""
        self.fail_on_page = None
        self.timeout_on_page = None
        self.return_truncated_page = None
        self.return_malformed_cursor_page = None
        self.return_unsorted_batch_page = None
        self.expire_snapshot_on_page = None
        self.requests_served_in_sweep = 0

    def hard_delete(self, tenant_id: str, record_id: int):
        """Remove a record entirely (hard delete)."""
        self.records.pop((tenant_id, record_id), None)

    def soft_delete(self, tenant_id: str, record_id: int, now: datetime):
        """Mark a record as deleted (soft delete)."""
        key = (tenant_id, record_id)
        if key in self.records:
            self.records[key].is_deleted = True
            self.records[key].updated_at = now

    def insert_new(
        self,
        tenant_id: str,
        record_id: int,
        source_created_at: datetime,
        updated_at: datetime,
        mrr_cents: int = 10000,
    ) -> SubscriptionRecord:
        """Add a new subscription record."""
        rec = SubscriptionRecord(
            tenant_id=tenant_id,
            id=record_id,
            mrr_cents=mrr_cents,
            is_deleted=False,
            source_created_at=source_created_at,
            updated_at=updated_at,
            transient_omit_sweeps_left=0,
        )
        self.records[(tenant_id, record_id)] = rec
        return rec

    def set_transient_omission(self, tenant_id: str, record_id: int, sweeps: int = 1):
        """Simulate replica lag by hiding a record for N sweeps."""
        key = (tenant_id, record_id)
        if key in self.records:
            self.records[key].transient_omit_sweeps_left = sweeps

    def decrement_transient_omissions(self, tenant_id: str):
        """Advance time across sweeps, decrementing replica lag counters."""
        for (t_id, _), rec in self.records.items():
            if t_id == tenant_id and rec.transient_omit_sweeps_left > 0:
                rec.transient_omit_sweeps_left -= 1

    def create_snapshot(self, tenant_id: str, snapshot_id: str):
        """Freeze current active records into a snapshot."""
        active = sorted(
            (
                deepcopy(r)
                for r in self.records.values()
                if r.tenant_id == tenant_id
                and not r.is_deleted
                and r.transient_omit_sweeps_left == 0
            ),
            key=lambda r: r.id,
        )
        self.snapshots[snapshot_id] = active


STATE = SourceState()


def _check_fault_injection():
    STATE.requests_served_in_sweep += 1
    req = STATE.requests_served_in_sweep
    if STATE.fail_on_page and req == STATE.fail_on_page:
        raise HTTPException(status_code=500, detail="Simulated upstream API 500 error")
    if STATE.timeout_on_page and req == STATE.timeout_on_page:
        raise httpx.ReadTimeout("Simulated upstream network read timeout")


@app.get("/records/incremental")
def list_records_incremental(
    tenant_id: str = Query(...),
    updated_after: datetime = Query(...),
    after_id: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """Incremental sync endpoint returning recently updated rows and tombstones."""
    _check_fault_injection()
    matches = sorted(
        (
            r
            for r in STATE.records.values()
            if r.tenant_id == tenant_id
            and (
                r.updated_at > updated_after
                or (r.updated_at == updated_after and after_id > 0 and r.id > after_id)
            )
        ),
        key=lambda r: (r.updated_at, r.id),
    )
    page = matches[:limit]
    has_more = len(matches) > limit
    next_cursor_time = page[-1].updated_at.isoformat() if page else None
    next_after_id = page[-1].id if page else 0
    return {
        "records": [r.model_dump(mode="json") for r in page],
        "has_more": has_more,
        "next_updated_after": next_cursor_time,
        "next_after_id": next_after_id,
    }


@app.get("/records/offset")
def list_records_offset(
    tenant_id: str = Query(...),
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """OFFSET/LIMIT pagination endpoint vulnerable to boundary drift."""
    _check_fault_injection()
    active = sorted(
        (
            r
            for r in STATE.records.values()
            if r.tenant_id == tenant_id
            and not r.is_deleted
            and r.transient_omit_sweeps_left == 0
        ),
        key=lambda r: r.id,
    )
    page = active[offset : offset + limit]
    has_more = (offset + limit) < len(active)
    return {
        "ids": [r.id for r in page],
        "has_more": has_more,
    }


@app.get("/records/keyset")
def list_records_keyset(
    tenant_id: str = Query(...),
    after_id: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """Keyset pagination endpoint using after_id cursors."""
    _check_fault_injection()
    req_num = STATE.requests_served_in_sweep

    if STATE.return_truncated_page and req_num == STATE.return_truncated_page:
        return {"ids": [], "next_after_id": after_id, "has_more": True}

    active = sorted(
        (
            r
            for r in STATE.records.values()
            if r.tenant_id == tenant_id
            and not r.is_deleted
            and r.id > after_id
            and r.transient_omit_sweeps_left == 0
        ),
        key=lambda r: r.id,
    )
    page = active[:limit]
    has_more = len(active) > limit
    ids = [r.id for r in page]
    next_cursor = ids[-1] if ids else None

    if STATE.return_malformed_cursor_page and req_num == STATE.return_malformed_cursor_page:
        next_cursor = after_id

    if (
        STATE.return_unsorted_batch_page
        and req_num == STATE.return_unsorted_batch_page
        and len(ids) >= 2
    ):
        ids[0], ids[1] = ids[1], ids[0]

    return {
        "ids": ids,
        "next_after_id": next_cursor,
        "has_more": has_more,
    }


@app.post("/records/snapshot/init")
def init_snapshot(tenant_id: str = Query(...)):
    """Initialize a point-in-time snapshot cursor at sweep start."""
    snapshot_id = f"snap_{tenant_id}_{len(STATE.snapshots) + 1}"
    STATE.create_snapshot(tenant_id, snapshot_id)
    return {"snapshot_id": snapshot_id}


@app.get("/records/snapshot")
def list_records_snapshot(
    snapshot_id: str = Query(...),
    after_id: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """Paginate over a frozen point-in-time snapshot."""
    _check_fault_injection()
    if (
        STATE.expire_snapshot_on_page
        and STATE.requests_served_in_sweep == STATE.expire_snapshot_on_page
    ):
        STATE.snapshots.pop(snapshot_id, None)

    if snapshot_id not in STATE.snapshots:
        raise HTTPException(status_code=404, detail="Snapshot expired or not found")

    snap_records = [r for r in STATE.snapshots[snapshot_id] if r.id > after_id]
    page = snap_records[:limit]
    has_more = len(snap_records) > limit
    next_cursor = page[-1].id if page else None
    return {
        "ids": [r.id for r in page],
        "next_after_id": next_cursor,
        "has_more": has_more,
    }
