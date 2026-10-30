"""Data models for reconciliation sweeps, subscription entities, and execution metrics."""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from pydantic import BaseModel


@dataclass
class SweepExecutionStats:
    """Detailed performance and buffer metrics captured during a single sweep."""

    api_requests: int
    response_body_bytes: int
    pre_scan_db_ms: float
    post_scan_db_ms: float
    total_db_reconcile_ms: float
    delete_query_plan_ms: float
    delete_plan_root_shared_hits: Optional[int]


class SubscriptionRecord(BaseModel):
    """Represents a subscription row in the source SaaS API."""

    tenant_id: str
    id: int
    mrr_cents: int
    is_deleted: bool
    source_created_at: datetime
    updated_at: datetime
    transient_omit_sweeps_left: int = 0
