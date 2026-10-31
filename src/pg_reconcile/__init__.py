"""PostgreSQL API reconciliation engine and mutation benchmarking suite."""

from .engine import (
    ConcurrentSweepError,
    IncompleteSweepError,
    MassDeletionAbortedError,
    acquire_tenant_sweep_lock,
    assert_deletion_safety_bound,
    assert_disposable_database,
    capture_destination_hwm_seq,
    finalize_completed_sweep,
    ingest_subscription_transactional,
    release_tenant_sweep_lock,
    seal_tenant_ingestion_epoch,
    stage_seen_ids,
    tenant_advisory_lock_id,
)
from .models import KeysetScanResult, SubscriptionRecord, SweepExecutionStats
from .strategies import (
    run_strategy_incremental,
    run_strategy_naive_offset,
    run_strategy_snapshot_cursor,
    run_strategy_staged_keyset,
    run_strategy_two_pass_quarantine,
)

__all__ = [
    "ConcurrentSweepError",
    "IncompleteSweepError",
    "KeysetScanResult",
    "MassDeletionAbortedError",
    "SubscriptionRecord",
    "SweepExecutionStats",
    "acquire_tenant_sweep_lock",
    "assert_deletion_safety_bound",
    "assert_disposable_database",
    "capture_destination_hwm_seq",
    "finalize_completed_sweep",
    "ingest_subscription_transactional",
    "release_tenant_sweep_lock",
    "run_strategy_incremental",
    "run_strategy_naive_offset",
    "run_strategy_snapshot_cursor",
    "run_strategy_staged_keyset",
    "run_strategy_two_pass_quarantine",
    "seal_tenant_ingestion_epoch",
    "stage_seen_ids",
    "tenant_advisory_lock_id",
]
