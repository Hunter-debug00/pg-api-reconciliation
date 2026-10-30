# PostgreSQL API Reconciliation: Concurrency, Mutations & Anti-Joins

A benchmark comparing seven strategies for purging hard-deleted rows from PostgreSQL without dropping active records during concurrent API mutations.

---

## Overview

When downstream databases sync records from third-party SaaS APIs (such as Stripe, Shopify, HubSpot, or internal microservices), mutations generally arrive via incremental endpoints:

```text
GET /subscriptions?updated_after=2026-01-01T12:00:00Z
```

These changes are upserted into PostgreSQL using `INSERT ... ON CONFLICT (tenant_id, id) DO UPDATE`.

This works reliably until the upstream system **hard-deletes** a record instead of publishing a soft-delete tombstone (`is_deleted = true`). Because hard-deleted records no longer exist at the source, they never appear in incremental cursor queries. Downstream in PostgreSQL, those records remain permanently as **ghost rows**.

In billing systems, ghost rows quietly corrupt financial aggregates, artificially inflating Monthly Recurring Revenue (MRR) and skewing subscription metrics.

To eliminate ghost rows, teams often implement periodic full reconciliation sweeps:
1. Paginate through all active upstream records.
2. Collect the set of returned IDs.
3. Delete local records absent from the scan:
   ```sql
   DELETE FROM destination_subscriptions
   WHERE tenant_id = 'acme'
     AND id NOT IN (seen_ids);
   ```

Under concurrent traffic, this naive anti-join introduces a failure mode far worse than ghost rows: **valid, active customer records are skipped during pagination and falsely deleted from PostgreSQL.**

This repository models, reproduces, and benchmarks seven reconciliation strategies under deterministic concurrent mutations, quantifying correctness, concurrency barriers, and PostgreSQL execution costs.

---

## Core Failure Modes

### 1. OFFSET Boundary Slip

When an API paginates using `OFFSET` and `LIMIT`, any deletion occurring ahead of the pagination cursor shifts subsequent records backward by one index position.

```text
Page 1 Fetch (OFFSET 0, LIMIT 100):
  [ ID 10 ... ID 420 ... ID 1000 ]   (Client receives IDs 10..1000)
                 |
      ID 420 hard-deleted at source before Page 2 request
                 v
Source index shifts left by 1:
  Index 98: ID 1000
  Index 99: ID 1010  (Slips from index 100 to index 99, inside Page 1 range)
  Index 100: ID 1020 (Page 2 OFFSET 100, LIMIT 100 starts here)

Result:
  ID 1010 was never returned on Page 1 or Page 2.
  Anti-join deletes ID 1010 from PostgreSQL despite remaining active at source.
```

Switching to monotonic keyset pagination (`WHERE id > cursor LIMIT 100`) solves positional drift, but keyset pagination alone does not protect against concurrent writes.

---

### 2. The Non-Transactional Sequence Race

To prevent deleting records created *while* the reconciler is paginating, systems often capture a high-water mark (HWM) before scanning starts:
- Timestamp watermark: `DELETE WHERE source_created_at < sweep_start_time`
- Monotonic sequence watermark: `DELETE WHERE ingestion_seq <= hwm_seq`

With high-concurrency ingestion writers, sequence allocation is non-transactional. PostgreSQL `nextval()` advances immediately outside the transaction boundary:

```text
Ingestion Tx A:   BEGIN
Ingestion Tx A:   nextval('dest_ingestion_seq') -> allocates 10001 for ID 485 (uncommitted)

Reconciler B:     Captures sequence HWM = 10001
Reconciler B:     Paginates past ID 485 (ID 485 not yet committed or visible)

Ingestion Tx A:   COMMIT (ID 485 committed with ingestion_seq = 10001 <= HWM)

Reconciler B:     DELETE WHERE ingestion_seq <= 10001
                  -> Deletes active ID 485 because 10001 <= HWM!
```

---

### 3. The Transactional Epoch Barrier Solution

To close this race window without table-level write locks, we implement a database-enforced epoch barrier using PostgreSQL row locks on a lightweight `tenant_watermarks` table.

Every ingestion write automatically acquires a `FOR SHARE` lock on its tenant's watermark row via a `BEFORE INSERT OR UPDATE` trigger:

```sql
CREATE OR REPLACE FUNCTION enforce_tenant_epoch_and_quarantine()
RETURNS TRIGGER AS $$
DECLARE
    v_epoch BIGINT;
BEGIN
    SELECT current_epoch INTO v_epoch
    FROM tenant_watermarks
    WHERE tenant_id = NEW.tenant_id
    FOR SHARE;

    NEW.ingestion_epoch := v_epoch;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

When a reconciliation sweep starts, it executes:

```sql
UPDATE tenant_watermarks
SET current_epoch = current_epoch + 1
WHERE tenant_id = 'acme'
RETURNING current_epoch - 1;
```

This `UPDATE` requests an exclusive row lock on the watermark row. PostgreSQL holds the update until all active writers in the current epoch have committed. Once unblocked, the prior epoch is sealed. All subsequent writes automatically stamp into the new epoch.

The reconciler then scopes its anti-join strictly to the sealed epoch:

```sql
DELETE FROM destination_subscriptions d
WHERE d.tenant_id = 'acme'
  AND d.is_deleted = FALSE
  AND d.ingestion_epoch <= :sealed_epoch
  AND NOT EXISTS (
      SELECT 1 FROM unlogged_seen_ids s
      WHERE s.sweep_id = :sweep_id
        AND s.tenant_id = d.tenant_id
        AND s.id = d.id
  );
```

Result: zero false deletions caused by in-flight commits.

---

## Benchmark Results

The benchmark harness tests seven configurations against 10,000 subscription records (5,000 per tenant, overlapping ID space `10..50000`, page size 100) under concurrent mutations:

<!-- BENCHMARK_TABLE_START -->
*Benchmark metadata (Git `d7459d5`, Python `3.12.11`, PostgreSQL `16.15`, Seed `42`):*

| Strategy                                   |   Ghosts |   Drift Del |   Backdated Del |   Seq Race Del |   Lag Del |   False Del | Net MRR Diff   | Gross MRR Diff   |   API Reqs | Body Bytes   | Pre-Scan DB   | Post-Scan DB   | Total DB (Med)   | Total DB (P95)   | DEL Plan (Med)   | DEL Plan Root Hits   |
|--------------------------------------------|----------|-------------|-----------------|----------------|-----------|-------------|----------------|------------------|------------|--------------|---------------|----------------|------------------|------------------|------------------|----------------------|
| 0. Incremental tombstone sync              |        2 |           0 |               0 |              0 |         0 |           0 | $100.00        | $100.00          |          1 | 0.8 KB       | 0.00 ms       | 2.67 ms        | 2.67 ms          | 8.53 ms          | 0.00 ms          | N/A                  |
| 1. Naive OFFSET anti-join                  |        1 |           1 |               1 |              1 |         1 |           4 | $200.00        | $300.00          |         50 | 29.4 KB      | 0.00 ms       | 69.94 ms       | 69.94 ms         | 99.46 ms         | 59.73 ms         | 78                   |
| 2a. Keyset with source timestamp watermark |        1 |           0 |               1 |              0 |         1 |           2 | $100.00        | $200.00          |         50 | 30.5 KB      | 3.78 ms       | 49.66 ms       | 53.73 ms         | 96.29 ms         | 9.82 ms          | 15073                |
| 2b. Keyset with sequence watermark         |        1 |           0 |               0 |              1 |         1 |           2 | $50.00         | $150.00          |         50 | 30.5 KB      | 3.70 ms       | 49.28 ms       | 53.45 ms         | 89.53 ms         | 9.39 ms          | 15089                |
| 2c. Transactional epoch barrier            |        1 |           0 |               0 |              0 |         1 |           1 | $0.00          | $100.00          |         50 | 30.5 KB      | 4.95 ms       | 49.04 ms       | 54.85 ms         | 88.63 ms         | 9.60 ms          | 15069                |
| 3. Two-phase quarantine (streak >= 2)      |        1 |           0 |               0 |              0 |         0 |           0 | $50.00         | $50.00           |        100 | 61.0 KB      | 6.50 ms       | 104.32 ms      | 115.68 ms        | 156.93 ms        | 0.11 ms          | 6                    |
| 4. Consistent snapshot pagination          |        2 |           0 |               0 |              0 |         1 |           1 | $50.00         | $150.00          |         51 | 30.5 KB      | 7.12 ms       | 50.68 ms       | 58.24 ms         | 91.91 ms         | 9.33 ms          | 15069                |
<!-- BENCHMARK_TABLE_END -->

### Metric Glossary

- **Drift Del:** Records falsely deleted due to OFFSET boundary shifts.
- **Backdated Del:** Records falsely deleted due to relying on upstream creation timestamps.
- **Seq Race Del:** Records falsely deleted due to uncommitted sequence allocation.
- **Lag Del:** Records falsely deleted due to temporary read-replica omission.
- **Net MRR Diff:** Aggregate error in tenant recurring revenue (`abs(dest_total - source_total)`).
- **Gross MRR Diff:** Record-level L1 absolute revenue error summed across all IDs.
- **Pre-Scan DB:** Median time spent acquiring per-tenant advisory locks and advancing epoch watermarks.
- **Post-Scan DB:** Median time spent bulk-loading staged IDs, checking safety bounds, and executing the anti-join.
- **DEL Plan Root Hits:** Root shared buffer hit count captured via `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)`.

---

## Analysis & Engineering Takeaways

### 1. Net Aggregate Metrics Mask Critical Row-Level Errors

In Strategy 2c, 1 remaining ghost row (ID 420, +$50.00 MRR) cancels out 1 false deletion caused by replica lag (ID 3000, -$50.00 MRR), producing a **$0.00 Net MRR Diff**.

However, the **Gross MRR Diff is $100.00**. Relying solely on dashboard totals hides silent record loss until customers report missing accounts.

### 2. Mass-Deletion Bounds Are Mandatory Defense-in-Depth

Relying solely on pagination stream flags (`has_more = false`) leaves systems vulnerable to upstream API outages. If an upstream gateway returns an HTTP 200 with an empty body (`{"ids": [], "has_more": false}`), an unguarded anti-join purges the entire tenant dataset.

The engine implements an explicit safety guard:

```python
if delete_ratio > max_delete_ratio:
    raise MassDeletionAbortedError(
        f"Sweep would delete {candidates}/{total} ({delete_ratio:.1%}) records, "
        f"exceeding safety limit of {max_delete_ratio:.0%}."
    )
```

### 3. Two-Phase Quarantine Handles Replica Lag

When upstream APIs serve pagination queries from read replicas with replication lag, active records can be temporarily omitted during a sweep.

Strategy 3 introduces an incremental state machine tracking `missing_streak`:
- Records absent from a sweep increment `missing_streak = missing_streak + 1`.
- Any live update or successful scan resets `missing_streak = 0`.
- Only records missing across two consecutive completed sweeps (`missing_streak >= 2`) are purged.

This eliminates false deletions from replica lag without requiring coordinated upstream cache invalidation.

---

## Project Structure

```text
.
├── benchmarks/
│   └── run_benchmark.py       # Seeded 20-trial rotated benchmark harness
├── sql/
│   └── schema.sql             # PostgreSQL tables, indexes, triggers, and sequences
├── src/
│   └── pg_reconcile/
│       ├── __init__.py        # Public module exports
│       ├── engine.py          # Advisory locks, epoch barriers, and staging primitives
│       ├── models.py          # Data classes and metrics structures
│       ├── simulator.py       # Deterministic FastAPI SaaS mutation simulator
│       └── strategies.py      # Reconciliation strategy implementations
├── tests/
│   ├── conftest.py            # Pytest fixtures and mock HTTP transports
│   ├── test_concurrency.py    # Advisory locks, sequence races, and multi-tenant isolation
│   ├── test_safety.py         # Mass-deletion bounds, lock timeouts, and network faults
│   └── test_strategies.py     # Functional correctness tests across strategies
├── BENCHMARK_TABLE.md         # Generated markdown table
├── benchmark_results.json     # Complete benchmark trials and machine metadata
└── pyproject.toml             # Python configuration and test paths
```

---

## Quickstart

### Prerequisites

- Docker with Docker Compose
- Python 3.12+ and `uv`

### Running Locally

```bash
# 1. Start the PostgreSQL 16 container
docker compose -f .devcontainer/docker-compose.yml up -d db

# 2. Sync dependencies
uv sync --locked

# 3. Run Ruff linter
uv run --locked ruff check .

# 4. Run the test suite
uv run --locked pytest -v

# 5. Run the 20-trial seeded benchmark
uv run --locked python benchmarks/run_benchmark.py

# 6. Stop container
docker compose -f .devcontainer/docker-compose.yml down -v
```

> **Note:** The final command removes container volumes. Ensure `DATABASE_URL` points to a disposable local test database before running.
