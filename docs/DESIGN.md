# Design notes

How the sweep works, why each guard exists, and what the benchmark columns mean. The README has the results. This file has the mechanics.

## The sweep lifecycle

Every keyset-based strategy (2a, 2b, 2c, 3, 4) runs the same way, and each sweep leaves a row in `reconciliation_sweeps`:

1. Take a session-level advisory lock for the tenant (`pg_try_advisory_lock`, key derived from a SHA-256 of the tenant ID). If another sweep holds it, raise `ConcurrentSweepError` at once instead of waiting. The same step marks any `STARTED` or `STAGED` sweep left behind by a crashed worker as `ABORTED` and clears its staged IDs.
2. Record the watermark (see below), then insert the sweep as `STARTED`.
3. Page through the API with `after_id` and validate every page. Abort if the HTTP status is not 200, if a page is empty while `has_more` is true, if IDs are not strictly increasing, or if `next_after_id` is not the last ID of the page.
4. Only after the last page, bulk-load the IDs into the `UNLOGGED` table `unlogged_seen_ids` with `COPY`, and set the sweep to `STAGED`.
5. Run the safety check, then the anti-join `DELETE`, then set the sweep to `COMPLETED` and clear the staged IDs, all in one transaction.
6. Any exception rolls back, marks the sweep `ABORTED`, clears staging, and releases the lock. If the unlock itself fails, the connection is closed so the lock cannot leak.

The staging table is `UNLOGGED` because it is disposable. It is skipped by WAL and not copied to standbys, and losing it only means the sweep is retried. The staging key is `(sweep_id, tenant_id, id)`, so two tenants with the same local ID, or two sweeps, never mix.

## The epoch barrier

The problem: a writer allocates `ingestion_seq` with `nextval()`, the sweep reads `last_value` and starts scanning, then the writer commits. The row's number is below the mark but the scan never saw it. Sequences do not roll back and do not follow commit order, so no sequence value can say "everything up to here is committed."

The fix uses row locks, which do follow commit order. `tenant_watermarks` has one row per tenant with a `current_epoch`. A `BEFORE INSERT OR UPDATE` trigger on `destination_subscriptions` reads that row with `FOR SHARE` and stamps the value into the new row's `ingestion_epoch`:

```sql
SELECT current_epoch INTO v_epoch
FROM tenant_watermarks
WHERE tenant_id = NEW.tenant_id
FOR SHARE;

NEW.ingestion_epoch := v_epoch;
```

`FOR SHARE` locks are compatible with each other, so writers do not block one another. They hold the lock until their transaction ends. At sweep start, the sealer runs:

```sql
UPDATE tenant_watermarks
SET current_epoch = current_epoch + 1
WHERE tenant_id = 'acme'
RETURNING current_epoch - 1;
```

That `UPDATE` needs an exclusive row lock, so it waits for every open writer to commit or roll back. When it returns, the old epoch is sealed: every row stamped with it is committed. Later writes get the new epoch. The `DELETE` is restricted to `ingestion_epoch <= sealed_epoch`, so rows that arrive during the scan can never be deleted by it.

The sealer sets `lock_timeout` (default 5 s). That limits each lock wait, and a wait that expires raises `IncompleteSweepError`, so one stalled writer aborts the sweep. See Known gaps for what it does not limit.

Other things the trigger does: it rejects changes to `tenant_id`, resets `missing_streak` on live writes, and preserves the existing epoch when an `UPDATE` increments `missing_streak` with untouched business columns (preventing sweep streak bumps from advancing rows into the newest epoch). Resets of `missing_streak` to zero follow the normal epoch-stamping path.

Trade-offs: every write to a tenant takes a lock on the same row, and the sweep's start waits for open writes. Both costs grow with write volume on a single tenant.

## The safety check

Before any `DELETE`, `assert_deletion_safety_bound` counts the eligible rows (active, in a sealed epoch) and the rows the `DELETE` would remove. It raises `MassDeletionAbortedError` and deletes nothing if the scan staged zero IDs, or if the delete ratio is above `max_delete_ratio` (default 0.20). A legitimate purge bigger than the limit needs the parameter raised on purpose.

## Two-pass quarantine

Strategy 3 runs two sweeps in one call. For rows the scan did not see, `missing_streak` goes up by one. For rows it did see, it resets to zero. Rows with `missing_streak >= 2` are deleted. `last_missed_sweep_seq` stops one sweep from counting twice. In production the two sweeps would run apart, hours or days. The benchmark runs them back to back, with a hook between them that ends the simulated replica lag.

## Snapshot strategy

Strategy 4 asks the simulator for a snapshot (`POST /records/snapshot/init`) and pages through that frozen list. Deletes that happen after the snapshot stay as ghost rows until the next sweep, which is why it ends with two ghosts. If the replica is already lagging when the snapshot is taken, the snapshot inherits the gap, which is the replica lag cell in the results.

## Mutation schedule

The benchmark runs deterministic mutations against tenant `acme` concurrently with page fetches:

| Failure Mode | Target Record | Mutation Timing | Mechanism | Expected Outcome under Naive Scan |
| :--- | :--- | :--- | :--- | :--- |
| **Positional Boundary Shift** | `ID 1010` | Hard delete `ID 420` during Page 1 fetch | Deleting `ID 420` shifts `ID 1010` backward into Page 1's already-read window. | `ID 1010` is omitted from scan and falsely deleted by anti-join. |
| **Hard Delete Discovery** | `ID 9500` | Hard delete `ID 9500` during Page 1 fetch | Record is purged upstream without a tombstone (`is_deleted = true`). | Must be purged by anti-join; incremental sync fails to detect it. |
| **Soft Delete Tombstone** | `ID 1500` | Soft delete `ID 1500` during Page 1 fetch | Record flagged `is_deleted = true` with updated timestamp. | Purged or updated by all strategies. |
| **Backdated Webhook Insertion** | `ID 455` | Insert after Page 2 with historical timestamp | Arrives behind scan cursor with historic `source_created_at`. | Timestamp watermark (`source_created_at < sweep_start`) falsely deletes `ID 455`. |
| **Uncommitted Sequence Race** | `ID 485` | Ingestion sequence pre-allocated before sweep; committed after Page 2 | Sequence counter advances immediately; row arrives behind cursor. | Sequence watermark (`ingestion_seq <= HWM`) falsely deletes `ID 485`. The concurrent multi-connection race is tested in `test_concurrency.py`. |
| **Transient Replica Lag** | `ID 3000` | Omitted from API responses on sweep pass 1; returns on pass 2 | Simulates read-replica synchronization lag. | Single-pass anti-join falsely deletes `ID 3000`; two-phase quarantine preserves it. |

## Benchmark columns

The full table in `BENCHMARK_TABLE.md` has these columns.

| Column | Meaning |
|---|---|
| Ghosts | Rows deleted at the source (IDs 420 and 9500) that are still in PostgreSQL. |
| Drift Del | ID 1010 was deleted because of the OFFSET page shift. |
| Backdated Del | ID 455 was deleted. It arrived mid-scan with an old source `created_at`. |
| Seq Race Del | ID 485 was deleted. Its sequence number was allocated before the mark and committed after. |
| Lag Del | ID 3000 was deleted. The simulated replica hid it for one sweep. |
| False Del | All valid rows missing from PostgreSQL at the end. |
| Net MRR Diff | `abs(destination total - source total)` for the tenant. Can be hidden by offsetting errors (e.g. an unpurged ghost offsetting a falsely deleted customer). |
| Gross MRR Diff | The sum of per-row absolute differences across all records. Mistakes cannot cancel out. |
| Body Bytes | Size of API response bodies. Not network traffic, because transport runs in-process. |
| Pre-Scan DB | Time for the advisory lock, the watermark and the sweep row. |
| Post-Scan DB | Time for `COPY`, `ANALYZE`, the safety check and the `DELETE`. |
| Total DB | Pre-scan plus post-scan. Median and P95 over 20 trials. With 20 samples, P95 is the second slowest run. |
| DEL Plan | Execution time of the `DELETE` alone, from `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)`. |
| DEL Plan Root Hits | Shared buffer hits on the root node of that plan. For strategies 0 and 3 it is `N/A` or tiny, because they do not run the same `DELETE`. |

## Known gaps & scope limits

- **Single-record failure injection:** Each failure mode uses one scripted record. A randomized workload would provide continuous statistical distributions rather than discrete pass/fail signals.
- **In-process HTTP transport:** The simulator runs in-process via ASGI (`httpx.ASGITransport`). Results reflect database locking, query planning, and pagination mechanics, but do not measure network bandwidth, packet jitter, or socket overhead.
- **Lock timeout accumulation:** `lock_timeout` limits each individual lock wait, not cumulative wait time across repeated acquisitions. In [`benchmarks/seal_timeout_repro.py`](../benchmarks/seal_timeout_repro.py), 8 concurrent writers caused a seal with `lock_timeout = 5s` to wait over 14 s across multiple brief acquisitions. Adding `SET LOCAL statement_timeout` strictly capped cumulative wait at the target boundary, raising `QueryCanceled`.
- **Index equivalence:** All watermarking columns (`source_created_at`, `ingestion_seq`, `ingestion_epoch`, `ingested_at`) have matching partial indexes (`WHERE is_deleted = FALSE`) on `destination_subscriptions` to ensure equivalent index-scan planning during anti-join deletion.
- **Safety guard scope:** The `max_delete_ratio` (0.20) guard prevents catastrophic empty-scan purges, but cannot detect small partial omission spikes below 20%.
- **Trigger enforcement assumption:** The epoch barrier guarantees consistency only when all writes pass through the trigger. Direct writes that bypass PostgreSQL triggers will not acquire epoch locks.
- **Crash recovery:** Tested by inserting a dangling `STAGED` sweep by hand, not by sending `SIGKILL` to an active process mid-transaction.
- **Environment:** Tested on Linux x86_64 with PostgreSQL 16 and Python 3.12 (CPython).
