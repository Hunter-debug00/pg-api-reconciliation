# Can You Safely Delete Rows from a Mutable API?

Seven PostgreSQL reconciliation strategies tested against a changing paginated source.

[![CI](https://github.com/Hunter-debug00/pg-api-reconciliation/actions/workflows/ci.yml/badge.svg)](https://github.com/Hunter-debug00/pg-api-reconciliation/actions/workflows/ci.yml)
![Python 3.12](https://img.shields.io/badge/python-3.12-blue)
![PostgreSQL 16](https://img.shields.io/badge/postgresql-16-blue)
![License MIT](https://img.shields.io/badge/license-MIT-green)

You sync subscriptions from a SaaS API into PostgreSQL. A customer is hard-deleted upstream, and your copy keeps them forever. So you scan the API, collect every ID you see, and delete the rest.

Then the scan misses an active customer, and you delete them too.

This repo reproduces that failure on purpose and compares seven ways of deciding a row is really gone. A FastAPI simulator plays the upstream API, PostgreSQL 16 holds the copy, and the data is 10,000 subscriptions across two tenants. Mutations hit the source while the scan is running, the same way every time. 32 tests, 20 repeated timing trials per strategy.

## The problem

Incremental sync (`GET /subscriptions?updated_after=...`) catches new rows, edits and soft deletes. It cannot catch a hard delete, because a deleted row has no `updated_at` to report. The row stays in PostgreSQL as a ghost and keeps counting toward your MRR.

The usual fix is a full sweep:

```sql
DELETE FROM destination_subscriptions
WHERE tenant_id = 'acme'
  AND id NOT IN (/* every ID the scan returned */);
```

That statement trusts the scan completely. A scan over a source that is changing under it can be wrong in four ways, and each one is a test in this repo.

## How a scan goes wrong

OFFSET pagination is the clearest case. IDs here are 10, 20, 30 and so on, 100 per page.

```text
Page 1 (OFFSET 0, LIMIT 100) returns IDs 10 ... 1000.
ID 420 is hard-deleted at the source.
Every later row moves up one slot, so ID 1010 now sits at index 99.
Page 2 (OFFSET 100) starts at ID 1020.

ID 1010 was never returned. The DELETE removes it. It is still an active customer.
```

The other three:

- Backdated insert. A new customer arrives mid-scan with an old `created_at`, so a `created_at < sweep_start` guard treats it as old enough to delete.
- Uncommitted write. A writer takes a sequence number before the scan starts and commits after it. A guard of `ingestion_seq <= mark` then covers a row the scan could not have seen.
- Replica lag. The source read replica leaves out one valid row for a single sweep.

## Results

<!-- BENCHMARK_TABLE_START -->
*Benchmark metadata (Git `0791437880`, Python `3.12.11`, PostgreSQL `16.15`, Seed `42`):*

Which scripted failure deletes a valid row?

| Strategy                                   | Page drift   | Backdated insert   | Uncommitted write   | Replica lag   |
|--------------------------------------------|--------------|--------------------|---------------------|---------------|
| 0. Incremental tombstone sync              | ok           | ok                 | ok                  | ok            |
| 1. Naive OFFSET anti-join                  | DELETED      | DELETED            | DELETED             | DELETED       |
| 2a. Keyset with source timestamp watermark | ok           | DELETED            | ok                  | DELETED       |
| 2b. Keyset with sequence watermark         | ok           | ok                 | DELETED             | DELETED       |
| 2c. Transactional epoch barrier            | ok           | ok                 | ok                  | DELETED       |
| 3. Two-phase quarantine (streak >= 2)      | ok           | ok                 | ok                  | ok            |
| 4. Consistent snapshot pagination          | ok           | ok                 | ok                  | DELETED       |

What each strategy leaves behind, and what it costs:

| Strategy                                   |   Ghost rows left |   Valid rows deleted | Gross MRR error   |   API calls | Median DB time   |
|--------------------------------------------|-------------------|----------------------|-------------------|-------------|------------------|
| 0. Incremental tombstone sync              |                 2 |                    0 | $100.00           |           1 | 2.9 ms           |
| 1. Naive OFFSET anti-join                  |                 1 |                    4 | $300.00           |          50 | 69.2 ms          |
| 2a. Keyset with source timestamp watermark |                 1 |                    2 | $200.00           |          50 | 55.5 ms          |
| 2b. Keyset with sequence watermark         |                 1 |                    2 | $150.00           |          50 | 54.8 ms          |
| 2c. Transactional epoch barrier            |                 1 |                    1 | $100.00           |          50 | 56.2 ms          |
| 3. Two-phase quarantine (streak >= 2)      |                 1 |                    0 | $50.00            |         100 | 110.7 ms         |
| 4. Consistent snapshot pagination          |                 2 |                    1 | $150.00           |          51 | 58.4 ms          |
<!-- BENCHMARK_TABLE_END -->

"ok" means the scripted record survived; "DELETED" means an active customer was falsely purged. There is one record per failure, so read these as pass or fail, not as rates. Gross MRR error adds up the per-row differences, so a ghost row and a wrongly deleted row cannot cancel out. The full 17-column table is in [`BENCHMARK_TABLE.md`](BENCHMARK_TABLE.md) and individual trial timings are in [`benchmark_results.json`](benchmark_results.json).

## What the results show

### Totals hide damage

Strategy 2c leaves one ghost row (+$50) and wrongly deletes one customer (-$50). The net MRR error is $0.00. The gross error is $100.00. A dashboard built on the total would look perfect.

### A sequence number is not a commit order

Strategy 2b reads `last_value` from a sequence before the scan and deletes rows at or below it. `nextval()` is not transactional, so a writer can hold a number below the mark and commit after the scan passed it. Strategy 2c fixes this with an epoch barrier: every write takes a `FOR SHARE` lock on its tenant's watermark row (a trigger does it in [sql/schema.sql](sql/schema.sql#L62-L80), so application code cannot forget), and the sweep starts by bumping the epoch, which waits for open writes to commit. The DELETE only touches rows from sealed epochs. [How it works](docs/DESIGN.md#the-epoch-barrier).

### Quarantine helps with replica lag, and proves nothing

Strategy 3 deletes a row only after it is missing from two sweeps in a row. That clears a transient single-sweep replica omission and costs twice the API calls. A row that stays hidden for two sweeps is still deleted, and `test_two_pass_quarantine_purges_after_consecutive_omissions` shows it.

### A bad scan should delete nothing

The sweep aborts, and deletes nothing, on an HTTP 500 mid-scan, an empty page that claims more pages exist, a cursor that does not match the last ID, or IDs out of order. It also refuses to run if the scan returns zero IDs or would delete more than 20% of a tenant's rows (configurable with `max_delete_ratio`).

## Run it

```bash
docker compose -f .devcontainer/docker-compose.yml up -d db   # PostgreSQL 16
uv sync --locked
uv run --locked pytest -v                                      # about 15 s
uv run --locked python benchmarks/run_benchmark.py             # about 1 min
docker compose -f .devcontainer/docker-compose.yml down -v
```

In a GitHub Codespace the dev container starts the database for you, so skip the first and last lines.

**`sql/schema.sql` drops and recreates its tables.** Point `DATABASE_URL` at a throwaway database only. The benchmark also rewrites the results table in this README, `benchmark_results.json` and `BENCHMARK_TABLE.md`, so expect `git status` to show changes after a run.

## What this does not prove

- The simulator runs in the same process as the client, through httpx's ASGI transport. There is no network and no load.
- Each failure is one scripted record. All 20 trials run the same mutations, so they measure timing noise. Correctness never changes between trials, and the harness asserts that.
- The timings include implementation choices (row-by-row `COPY` from Python, an `ANALYZE`, the safety-check counts), not only the algorithm. Strategy 3 runs both of its sweeps back to back, so the real delay of waiting for a second sweep is not measured. Don't read the table as a ranking of algorithms.
- The data is 10,000 rows. Nothing here says how it behaves at a million.
- The epoch barrier protects against writers that go through the destination table. It cannot protect against a source that lies. It also makes each sweep wait for ingestion transactions still open on that tenant, and under saturating write load that wait can get long (see Known gaps in [`docs/DESIGN.md`](docs/DESIGN.md#known-gaps)).

## Layout

```text
src/pg_reconcile/             engine.py (locks, epochs, staging, guards), strategies.py, simulator.py
sql/schema.sql                tables, trigger, indexes
tests/                        concurrency, safety and strategy tests
benchmarks/                   run_benchmark.py, seal_timeout_repro.py
docs/DESIGN.md                how the sweep, the barrier, the guards, and failure modes work
```

MIT licensed.
