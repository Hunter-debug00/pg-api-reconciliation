"""PostgreSQL API reconciliation benchmark harness.

Measures false deletion rates, ghost row retention, database execution time,
and root buffer hits across seven reconciliation strategies under deterministic
concurrent mutations.
"""

import asyncio
import json
import os
import platform
import random
import statistics
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import psycopg
from tabulate import tabulate

ROOT_DIR = Path(__file__).resolve().parent.parent
SCHEMA_PATH = ROOT_DIR / "sql" / "schema.sql"

if str(ROOT_DIR / "src") not in sys.path:
    sys.path.insert(0, str(ROOT_DIR / "src"))

from pg_reconcile import (  # noqa: E402
    SweepExecutionStats,
    assert_disposable_database,
    ingest_subscription_transactional,
    run_strategy_incremental,
    run_strategy_naive_offset,
    run_strategy_snapshot_cursor,
    run_strategy_staged_keyset,
    run_strategy_two_pass_quarantine,
)
from pg_reconcile.simulator import STATE, app  # noqa: E402

DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/reconcile_bench",
)

PAGE_SIZE = 100
TRIALS = int(os.getenv("BENCHMARK_TRIALS", "20"))
RANDOM_SEED = 42

BASE_TIME = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
SWEEP_START = BASE_TIME + timedelta(seconds=2)
MID_SWEEP_TIME = BASE_TIME + timedelta(seconds=15)
BACKDATED_SOURCE_TIME = BASE_TIME - timedelta(minutes=30)


@dataclass
class StrategyResultSummary:
    strategy_name: str
    page_size: int
    trials: int
    hard_delete_ghosts_remaining: int
    soft_delete_tombstones_missed: int
    false_deletions_boundary_shift: int
    false_deletions_mid_sweep_backdated: int
    false_deletions_uncommitted_seq: int
    false_deletions_transient_lag: int
    total_false_deletions: int
    globex_rows_wiped: int
    net_mrr_distortion_cents: int
    gross_mrr_mismatch_cents: int
    api_requests: int
    response_body_bytes: int
    median_pre_scan_db_ms: float
    median_post_scan_db_ms: float
    median_total_db_reconcile_ms: float
    p95_total_db_reconcile_ms: float
    median_delete_plan_ms: float
    delete_plan_root_shared_hits: int | None
    median_end_to_end_sweep_ms: float


def reset_environment(conn: psycopg.Connection) -> int:
    """Recreate tables and seed initial active subscriptions."""
    assert_disposable_database(conn)
    conn.rollback()
    with open(SCHEMA_PATH, encoding="utf-8") as f:
        schema_sql = f.read()

    with conn.cursor() as cur:
        cur.execute(schema_sql)

    STATE.seed_dataset(BASE_TIME)
    STATE.set_transient_omission(tenant_id="acme", record_id=3000, sweeps=1)

    with conn.cursor() as cur:
        # Disable trigger during initial bulk seed; re-enabled before any sweep or mutation runs.
        cur.execute(
            "ALTER TABLE destination_subscriptions DISABLE TRIGGER trg_enforce_tenant_epoch"
        )
        with cur.copy(
            """
            COPY destination_subscriptions
                (tenant_id, id, mrr_cents, is_deleted, source_created_at, updated_at,
                 ingested_at, ingestion_seq, ingestion_epoch, missing_streak)
            FROM STDIN
            """
        ) as copy:
            seq = 0
            for r in STATE.records.values():
                seq += 1
                copy.write_row(
                    (
                        r.tenant_id,
                        r.id,
                        r.mrr_cents,
                        r.is_deleted,
                        r.source_created_at,
                        r.updated_at,
                        BASE_TIME,
                        seq,
                        1,
                        0,
                    )
                )
        cur.execute("ALTER TABLE destination_subscriptions ENABLE TRIGGER trg_enforce_tenant_epoch")
        cur.execute("SELECT setval('dest_ingestion_seq', %s)", (seq,))
        cur.execute("SELECT nextval('dest_ingestion_seq')")
        preallocated_seq = int(cur.fetchone()[0])
    conn.commit()
    return preallocated_seq


def trigger_deterministic_mutations(page_num: int, conn: psycopg.Connection, preallocated_seq: int):
    """Inject concurrent mutations during sweep pagination of tenant acme."""
    if page_num == 1:
        STATE.hard_delete("acme", 420)
        STATE.hard_delete("acme", 9500)
        STATE.soft_delete("acme", 1500, MID_SWEEP_TIME)
    elif page_num == 2:
        rec_backdated = STATE.insert_new(
            tenant_id="acme",
            record_id=455,
            source_created_at=BACKDATED_SOURCE_TIME,
            updated_at=MID_SWEEP_TIME,
            mrr_cents=10000,
        )
        rec_late_commit = STATE.insert_new(
            tenant_id="acme",
            record_id=485,
            source_created_at=MID_SWEEP_TIME,
            updated_at=MID_SWEEP_TIME,
            mrr_cents=5000,
        )
        rec_ahead = STATE.insert_new(
            tenant_id="acme",
            record_id=25005,
            source_created_at=MID_SWEEP_TIME,
            updated_at=MID_SWEEP_TIME,
            mrr_cents=10000,
        )
        ingest_subscription_transactional(
            conn=conn,
            tenant_id=rec_backdated.tenant_id,
            record_id=rec_backdated.id,
            mrr_cents=rec_backdated.mrr_cents,
            is_deleted=False,
            source_created_at=rec_backdated.source_created_at,
            updated_at=rec_backdated.updated_at,
            ingested_at=MID_SWEEP_TIME,
        )
        ingest_subscription_transactional(
            conn=conn,
            tenant_id=rec_late_commit.tenant_id,
            record_id=rec_late_commit.id,
            mrr_cents=rec_late_commit.mrr_cents,
            is_deleted=False,
            source_created_at=rec_late_commit.source_created_at,
            updated_at=rec_late_commit.updated_at,
            ingested_at=MID_SWEEP_TIME,
            preallocated_seq=preallocated_seq,
        )
        ingest_subscription_transactional(
            conn=conn,
            tenant_id=rec_ahead.tenant_id,
            record_id=rec_ahead.id,
            mrr_cents=rec_ahead.mrr_cents,
            is_deleted=False,
            source_created_at=rec_ahead.source_created_at,
            updated_at=rec_ahead.updated_at,
            ingested_at=MID_SWEEP_TIME,
        )
        conn.commit()


def sync_soft_delete_tombstone(conn: psycopg.Connection):
    """Apply the soft-delete tombstone for record 1500 to PostgreSQL destination."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE destination_subscriptions
            SET is_deleted = TRUE, updated_at = %s
            WHERE tenant_id = 'acme' AND id = 1500
            """,
            (MID_SWEEP_TIME,),
        )
    conn.commit()


def evaluate_correctness(conn: psycopg.Connection) -> dict[str, int]:
    """Compare destination tables against upstream ground truth."""
    source_acme = {
        r.id: r for r in STATE.records.values() if r.tenant_id == "acme" and not r.is_deleted
    }
    source_globex = {
        r.id: r for r in STATE.records.values() if r.tenant_id == "globex" and not r.is_deleted
    }

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, mrr_cents
            FROM destination_subscriptions
            WHERE tenant_id = 'acme' AND is_deleted = FALSE
            """
        )
        dest_acme = {row[0]: row[1] for row in cur.fetchall()}

        cur.execute(
            """
            SELECT id
            FROM destination_subscriptions
            WHERE tenant_id = 'globex' AND is_deleted = FALSE
            """
        )
        dest_globex_ids = {row[0] for row in cur.fetchall()}

    source_acme_ids = set(source_acme.keys())
    dest_acme_ids = set(dest_acme.keys())

    ghosts = dest_acme_ids - source_acme_ids
    hard_delete_ghosts = {gid for gid in ghosts if gid in (420, 9500)}
    soft_delete_missed = 1 if 1500 in ghosts else 0

    false_deletions = source_acme_ids - dest_acme_ids
    globex_wiped = len(set(source_globex.keys()) - dest_globex_ids)

    true_source_mrr = sum(r.mrr_cents for r in source_acme.values())
    dest_mrr = sum(dest_acme.values())
    net_mrr_distortion = abs(dest_mrr - true_source_mrr)

    all_acme_ids = source_acme_ids | dest_acme_ids
    gross_mrr_mismatch = sum(
        abs((source_acme[rid].mrr_cents if rid in source_acme else 0) - dest_acme.get(rid, 0))
        for rid in all_acme_ids
    )

    return {
        "hard_delete_ghosts_remaining": len(hard_delete_ghosts),
        "soft_delete_tombstones_missed": soft_delete_missed,
        "false_deletions_boundary_shift": 1 if 1010 in false_deletions else 0,
        "false_deletions_mid_sweep_backdated": 1 if 455 in false_deletions else 0,
        "false_deletions_uncommitted_seq": 1 if 485 in false_deletions else 0,
        "false_deletions_transient_lag": 1 if 3000 in false_deletions else 0,
        "total_false_deletions": len(false_deletions),
        "globex_rows_wiped": globex_wiped,
        "net_mrr_distortion_cents": net_mrr_distortion,
        "gross_mrr_mismatch_cents": gross_mrr_mismatch,
    }


async def run_single_trial(
    strategy_key: str, conn: psycopg.Connection
) -> tuple[SweepExecutionStats, dict[str, int], float]:
    preallocated_seq = reset_environment(conn)
    transport = httpx.ASGITransport(app=app)
    t_start = time.perf_counter()

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        if strategy_key == "s0_incremental":
            trigger_deterministic_mutations(1, conn, preallocated_seq)
            trigger_deterministic_mutations(2, conn, preallocated_seq)
            stats = await run_strategy_incremental(
                client, conn, "acme", BASE_TIME, MID_SWEEP_TIME, page_size=PAGE_SIZE
            )

        elif strategy_key == "s1_naive_offset":
            sync_soft_delete_tombstone(conn)
            stats = await run_strategy_naive_offset(
                client,
                conn,
                "acme",
                page_size=PAGE_SIZE,
                on_page_fetched=lambda p: trigger_deterministic_mutations(
                    p, conn, preallocated_seq
                ),
                scope_to_tenant=True,
            )

        elif strategy_key == "s2a_keyset_source_created_hwm":
            sync_soft_delete_tombstone(conn)
            stats = await run_strategy_staged_keyset(
                client,
                conn,
                "acme",
                SWEEP_START,
                page_size=PAGE_SIZE,
                on_page_fetched=lambda p: trigger_deterministic_mutations(
                    p, conn, preallocated_seq
                ),
                hwm_mode="source_created_at",
            )

        elif strategy_key == "s2b_keyset_non_tx_seq_hwm":
            sync_soft_delete_tombstone(conn)
            stats = await run_strategy_staged_keyset(
                client,
                conn,
                "acme",
                SWEEP_START,
                page_size=PAGE_SIZE,
                on_page_fetched=lambda p: trigger_deterministic_mutations(
                    p, conn, preallocated_seq
                ),
                hwm_mode="ingestion_seq",
            )

        elif strategy_key == "s2c_keyset_sealed_epoch_hwm":
            sync_soft_delete_tombstone(conn)
            stats = await run_strategy_staged_keyset(
                client,
                conn,
                "acme",
                SWEEP_START,
                page_size=PAGE_SIZE,
                on_page_fetched=lambda p: trigger_deterministic_mutations(
                    p, conn, preallocated_seq
                ),
                hwm_mode="sealed_epoch",
            )

        elif strategy_key == "s3_two_pass_quarantine":
            sync_soft_delete_tombstone(conn)
            stats = await run_strategy_two_pass_quarantine(
                client,
                conn,
                "acme",
                SWEEP_START,
                page_size=PAGE_SIZE,
                on_page_fetched_sweep_1=lambda p: trigger_deterministic_mutations(
                    p, conn, preallocated_seq
                ),
                after_sweep_1_hook=lambda: STATE.decrement_transient_omissions("acme"),
            )

        elif strategy_key == "s4_snapshot_cursor":
            sync_soft_delete_tombstone(conn)
            stats = await run_strategy_snapshot_cursor(
                client,
                conn,
                "acme",
                SWEEP_START,
                page_size=PAGE_SIZE,
                on_page_fetched=lambda p: trigger_deterministic_mutations(
                    p, conn, preallocated_seq
                ),
            )
        else:
            raise ValueError(f"Unknown strategy {strategy_key}")

    elapsed_ms = (time.perf_counter() - t_start) * 1000.0
    correctness = evaluate_correctness(conn)
    return stats, correctness, elapsed_ms


def _p95(values: list[float]) -> float:
    if len(values) < 2:
        return values[0]
    sorted_vals = sorted(values)
    idx = min(int(round(0.95 * (len(sorted_vals) - 1))), len(sorted_vals) - 1)
    return sorted_vals[idx]


async def benchmark_all(
    conn: psycopg.Connection, trials: int = TRIALS
) -> list[StrategyResultSummary]:
    strategies = [
        ("s0_incremental", "0. Incremental tombstone sync"),
        ("s1_naive_offset", "1. Naive OFFSET anti-join"),
        ("s2a_keyset_source_created_hwm", "2a. Keyset with source timestamp watermark"),
        ("s2b_keyset_non_tx_seq_hwm", "2b. Keyset with sequence watermark"),
        ("s2c_keyset_sealed_epoch_hwm", "2c. Transactional epoch barrier"),
        ("s3_two_pass_quarantine", "3. Two-phase quarantine (streak >= 2)"),
        ("s4_snapshot_cursor", "4. Consistent snapshot pagination"),
    ]

    for key, _ in strategies:
        await run_single_trial(key, conn)

    rng = random.Random(RANDOM_SEED)
    schedule: list[str] = []
    for _ in range(trials):
        keys = [k for k, _ in strategies]
        rng.shuffle(keys)
        schedule.extend(keys)

    collected_pre_db: dict[str, list[float]] = {k: [] for k, _ in strategies}
    collected_post_db: dict[str, list[float]] = {k: [] for k, _ in strategies}
    collected_db_times: dict[str, list[float]] = {k: [] for k, _ in strategies}
    collected_plan_times: dict[str, list[float]] = {k: [] for k, _ in strategies}
    collected_sweep_times: dict[str, list[float]] = {k: [] for k, _ in strategies}
    collected_buffers: dict[str, list[int]] = {k: [] for k, _ in strategies}
    collected_corrs: dict[str, list[dict[str, int]]] = {k: [] for k, _ in strategies}
    last_stats_map: dict[str, SweepExecutionStats] = {}

    for key in schedule:
        stats, corr, elapsed = await run_single_trial(key, conn)
        collected_pre_db[key].append(stats.pre_scan_db_ms)
        collected_post_db[key].append(stats.post_scan_db_ms)
        collected_db_times[key].append(stats.total_db_reconcile_ms)
        collected_plan_times[key].append(stats.delete_query_plan_ms)
        collected_sweep_times[key].append(elapsed)
        if stats.delete_plan_root_shared_hits is not None:
            collected_buffers[key].append(stats.delete_plan_root_shared_hits)
        collected_corrs[key].append(corr)
        last_stats_map[key] = stats

    summaries: list[StrategyResultSummary] = []
    for key, label in strategies:
        first_corr = collected_corrs[key][0]
        assert all(
            c == first_corr for c in collected_corrs[key]
        ), f"Non-deterministic correctness result across trials for {key}"

        last_stats = last_stats_map[key]
        med_buffers = (
            int(round(statistics.median(collected_buffers[key])))
            if collected_buffers[key]
            else None
        )

        summaries.append(
            StrategyResultSummary(
                strategy_name=label,
                page_size=PAGE_SIZE,
                trials=trials,
                hard_delete_ghosts_remaining=first_corr["hard_delete_ghosts_remaining"],
                soft_delete_tombstones_missed=first_corr["soft_delete_tombstones_missed"],
                false_deletions_boundary_shift=first_corr["false_deletions_boundary_shift"],
                false_deletions_mid_sweep_backdated=first_corr[
                    "false_deletions_mid_sweep_backdated"
                ],
                false_deletions_uncommitted_seq=first_corr["false_deletions_uncommitted_seq"],
                false_deletions_transient_lag=first_corr["false_deletions_transient_lag"],
                total_false_deletions=first_corr["total_false_deletions"],
                globex_rows_wiped=first_corr["globex_rows_wiped"],
                net_mrr_distortion_cents=first_corr["net_mrr_distortion_cents"],
                gross_mrr_mismatch_cents=first_corr["gross_mrr_mismatch_cents"],
                api_requests=last_stats.api_requests,
                response_body_bytes=last_stats.response_body_bytes,
                median_pre_scan_db_ms=round(statistics.median(collected_pre_db[key]), 3),
                median_post_scan_db_ms=round(statistics.median(collected_post_db[key]), 3),
                median_total_db_reconcile_ms=round(statistics.median(collected_db_times[key]), 3),
                p95_total_db_reconcile_ms=round(_p95(collected_db_times[key]), 3),
                median_delete_plan_ms=round(statistics.median(collected_plan_times[key]), 3),
                delete_plan_root_shared_hits=med_buffers,
                median_end_to_end_sweep_ms=round(statistics.median(collected_sweep_times[key]), 2),
            )
        )

    raw_trials: dict[str, dict] = {}
    for key, label in strategies:
        raw_trials[label] = {
            "pre_scan_db_ms": collected_pre_db[key],
            "post_scan_db_ms": collected_post_db[key],
            "total_db_reconcile_ms": collected_db_times[key],
            "delete_query_plan_ms": collected_plan_times[key],
            "delete_plan_shared_hits": collected_buffers[key],
            "end_to_end_sweep_ms": collected_sweep_times[key],
        }

    return summaries, raw_trials


def get_environment_metadata(conn: psycopg.Connection) -> dict:
    with conn.cursor() as cur:
        cur.execute("SHOW server_version")
        pg_version = cur.fetchone()[0]
    try:
        git_sha = subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip()[:10]
    except Exception:
        git_sha = "unknown"

    return {
        "git_commit": git_sha,
        "python_version": platform.python_version(),
        "postgres_version": pg_version,
        "platform": platform.platform(),
        "page_size": PAGE_SIZE,
        "trials_per_strategy": TRIALS,
        "random_seed": RANDOM_SEED,
        "timestamp_utc": datetime.now(UTC).isoformat(),
    }


def format_benchmark_tables(summaries: list[StrategyResultSummary]) -> str:
    """Formats benchmark results into two focused tables (Correctness vs. Cost) to avoid horizontal scroll."""
    correctness_rows = []
    performance_rows = []

    for s in summaries:
        correctness_rows.append(
            [
                s.strategy_name,
                s.hard_delete_ghosts_remaining,
                s.false_deletions_boundary_shift,
                s.false_deletions_mid_sweep_backdated,
                s.false_deletions_uncommitted_seq,
                s.false_deletions_transient_lag,
                s.total_false_deletions,
                f"${s.net_mrr_distortion_cents / 100:,.2f}",
                f"${s.gross_mrr_mismatch_cents / 100:,.2f}",
            ]
        )

        buf_str = (
            f"{s.delete_plan_root_shared_hits:,}"
            if s.delete_plan_root_shared_hits is not None
            else "N/A"
        )
        performance_rows.append(
            [
                s.strategy_name,
                s.api_requests,
                f"{s.response_body_bytes / 1024:.1f} KB",
                f"{s.median_pre_scan_db_ms:.2f} ms",
                f"{s.median_post_scan_db_ms:.2f} ms",
                f"{s.median_total_db_reconcile_ms:.2f} ms",
                f"{s.p95_total_db_reconcile_ms:.2f} ms",
                f"{s.median_delete_plan_ms:.2f} ms",
                buf_str,
            ]
        )

    corr_headers = [
        "Strategy",
        "Ghosts",
        "Drift",
        "Backdated",
        "Seq Race",
        "Replica Lag",
        "Total False Del",
        "Net MRR Diff",
        "Gross MRR Diff",
    ]
    perf_headers = [
        "Strategy",
        "API Reqs",
        "Payload",
        "Pre-Scan DB",
        "Post-Scan DB",
        "Total DB (Med)",
        "Total DB (P95)",
        "DELETE Plan",
        "Shared Hits",
    ]

    t1 = tabulate(correctness_rows, headers=corr_headers, tablefmt="github")
    t2 = tabulate(performance_rows, headers=perf_headers, tablefmt="github")

    return (
        "#### 1. Correctness & Revenue Distortion\n\n"
        f"{t1}\n\n"
        "#### 2. Database & API Execution Cost\n\n"
        f"{t2}"
    )


def build_readme_tables(summaries: list[StrategyResultSummary]) -> str:
    """Two compact tables for the README. The full 17-column table stays in BENCHMARK_TABLE.md."""
    verdict = lambda n: "DELETED" if n else "ok"  # noqa: E731
    matrix = [
        [
            s.strategy_name,
            verdict(s.false_deletions_boundary_shift),
            verdict(s.false_deletions_mid_sweep_backdated),
            verdict(s.false_deletions_uncommitted_seq),
            verdict(s.false_deletions_transient_lag),
        ]
        for s in summaries
    ]
    numbers = [
        [
            s.strategy_name,
            s.hard_delete_ghosts_remaining,
            s.total_false_deletions,
            f"${s.gross_mrr_mismatch_cents / 100:,.2f}",
            s.api_requests,
            f"{s.median_total_db_reconcile_ms:.1f} ms",
        ]
        for s in summaries
    ]
    matrix_md = tabulate(
        matrix,
        headers=["Strategy", "Page drift", "Backdated insert", "Uncommitted write", "Replica lag"],
        tablefmt="github",
    )
    numbers_md = tabulate(
        numbers,
        headers=[
            "Strategy",
            "Ghost rows left",
            "Valid rows deleted",
            "Gross MRR error",
            "API calls",
            "Median DB time",
        ],
        tablefmt="github",
    )
    return (
        "Which scripted failure deletes a valid row?\n\n"
        f"{matrix_md}\n\n"
        "What each strategy leaves behind, and what it costs:\n\n"
        f"{numbers_md}"
    )


def update_readme_table(md_table: str, env_meta: dict):
    """Update only the bounded table section inside README.md without modifying prose."""
    marker_start = "<!-- BENCHMARK_TABLE_START -->"
    marker_end = "<!-- BENCHMARK_TABLE_END -->"

    metadata_line = (
        f"*Benchmark metadata (Git `{env_meta['git_commit']}`, Python `{env_meta['python_version']}`, "
        f"PostgreSQL `{env_meta['postgres_version']}`, Seed `{env_meta['random_seed']}`):*"
    )
    replacement_block = f"{marker_start}\n{metadata_line}\n\n{md_table}\n{marker_end}"

    readme_path = ROOT_DIR / "README.md"
    if readme_path.exists():
        with open(readme_path, encoding="utf-8") as f:
            content = f.read()

        if marker_start in content and marker_end in content:
            pre = content.split(marker_start)[0]
            post = content.split(marker_end)[1]
            new_content = f"{pre}{replacement_block}{post}"
            with open(readme_path, "w", encoding="utf-8") as f:
                f.write(new_content)


async def main():
    print()
    print(
        f"Running reconciliation benchmark across 10,000 subscription records "
        f"({TRIALS} repeated timing trials with a seeded, rotated strategy execution order, page_size={PAGE_SIZE})..."
    )
    with psycopg.connect(DB_URL) as conn:
        env_meta = get_environment_metadata(conn)
        summaries, raw_trials = await benchmark_all(conn, trials=TRIALS)

    table_rows = []
    for s in summaries:
        buf_str = (
            str(s.delete_plan_root_shared_hits)
            if s.delete_plan_root_shared_hits is not None
            else "N/A"
        )
        table_rows.append(
            [
                s.strategy_name,
                s.hard_delete_ghosts_remaining,
                s.false_deletions_boundary_shift,
                s.false_deletions_mid_sweep_backdated,
                s.false_deletions_uncommitted_seq,
                s.false_deletions_transient_lag,
                s.total_false_deletions,
                f"${s.net_mrr_distortion_cents / 100:,.2f}",
                f"${s.gross_mrr_mismatch_cents / 100:,.2f}",
                s.api_requests,
                f"{s.response_body_bytes / 1024:.1f} KB",
                f"{s.median_pre_scan_db_ms:.2f} ms",
                f"{s.median_post_scan_db_ms:.2f} ms",
                f"{s.median_total_db_reconcile_ms:.2f} ms",
                f"{s.p95_total_db_reconcile_ms:.2f} ms",
                f"{s.median_delete_plan_ms:.2f} ms",
                buf_str,
            ]
        )

    headers = [
        "Strategy",
        "Ghosts",
        "Drift Del",
        "Backdated Del",
        "Seq Race Del",
        "Lag Del",
        "False Del",
        "Net MRR Diff",
        "Gross MRR Diff",
        "API Reqs",
        "Body Bytes",
        "Pre-Scan DB",
        "Post-Scan DB",
        "Total DB (Med)",
        "Total DB (P95)",
        "DEL Plan (Med)",
        "DEL Plan Root Hits",
    ]

    md_table = tabulate(table_rows, headers=headers, tablefmt="github")
    split_tables = format_benchmark_tables(summaries)
    print("\n=== REPRODUCIBLE BENCHMARK RESULTS ===")
    print(split_tables)

    payload = {
        "environment": env_meta,
        "results": [asdict(s) for s in summaries],
        "raw_trial_observations": raw_trials,
        "markdown_table": md_table,
        "split_markdown_tables": split_tables,
    }

    results_path = ROOT_DIR / "benchmark_results.json"
    with open(results_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    table_path = ROOT_DIR / "BENCHMARK_TABLE.md"
    with open(table_path, "w", encoding="utf-8") as f:
        f.write(
            "# Reproducible Benchmark Results\n\n"
            f"{split_tables}\n\n"
            "---\n\n"
            "### Consolidated 17-Column Metric View\n\n"
            f"{md_table}\n"
        )

    update_readme_table(build_readme_tables(summaries), env_meta)
    print("\nSynchronized benchmark_results.json, BENCHMARK_TABLE.md, and README.md")


if __name__ == "__main__":
    asyncio.run(main())
