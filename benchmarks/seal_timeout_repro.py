"""Does lock_timeout alone bound how long the epoch seal can wait?

Run from the repo root, against a THROWAWAY database (it executes sql/schema.sql, which drops tables):
    DATABASE_URL=postgresql://postgres:postgres@localhost:5432/reconcile_bench uv run python benchmarks/seal_timeout_repro.py
8 writers commit back to back on tenant acme while the seal UPDATE is attempted 4 times per mode.
"""

import os
import sys
import threading
import time
from pathlib import Path

import psycopg

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR / "src") not in sys.path:
    sys.path.insert(0, str(ROOT_DIR / "src"))

from pg_reconcile import assert_disposable_database  # noqa: E402

DB = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/reconcile_bench")
SCHEMA = Path("sql/schema.sql").read_text()
UPSERT = """INSERT INTO destination_subscriptions (tenant_id, id, mrr_cents, is_deleted, source_created_at, updated_at)
VALUES ('acme', %s, 5000, FALSE, now(), now())
ON CONFLICT (tenant_id, id) DO UPDATE SET mrr_cents = EXCLUDED.mrr_cents, updated_at = now()"""


def writer(wid: int, stop: threading.Event) -> None:
    n = 0
    with psycopg.connect(DB) as c:
        while not stop.is_set():
            with c.cursor() as cur:
                cur.execute(UPSERT, (wid * 1_000_000 + (n % 2000),))
                time.sleep(0.005)
            c.commit()
            n += 1


def main() -> None:
    with psycopg.connect(DB, autocommit=True) as c:
        assert_disposable_database(c)
        c.execute(SCHEMA)

    stop = threading.Event()
    ts = [threading.Thread(target=writer, args=(i + 1, stop)) for i in range(8)]
    for t in ts:
        t.start()
    time.sleep(1.0)

    for mode in ("lock_timeout=5s only", "lock_timeout=5s + statement_timeout=3s"):
        out: list[str] = []
        with psycopg.connect(DB) as sc:
            for _ in range(4):
                t0 = time.perf_counter()
                try:
                    with sc.cursor() as cur:
                        cur.execute("SELECT set_config('lock_timeout', '5s', true)")
                        if "statement" in mode:
                            cur.execute("SELECT set_config('statement_timeout', '3s', true)")
                        cur.execute(
                            "UPDATE tenant_watermarks SET current_epoch = current_epoch + 1 WHERE tenant_id = 'acme'"
                        )
                    sc.commit()
                    out.append(f"ok {(time.perf_counter() - t0) * 1000:.0f}ms")
                except (psycopg.errors.QueryCanceled, psycopg.errors.LockNotAvailable) as e:
                    sc.rollback()
                    out.append(f"{type(e).__name__} {(time.perf_counter() - t0) * 1000:.0f}ms")
                time.sleep(0.3)
        print(f"{mode:42s} -> {out}", flush=True)

    stop.set()
    for t in ts:
        t.join()


if __name__ == "__main__":
    main()
