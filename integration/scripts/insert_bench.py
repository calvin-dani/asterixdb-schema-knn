#!/usr/bin/env python3
"""
End-to-end insert-after-index-creation latency benchmark for AsterixDB vector indexes.

Full sequence:
  1. Create dataverse with Staging + Target datasets
  2. Load local JSON file into Staging
  3. Seed Target with initial records
  4. Create vector index on Target
  5. Benchmark INSERT latency (remaining records, batched)
  6. Report p50/p95/p99 latency statistics

Usage:
    # Run with defaults (seed 7000, insert remaining 3000 in batches of 100)
    python insert_bench.py --file ../datasets/movie_embeddings_384d.json

    # Custom split and batch size
    python insert_bench.py --file ../datasets/movie_embeddings_384d.json \\
        --seed 5000 --batch-size 200

    # Custom index parameters
    python insert_bench.py --file ../datasets/movie_embeddings_384d.json \\
        --num-clusters 80 --train-list 5000 --similarity euclidean_squared

    # Skip setup (reuse existing dataverse, only run benchmark)
    python insert_bench.py --skip-setup --dataverse BENCH \\
        --source Staging --target Target \\
        --total 3000 --batch-size 100 --offset 7000
"""

import os
import sys
import time
import argparse
import statistics
import requests

ASTERIX_URL = "http://localhost:29002/query/service"
HEADERS = {"Content-Type": "application/x-www-form-urlencoded"}


# ---------------------------------------------------------------------------
# AsterixDB helpers
# ---------------------------------------------------------------------------

def execute_statement(statement, context_id="default", timeout=600):
    """Execute a SQL++ statement and return parsed JSON response."""
    data = {
        "statement": statement,
        "pretty": "true",
        "client_context_id": context_id,
    }
    resp = requests.post(ASTERIX_URL, headers=HEADERS, data=data, timeout=timeout)
    resp.raise_for_status()
    result = resp.json()
    status = result.get("status", "")
    if status not in ("success", "running"):
        errors = result.get("errors", [])
        raise RuntimeError(f"Statement failed ({status}): {errors}")
    return result


def count_records(dataverse, dataset):
    """Return record count for a dataset."""
    stmt = f"USE {dataverse}; SELECT VALUE COUNT(*) FROM {dataset};"
    result = execute_statement(stmt, context_id=f"count_{dataset}")
    return result["results"][0]


# ---------------------------------------------------------------------------
# Setup phase
# ---------------------------------------------------------------------------

def run_setup(args):
    """Create dataverse, load data, seed target, create index."""
    dataverse = args.dataverse
    file_path = os.path.abspath(args.file).replace("\\", "/")
    localfs_path = f"localhost://{file_path}"

    # Count total records in file
    with open(args.file, "r") as f:
        total_lines = sum(1 for _ in f)
    print(f"[info] Dataset file: {file_path}")
    print(f"[info] Total records in file: {total_lines}")

    seed = args.seed
    if seed >= total_lines:
        print(f"[error] --seed ({seed}) must be less than total records ({total_lines})")
        sys.exit(1)
    insert_total = total_lines - seed
    print(f"[info] Seed target with: {seed} records")
    print(f"[info] Benchmark inserts: {insert_total} records")
    print()

    # Step 1: Create dataverse and datasets
    print("=" * 60)
    print("STEP 1: Create dataverse and datasets")
    print("=" * 60)
    stmt = f"""
    DROP DATAVERSE {dataverse} IF EXISTS;
    CREATE DATAVERSE {dataverse};
    USE {dataverse};

    CREATE TYPE MovieType AS {{ idx: int }};

    CREATE DATASET Staging(MovieType) PRIMARY KEY idx
      WITH {{"storage-format":{{"format":"column"}}}};

    CREATE DATASET Target(MovieType) PRIMARY KEY idx
      WITH {{"storage-format":{{"format":"column"}}}};
    """
    t0 = time.time()
    execute_statement(stmt, "setup_dataverse")
    print(f"[done] Dataverse created in {time.time() - t0:.1f}s\n")

    # Step 2: Load file into Staging
    print("=" * 60)
    print("STEP 2: Load local file into Staging")
    print("=" * 60)
    print(f"[info] localfs path: {localfs_path}")
    stmt = f"""
    USE {dataverse};
    LOAD DATASET Staging USING localfs (
      ("path" = "{localfs_path}"),
      ("format" = "json")
    );
    """
    t0 = time.time()
    execute_statement(stmt, "load_staging", timeout=600)
    elapsed = time.time() - t0
    staging_count = count_records(dataverse, "Staging")
    print(f"[done] Loaded {staging_count} records in {elapsed:.1f}s\n")

    # Step 3: Seed Target
    print("=" * 60)
    print(f"STEP 3: Seed Target with first {seed} records")
    print("=" * 60)
    stmt = f"""
    USE {dataverse};
    INSERT INTO Target (
      SELECT VALUE s FROM Staging s ORDER BY s.idx LIMIT {seed} OFFSET 0
    );
    """
    t0 = time.time()
    execute_statement(stmt, "seed_target", timeout=600)
    elapsed = time.time() - t0
    target_count = count_records(dataverse, "Target")
    print(f"[done] Seeded {target_count} records in {elapsed:.1f}s\n")

    # Step 4: Create vector index
    print("=" * 60)
    print("STEP 4: Create vector index on Target")
    print("=" * 60)
    print(f"[info] num_clusters={args.num_clusters}, "
          f"train_list={args.train_list}, "
          f"similarity={args.similarity}, "
          f"dimension={args.dimension}")

    stmt = f"""
    USE {dataverse};
    CREATE VECTOR INDEX ix_bench ON Target(embedding VECTOR) WITH {{
      "dimension": {args.dimension},
      "num_clusters": {args.num_clusters},
      "similarity": "{args.similarity}",
      "train_list_number": {args.train_list}
    }};
    """
    t0 = time.time()
    execute_statement(stmt, "create_index", timeout=600)
    elapsed = time.time() - t0
    print(f"[done] Vector index created in {elapsed:.1f}s\n")

    return insert_total, seed


# ---------------------------------------------------------------------------
# Benchmark phase
# ---------------------------------------------------------------------------

def insert_batch(dataverse, source, target, offset, limit):
    """Insert a batch via INSERT INTO ... SELECT. Returns elapsed seconds."""
    stmt = f"""USE {dataverse};
INSERT INTO {target} (
    SELECT VALUE s
    FROM {source} s
    ORDER BY s.idx
    LIMIT {limit}
    OFFSET {offset}
);"""
    t0 = time.time()
    execute_statement(stmt, context_id=f"insert_{offset}_{limit}", timeout=600)
    return time.time() - t0


def percentile(data, p):
    """Compute the p-th percentile of a sorted list."""
    if not data:
        return 0.0
    k = (len(data) - 1) * (p / 100.0)
    f = int(k)
    c = f + 1
    if c >= len(data):
        return data[f]
    return data[f] + (k - f) * (data[c] - data[f])


def run_benchmark(args, total, offset):
    """Run batched inserts and collect latency stats."""
    dataverse = args.dataverse
    source = args.source
    target = args.target
    batch_size = args.batch_size

    num_batches = (total + batch_size - 1) // batch_size

    print("=" * 60)
    print("STEP 5: Benchmark INSERT latency")
    print("=" * 60)
    print(f"  Source    : {dataverse}.{source}")
    print(f"  Target    : {dataverse}.{target}")
    print(f"  Total     : {total} records")
    print(f"  Batch size: {batch_size}")
    print(f"  Batches   : {num_batches}")
    print(f"  Offset    : {offset}")
    print()

    latencies = []
    for i in range(num_batches):
        batch_offset = offset + i * batch_size
        batch_limit = min(batch_size, total - i * batch_size)

        elapsed = insert_batch(dataverse, source, target, batch_offset, batch_limit)
        latencies.append(elapsed)

        print(f"  Batch {i + 1:3d}/{num_batches}: "
              f"offset={batch_offset:6d}, count={batch_limit:4d}, "
              f"latency={elapsed:.3f}s")

    # Final count
    try:
        final_count = count_records(dataverse, target)
        print(f"\n  Target count after inserts: {final_count}")
    except Exception as e:
        print(f"\n  Could not get final count: {e}")

    # Summary
    if not latencies:
        print("\nNo latency data collected.")
        return

    sorted_lat = sorted(latencies)
    total_time = sum(latencies)
    total_records = sum(min(batch_size, total - i * batch_size) for i in range(num_batches))

    print()
    print("=" * 60)
    print("INSERT LATENCY SUMMARY")
    print("=" * 60)
    print(f"  Batches completed : {len(latencies)}")
    print(f"  Batch size        : {batch_size} records")
    print(f"  Total records     : {total_records}")
    print(f"  Total time        : {total_time:.2f}s")
    print(f"  Throughput        : {total_records / total_time:.1f} records/s")
    print()
    print("  Per-batch latency (seconds):")
    print(f"    min  : {sorted_lat[0]:.3f}")
    print(f"    p50  : {percentile(sorted_lat, 50):.3f}")
    print(f"    p95  : {percentile(sorted_lat, 95):.3f}")
    print(f"    p99  : {percentile(sorted_lat, 99):.3f}")
    print(f"    max  : {sorted_lat[-1]:.3f}")
    print(f"    mean : {statistics.mean(latencies):.3f}")
    if len(latencies) > 1:
        print(f"    stdev: {statistics.stdev(latencies):.3f}")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="End-to-end insert-after-index-creation latency benchmark"
    )

    # Setup args
    parser.add_argument("--file",
                        help="Path to local JSON dataset file (required unless --skip-setup)")
    parser.add_argument("--dataverse", default="BENCH",
                        help="Dataverse name (default: BENCH)")
    parser.add_argument("--seed", type=int, default=7000,
                        help="Records to seed before index creation (default: 7000)")
    parser.add_argument("--dimension", type=int, default=384,
                        help="Vector dimension (default: 384)")
    parser.add_argument("--num-clusters", type=int, default=50,
                        help="Number of clusters for vector index (default: 50)")
    parser.add_argument("--train-list", type=int, default=5000,
                        help="Training list size (default: 5000)")
    parser.add_argument("--similarity", default="euclidean_squared",
                        help="Distance metric (default: euclidean_squared)")

    # Benchmark args
    parser.add_argument("--batch-size", type=int, default=100,
                        help="Records per INSERT batch (default: 100)")

    # Skip setup mode
    parser.add_argument("--skip-setup", action="store_true",
                        help="Skip setup, only run benchmark (must provide --total and --offset)")
    parser.add_argument("--source", default="Staging",
                        help="Source dataset name (default: Staging)")
    parser.add_argument("--target", default="Target",
                        help="Target dataset name (default: Target)")
    parser.add_argument("--total", type=int,
                        help="Total records to insert (required with --skip-setup)")
    parser.add_argument("--offset", type=int, default=0,
                        help="Starting offset in source (default: 0)")

    args = parser.parse_args()

    if args.skip_setup:
        if args.total is None:
            parser.error("--total is required when using --skip-setup")
        run_benchmark(args, args.total, args.offset)
    else:
        if args.file is None:
            parser.error("--file is required unless --skip-setup is used")
        insert_total, seed_count = run_setup(args)
        run_benchmark(args, insert_total, seed_count)


if __name__ == "__main__":
    main()