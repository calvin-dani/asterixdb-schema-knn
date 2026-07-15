#!/usr/bin/env python3
"""
GIST-960 Pruned Search Experiment.

Compares pruned search (triangle inequality) vs non-pruned ANN vs exact KNN
across different num_clusters configurations.

Dataset: gist-960-euclidean_train.jsonl.limited_100000
Dimension: 960
Similarity: euclidean
Quantization: SQ8

Usage:
    # Full experiment with auto cluster management
    python gist_pruned_search_experiment.py run

    # Skip lifecycle (cluster already running)
    python gist_pruned_search_experiment.py run --skip-lifecycle

    # Individual steps
    python gist_pruned_search_experiment.py setup
    python gist_pruned_search_experiment.py create-index --num-clusters 100
    python gist_pruned_search_experiment.py benchmark --num-clusters 100
    python gist_pruned_search_experiment.py cleanup

    # Capture JVM logs for visualization
    python gist_pruned_search_experiment.py run 2>&1 | tee experiment.log
"""

import sys
import json
import time
import argparse
import os

# Add integration/tests to path for reusing framework classes
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TESTS_DIR = os.path.join(SCRIPT_DIR, "..", "tests")
sys.path.insert(0, TESTS_DIR)

from asterixdb_client import AsterixDBClient, parse_time_ms
from asterixdb_lifecycle import AsterixDBLifecycleManager

# ── Configuration ────────────────────────────────────────────────────────────

DATAVERSE = "GIST_EXPERIMENT"
DATASET = "Gist"
DIMENSION = 960
SIMILARITY = "euclidean"
TRAIN_LIST = 10000
K = 10
EPSILON = 0.3
NUM_QUERY_VECTORS = 10
CLUSTER_CONFIGS = [50, 100, 200]

DATASET_FILE = os.path.join(SCRIPT_DIR, "..", "datasets",
                            "gist-960-euclidean_train.jsonl.limited_100000")

# Query vector IDs (pick 10 evenly spaced records)
QUERY_IDS = [0, 1000, 5000, 10000, 20000, 30000, 50000, 70000, 90000, 99000]

# Default config matching integration/tests/config.yaml
DEFAULT_CONFIG = {
    "asterixdb": {
        "url": "http://localhost:19002/query/service",
        "health_url": "http://localhost:19002/admin/cluster/summary",
        "startup_timeout": 120,
        "shutdown_timeout": 30,
        "main_class": "org.apache.asterix.api.common.AsterixHyracksIntegrationUtil",
        "project_root": "../..",
        "conf_file": "asterixdb/asterix-app/src/test/resources/cc-main.conf",
        "jvm_args": '-Xmx6144m -Dnode.Resolver="org.apache.asterix.external.util.IdentitiyResolverFactory"',
    }
}


def index_name(num_clusters):
    return f"ix_gist_c{num_clusters}"


def nprobe_for(num_clusters):
    """20% of num_clusters."""
    return max(1, int(num_clusters * 0.2))


# ── Setup ────────────────────────────────────────────────────────────────────

def cmd_setup(client, args):
    """Create dataverse, type, dataset, and load data."""
    print("=" * 60)
    print("SETUP: Create dataverse and load GIST-960 data")
    print("=" * 60)

    # Create dataverse
    print("[step] Creating dataverse...")
    client.execute(f"DROP DATAVERSE {DATAVERSE} IF EXISTS;")
    client.execute(f"CREATE DATAVERSE {DATAVERSE};")
    print("[done] Dataverse created")

    # Create type and dataset
    print("[step] Creating dataset...")
    client.execute(f"""
USE {DATAVERSE};
CREATE TYPE GistType AS open {{
    idx: int
}};
CREATE DATASET {DATASET}(GistType) PRIMARY KEY idx;
""")
    print("[done] Dataset created")

    # Load data
    dataset_path = os.path.abspath(DATASET_FILE)
    if not os.path.exists(dataset_path):
        print(f"[error] Dataset file not found: {dataset_path}")
        sys.exit(1)

    print(f"[step] Loading data from {dataset_path}...")
    start = time.time()
    client.load_dataset(DATAVERSE, DATASET, dataset_path)
    elapsed = time.time() - start
    print(f"[done] Data loaded in {elapsed:.1f}s")

    # Verify count
    cnt = client.count_records(DATAVERSE, DATASET)
    print(f"[info] {DATASET} has {cnt} records")
    print()


def cmd_create_index(client, args):
    """Create a vector index for a specific num_clusters."""
    nc = args.num_clusters
    name = index_name(nc)

    print(f"[step] Creating vector index {name} (clusters={nc}, quantization=SQ8)...")

    # Analyze dataset first (may fail if already analyzed)
    try:
        client.analyze_dataset(DATAVERSE, DATASET)
    except Exception as e:
        print(f"  [warn] ANALYZE DATASET failed (may already be analyzed): {e}")

    client.create_vector_index(
        dataverse=DATAVERSE,
        dataset_name=DATASET,
        index_name=name,
        vector_field="embedding",
        dimension=DIMENSION,
        num_clusters=nc,
        train_list=TRAIN_LIST,
        similarity=SIMILARITY,
        quantization="SQ8",
    )

    print(f"[done] Index {name} created")
    print()


def cmd_drop_index(client, args):
    """Drop a vector index."""
    nc = args.num_clusters
    name = index_name(nc)
    print(f"[step] Dropping index {name}...")
    client.execute(f"USE {DATAVERSE}; DROP INDEX {DATASET}.{name} IF EXISTS;")
    print(f"[done]")


# ── Query Helpers ────────────────────────────────────────────────────────────

def fetch_query_vector(client, query_id):
    """Fetch embedding for a record by idx."""
    return client.fetch_embedding(DATAVERSE, DATASET, "idx", query_id, "embedding")


def run_ann_query(client, qvec, nprobe, pruned=False):
    """Run ANN query, return (results, metrics)."""
    qvec_str = json.dumps(qvec)
    prefix = 'SET `compiler.vector.prunedsearch` "true";\n' if pruned else ""
    statement = f"""{prefix}USE {DATAVERSE};
LET qvec = {qvec_str}
FROM {DATASET} row
LET dist = ann_distance(row.embedding, qvec, "{SIMILARITY}", {nprobe}, {EPSILON})
SELECT row.idx, dist
ORDER BY dist
LIMIT {K};"""

    start = time.time()
    result = client.execute(statement, "ann_query")
    wall_ms = (time.time() - start) * 1000

    metrics = result.get("metrics", {})
    return result.get("results", []), {
        "wall_ms": wall_ms,
        "elapsed_ms": parse_time_ms(metrics.get("elapsedTime", "0ms")),
        "execution_ms": parse_time_ms(metrics.get("executionTime", "0ms")),
    }


def run_knn_query(client, qvec):
    """Run exact KNN query (brute-force), return (results, metrics)."""
    result = client.run_exact_knn_query(
        dataverse=DATAVERSE,
        dataset_name=DATASET,
        vector_field="embedding",
        qvec=qvec,
        similarity=SIMILARITY,
        k=K,
    )
    return result["results"], {
        "elapsed_ms": result["elapsed_ms"],
        "execution_ms": result["execution_ms"],
    }


def compute_recall(ann_results, knn_results):
    """Compute recall: fraction of KNN results found in ANN results."""
    ann_ids = set(r.get("idx") for r in ann_results)
    knn_ids = set(r.get("idx") for r in knn_results)
    if not knn_ids:
        return 0.0
    return len(ann_ids & knn_ids) / len(knn_ids)


# ── Benchmark ────────────────────────────────────────────────────────────────

def prefetch_query_vectors(client):
    """Pre-fetch all query vectors (must be called before index creation)."""
    print("[step] Fetching query vectors...")
    query_vectors = {}
    for qid in QUERY_IDS:
        qvec = fetch_query_vector(client, qid)
        if qvec:
            query_vectors[qid] = qvec
        else:
            print(f"  [warn] No embedding for idx={qid}, skipping")
    print(f"[done] {len(query_vectors)} query vectors ready")
    return query_vectors


def cmd_benchmark(client, args, query_vectors=None):
    """Run benchmark for a specific num_clusters config."""
    nc = args.num_clusters
    nprobe = nprobe_for(nc)

    print("=" * 70)
    print(f"BENCHMARK: num_clusters={nc}, nprobe={nprobe}, K={K}, epsilon={EPSILON}")
    print("=" * 70)

    # Use pre-fetched vectors or fetch now (standalone mode)
    if query_vectors is None:
        query_vectors = prefetch_query_vectors(client)
    print()

    if not query_vectors:
        print("[error] No query vectors found")
        return

    # Run queries
    results = []

    for qid, qvec in query_vectors.items():
        print(f"  Query idx={qid}:")

        # 1) ANN without pruning
        ann_results, ann_metrics = run_ann_query(client, qvec, nprobe, pruned=False)

        # 2) ANN with pruning
        pruned_results, pruned_metrics = run_ann_query(client, qvec, nprobe, pruned=True)

        # 3) Exact KNN
        knn_results, knn_metrics = run_knn_query(client, qvec)

        # Compute recalls
        ann_recall = compute_recall(ann_results, knn_results)
        pruned_recall = compute_recall(pruned_results, knn_results)

        ann_speedup = knn_metrics["elapsed_ms"] / ann_metrics["elapsed_ms"] if ann_metrics["elapsed_ms"] > 0 else 0
        pruned_speedup = knn_metrics["elapsed_ms"] / pruned_metrics["elapsed_ms"] if pruned_metrics["elapsed_ms"] > 0 else 0

        print(f"    ANN (no prune): {ann_metrics['elapsed_ms']:.1f}ms, recall={ann_recall:.2%}, speedup={ann_speedup:.1f}x")
        print(f"    ANN (pruned):   {pruned_metrics['elapsed_ms']:.1f}ms, recall={pruned_recall:.2%}, speedup={pruned_speedup:.1f}x")
        print(f"    KNN (exact):    {knn_metrics['elapsed_ms']:.1f}ms")

        results.append({
            "query_idx": qid,
            "ann": {"recall": ann_recall, **ann_metrics, "num_results": len(ann_results)},
            "pruned": {"recall": pruned_recall, **pruned_metrics, "num_results": len(pruned_results)},
            "knn": {**knn_metrics, "num_results": len(knn_results)},
        })

    print()
    print_summary(nc, nprobe, results)
    return results


def print_summary(num_clusters, nprobe, results):
    """Print aggregate summary."""
    n = len(results)
    if n == 0:
        return

    def avg(vals):
        return sum(vals) / len(vals) if vals else 0

    ann_recalls = [r["ann"]["recall"] for r in results]
    pruned_recalls = [r["pruned"]["recall"] for r in results]
    ann_elapsed = [r["ann"]["elapsed_ms"] for r in results]
    pruned_elapsed = [r["pruned"]["elapsed_ms"] for r in results]
    knn_elapsed = [r["knn"]["elapsed_ms"] for r in results]

    ann_speedups = [r["knn"]["elapsed_ms"] / r["ann"]["elapsed_ms"] if r["ann"]["elapsed_ms"] > 0 else 0 for r in results]
    pruned_speedups = [r["knn"]["elapsed_ms"] / r["pruned"]["elapsed_ms"] if r["pruned"]["elapsed_ms"] > 0 else 0 for r in results]

    print("=" * 70)
    print(f"SUMMARY: num_clusters={num_clusters}, nprobe={nprobe}, K={K}")
    print("=" * 70)
    print(f"  {'':20} {'Avg Recall':>12} {'Avg Time':>12} {'Avg Speedup':>14}")
    print(f"  {'-'*58}")
    print(f"  {'ANN (no pruning)':<20} {avg(ann_recalls):>11.2%} {avg(ann_elapsed):>10.1f}ms {avg(ann_speedups):>12.1f}x")
    print(f"  {'ANN (pruned)':<20} {avg(pruned_recalls):>11.2%} {avg(pruned_elapsed):>10.1f}ms {avg(pruned_speedups):>12.1f}x")
    print(f"  {'KNN (exact)':<20} {'100.00%':>12} {avg(knn_elapsed):>10.1f}ms {'1.0x':>14}")
    print()
    print(f"  Recall range (no prune): {min(ann_recalls):.2%} - {max(ann_recalls):.2%}")
    print(f"  Recall range (pruned):   {min(pruned_recalls):.2%} - {max(pruned_recalls):.2%}")
    print()


# ── Full Experiment ──────────────────────────────────────────────────────────

def cmd_run(client, args):
    """Run the full experiment: setup + benchmark all configs."""
    all_results = {}

    # Setup (skip if data already loaded)
    if args.skip_setup:
        print("[info] Skipping setup (--skip-setup), assuming data is loaded")
    else:
        cmd_setup(client, args)

    # Pre-fetch query vectors BEFORE creating any index
    # (querying the dataset fails while a vector index exists due to metadata resolution)
    query_vectors = prefetch_query_vectors(client)
    if not query_vectors:
        print("[error] No query vectors found, aborting")
        return
    print()

    for nc in CLUSTER_CONFIGS:
        print()
        print("#" * 70)
        print(f"# EXPERIMENT: num_clusters = {nc}")
        print("#" * 70)
        print()

        # Create index
        args.num_clusters = nc
        cmd_create_index(client, args)

        # Benchmark
        results = cmd_benchmark(client, args, query_vectors=query_vectors)
        all_results[nc] = results

        # Drop index before next config
        cmd_drop_index(client, args)
        print()

    # Final comparison table
    print()
    print("#" * 70)
    print("# FINAL COMPARISON ACROSS ALL CONFIGURATIONS")
    print("#" * 70)
    print()
    print(f"  {'Config':<15} {'nprobe':<8} {'ANN Recall':>12} {'ANN Time':>10} {'Pruned Recall':>15} {'Pruned Time':>12} {'KNN Time':>10}")
    print(f"  {'-'*82}")

    for nc in CLUSTER_CONFIGS:
        results = all_results.get(nc, [])
        if not results:
            continue
        nprobe = nprobe_for(nc)
        avg_ann_recall = sum(r["ann"]["recall"] for r in results) / len(results)
        avg_ann_time = sum(r["ann"]["elapsed_ms"] for r in results) / len(results)
        avg_pruned_recall = sum(r["pruned"]["recall"] for r in results) / len(results)
        avg_pruned_time = sum(r["pruned"]["elapsed_ms"] for r in results) / len(results)
        avg_knn_time = sum(r["knn"]["elapsed_ms"] for r in results) / len(results)
        print(f"  c={nc:<11} {nprobe:<8} {avg_ann_recall:>11.2%} {avg_ann_time:>8.1f}ms {avg_pruned_recall:>14.2%} {avg_pruned_time:>10.1f}ms {avg_knn_time:>8.1f}ms")

    print()

    # Save results
    output_file = args.output or "gist_pruned_experiment_results.json"
    output = {
        "config": {
            "dataset": f"{DATAVERSE}.{DATASET}",
            "dimension": DIMENSION,
            "similarity": SIMILARITY,
            "train_list": TRAIN_LIST,
            "K": K,
            "epsilon": EPSILON,
            "num_query_vectors": len(QUERY_IDS),
            "query_ids": QUERY_IDS,
            "cluster_configs": CLUSTER_CONFIGS,
        },
        "results": {str(nc): all_results.get(nc, []) for nc in CLUSTER_CONFIGS},
    }
    with open(output_file, "w") as f:
        json.dump(output, f, indent=2)
    print(f"  Results saved to: {output_file}")


def cmd_cleanup(client, args):
    """Drop the experiment dataverse."""
    print("[step] Dropping dataverse...")
    client.execute(f"DROP DATAVERSE {DATAVERSE} IF EXISTS;")
    print("[done]")


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    # Common args shared by all subcommands
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--config", default=None,
                        help="Path to config YAML (overrides default AsterixDB settings)")
    common.add_argument("--output", default=None, help="Output JSON file")
    common.add_argument("--skip-lifecycle", action="store_true",
                        help="Skip AsterixDB start/stop (assume already running)")
    common.add_argument("--skip-setup", action="store_true",
                        help="Skip dataverse/dataset creation and data loading")

    parser = argparse.ArgumentParser(description="GIST-960 Pruned Search Experiment")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("setup", parents=[common], help="Create dataverse, dataset, load data")
    subparsers.add_parser("cleanup", parents=[common], help="Drop experiment dataverse")

    p_create = subparsers.add_parser("create-index", parents=[common], help="Create index for specific num_clusters")
    p_create.add_argument("--num-clusters", type=int, required=True)

    p_drop = subparsers.add_parser("drop-index", parents=[common], help="Drop index for specific num_clusters")
    p_drop.add_argument("--num-clusters", type=int, required=True)

    p_bench = subparsers.add_parser("benchmark", parents=[common], help="Benchmark specific num_clusters config")
    p_bench.add_argument("--num-clusters", type=int, required=True)

    subparsers.add_parser("run", parents=[common], help="Full experiment: setup + all benchmarks")

    args = parser.parse_args()

    # Load config
    if args.config:
        import yaml
        with open(args.config, "r") as f:
            config = yaml.safe_load(f)
    else:
        config = DEFAULT_CONFIG

    ax_cfg = config["asterixdb"]

    # Initialize client
    client = AsterixDBClient(ax_cfg["url"])
    lifecycle = None

    # Lifecycle management
    if not args.skip_lifecycle:
        config_dir = os.path.dirname(os.path.abspath(args.config)) if args.config else TESTS_DIR
        lifecycle = AsterixDBLifecycleManager(ax_cfg, config_dir)
        try:
            lifecycle.start(cleanup=True)
            client.wait_for_ready(ax_cfg["health_url"], ax_cfg.get("startup_timeout", 120))
        except Exception as e:
            print(f"\nFATAL: Could not start AsterixDB: {e}")
            sys.exit(1)
    else:
        # Verify connectivity
        try:
            client.wait_for_ready(ax_cfg["health_url"], timeout=10)
        except Exception as e:
            print(f"\nFATAL: AsterixDB not reachable: {e}")
            sys.exit(1)

    try:
        commands = {
            "setup": cmd_setup,
            "cleanup": cmd_cleanup,
            "create-index": cmd_create_index,
            "drop-index": cmd_drop_index,
            "benchmark": cmd_benchmark,
            "run": cmd_run,
        }
        commands[args.command](client, args)
    finally:
        if lifecycle and not args.skip_lifecycle:
            try:
                lifecycle.stop()
            except Exception as e:
                print(f"Warning: Error stopping AsterixDB: {e}")


if __name__ == "__main__":
    main()
