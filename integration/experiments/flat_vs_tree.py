#!/usr/bin/env python3
"""Flat vs Hierarchical VTree A/B Experiment.

Compares recall, distance computations, page pins, and latency between
flat (single-level) and hierarchical (tree-routed) navigation on the same index.

Prerequisites:
  - AsterixDB cluster running at localhost:19002
  - MovieDB dataverse with Movies dataset and ix_movies_emb vector index created
  - Index created with both flat and hierarchical structures (our dual-structure build)

Usage:
  cd integration/experiments
  python3 flat_vs_tree.py
"""

import json
import os
import sys
import re
import time

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tests"))
from asterixdb_client import AsterixDBClient, parse_time_ms, load_query_vector_from_file

# --- Configuration ---
ASTERIXDB_URL = "http://localhost:19002/query/service"
DATAVERSE = "MovieDB"
DATASET = "Movies"
VECTOR_FIELD = "embedding"
SIMILARITY = "euclidean_squared"
PK_FIELD = "id"
DATASET_FILE = os.path.join(os.path.dirname(__file__), "..", "datasets", "movie_embeddings_384d.json")
DEBUG_LOG = os.path.join(os.path.dirname(__file__), "..", "..", "debug.log")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "results")

K = 100
NUM_QUERIES = 15
QUERY_START_IDX = 50
MIN_PROBE_FRACTION = 0.2
K_MULTIPLIER = 2

# Epsilon values to test for hierarchical navigation
HIERARCHICAL_EPSILONS = [0.1, 0.2, 0.3, 0.5, 1.0]
# Flat always scans all centroids, so epsilon only affects post-filter
FLAT_EPSILON = None  # Use index default


def run_ann_query(client, qvec, use_flat, epsilon_override=None):
    """Run ANN query with flat or hierarchical navigation."""
    qvec_str = json.dumps(qvec)
    flat_arg = 1 if use_flat else 0

    if epsilon_override is not None:
        stmt = f"""
        USE {DATAVERSE};
        LET qvec = {qvec_str}
        FROM {DATASET} m
        LET dist = ann_distance(m.{VECTOR_FIELD}, qvec, "{SIMILARITY}", {MIN_PROBE_FRACTION}, {K_MULTIPLIER}, {flat_arg}, {epsilon_override})
        SELECT m.{PK_FIELD} AS {PK_FIELD}, dist
        ORDER BY dist
        LIMIT {K};
        """
    else:
        stmt = f"""
        USE {DATAVERSE};
        LET qvec = {qvec_str}
        FROM {DATASET} m
        LET dist = ann_distance(m.{VECTOR_FIELD}, qvec, "{SIMILARITY}", {MIN_PROBE_FRACTION}, {K_MULTIPLIER}, {flat_arg})
        SELECT m.{PK_FIELD} AS {PK_FIELD}, dist
        ORDER BY dist
        LIMIT {K};
        """

    result = client.execute(stmt, "ann_query")
    metrics = result.get("metrics", {})
    return {
        "results": result.get("results", []),
        "elapsed_ms": parse_time_ms(metrics.get("elapsedTime", "0ms")),
        "execution_ms": parse_time_ms(metrics.get("executionTime", "0ms")),
    }


def run_knn_query(client, qvec):
    """Run exact brute-force KNN query."""
    qvec_str = json.dumps(qvec)
    stmt = f"""
    USE {DATAVERSE};
    LET qvec = {qvec_str}
    FROM {DATASET} m
    LET dist = euclidean_squared_distance(m.{VECTOR_FIELD}, qvec)
    SELECT m.{PK_FIELD} AS {PK_FIELD}, dist
    ORDER BY dist
    LIMIT {K};
    """
    result = client.execute(stmt, "knn_query")
    metrics = result.get("metrics", {})
    return {
        "results": result.get("results", []),
        "elapsed_ms": parse_time_ms(metrics.get("elapsedTime", "0ms")),
        "execution_ms": parse_time_ms(metrics.get("executionTime", "0ms")),
    }


def parse_nprobe_logs(log_path, marker_str, max_entries=4):
    """Parse NprobeStrategy logs from debug.log for the most recent query.

    Returns list of dicts with clusters, distComps, pagePins, timeUs.
    """
    entries = []
    with open(log_path, "r") as f:
        for line in f:
            if marker_str in line:
                m = re.search(
                    r"clusters=(\d+), epsilon=([0-9.]+), nprobe=(\d+), "
                    r"distComps=(\d+), pagePins=(\d+), timeUs=(\d+)",
                    line,
                )
                if m:
                    entries.append({
                        "clusters": int(m.group(1)),
                        "epsilon": float(m.group(2)),
                        "nprobe": int(m.group(3)),
                        "distComps": int(m.group(4)),
                        "pagePins": int(m.group(5)),
                        "timeUs": int(m.group(6)),
                    })
    # Return last max_entries (from most recent query)
    return entries[-max_entries:] if len(entries) >= max_entries else entries


def clear_debug_log(log_path):
    """Truncate debug.log to separate query runs."""
    with open(log_path, "w") as f:
        f.write("")


def compute_recall(ann_results, knn_results):
    """Compute recall as intersection of PK sets."""
    ann_ids = set(r.get(PK_FIELD) for r in ann_results)
    knn_ids = set(r.get(PK_FIELD) for r in knn_results)
    return len(ann_ids & knn_ids) / K if K > 0 else 0.0


def avg_metric(log_entries, key):
    """Average a metric across partitions."""
    if not log_entries:
        return 0
    return sum(e[key] for e in log_entries) / len(log_entries)


def run_experiment():
    client = AsterixDBClient(ASTERIXDB_URL, timeout=300)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Load query vectors: fetch PKs from DB, then fetch embeddings
    pk_result = client.execute(
        f"USE {DATAVERSE}; SELECT m.{PK_FIELD} FROM {DATASET} m ORDER BY m.{PK_FIELD} LIMIT {QUERY_START_IDX + NUM_QUERIES};",
        "fetch_pks")
    all_pks = [r[PK_FIELD] for r in pk_result["results"]]
    query_pks = all_pks[QUERY_START_IDX:QUERY_START_IDX + NUM_QUERIES]

    query_vectors = {}
    for pk in query_pks:
        emb_result = client.execute(
            f"USE {DATAVERSE}; SELECT VALUE m.{VECTOR_FIELD} FROM {DATASET} m WHERE m.{PK_FIELD} = {pk};",
            f"fetch_emb_{pk}")
        if emb_result["results"]:
            query_vectors[pk] = emb_result["results"][0]
    print(f"Loaded {len(query_vectors)} query vectors (PKs: {query_pks[:3]}...{query_pks[-1]})")

    # --- Phase 1: Brute-force KNN (ground truth) ---
    print("\n=== Phase 1: Brute-force KNN (ground truth) ===")
    knn_results = {}
    for pk, qvec in query_vectors.items():
        knn = run_knn_query(client, qvec)
        knn_results[pk] = knn
        print(f"  KNN idx={pk}: {len(knn['results'])} results, {knn['elapsed_ms']:.0f}ms")

    # --- Phase 2: Flat ANN queries ---
    print("\n=== Phase 2: Flat ANN queries ===")
    flat_data = {}
    for pk, qvec in query_vectors.items():
        clear_debug_log(DEBUG_LOG)
        time.sleep(0.2)  # Let log flush
        ann = run_ann_query(client, qvec, use_flat=True)
        time.sleep(0.3)
        logs = parse_nprobe_logs(DEBUG_LOG, "[NprobeStrategy FLAT]")
        recall = compute_recall(ann["results"], knn_results[pk]["results"])
        if pk == list(query_vectors.keys())[0]:
            print(f"    DEBUG ANN first 3: {ann['results'][:3]}")
            print(f"    DEBUG KNN first 3: {knn_results[pk]['results'][:3]}")
        flat_data[pk] = {
            "recall": recall,
            "elapsed_ms": ann["elapsed_ms"],
            "execution_ms": ann["execution_ms"],
            "avg_distComps": avg_metric(logs, "distComps"),
            "avg_pagePins": avg_metric(logs, "pagePins"),
            "avg_timeUs": avg_metric(logs, "timeUs"),
            "avg_clusters": avg_metric(logs, "clusters"),
        }
        print(f"  FLAT idx={pk}: recall={recall:.2%}, distComps={flat_data[pk]['avg_distComps']:.0f}, "
              f"pagePins={flat_data[pk]['avg_pagePins']:.0f}, {ann['elapsed_ms']:.0f}ms")

    # --- Phase 3: Hierarchical ANN queries (varying epsilon) ---
    print("\n=== Phase 3: Hierarchical ANN queries ===")
    hier_data = {}  # epsilon -> {pk -> metrics}
    for eps in HIERARCHICAL_EPSILONS:
        hier_data[eps] = {}
        print(f"\n  --- Epsilon = {eps} ---")
        for pk, qvec in query_vectors.items():
            clear_debug_log(DEBUG_LOG)
            time.sleep(0.2)
            ann = run_ann_query(client, qvec, use_flat=False, epsilon_override=eps)
            time.sleep(0.3)
            logs = parse_nprobe_logs(DEBUG_LOG, "[NprobeStrategy]")
            # Filter out any FLAT logs that might match
            logs = [l for l in logs if True]  # Already filtered by marker_str
            recall = compute_recall(ann["results"], knn_results[pk]["results"])
            hier_data[eps][pk] = {
                "recall": recall,
                "elapsed_ms": ann["elapsed_ms"],
                "execution_ms": ann["execution_ms"],
                "avg_distComps": avg_metric(logs, "distComps"),
                "avg_pagePins": avg_metric(logs, "pagePins"),
                "avg_timeUs": avg_metric(logs, "timeUs"),
                "avg_clusters": avg_metric(logs, "clusters"),
            }
            print(f"    HIER(eps={eps}) idx={pk}: recall={recall:.2%}, "
                  f"distComps={hier_data[eps][pk]['avg_distComps']:.0f}, "
                  f"pagePins={hier_data[eps][pk]['avg_pagePins']:.0f}, {ann['elapsed_ms']:.0f}ms")

    # --- Compute aggregates ---
    results_summary = compute_summary(flat_data, hier_data)
    save_results(results_summary, flat_data, hier_data)
    plot_results(results_summary)

    print("\n=== Experiment Complete ===")
    print(f"Results saved to {OUTPUT_DIR}/")


def compute_summary(flat_data, hier_data):
    """Compute per-config averages across all query vectors."""
    summary = {}

    # Flat
    flat_recalls = [v["recall"] for v in flat_data.values()]
    flat_distComps = [v["avg_distComps"] for v in flat_data.values()]
    flat_pagePins = [v["avg_pagePins"] for v in flat_data.values()]
    flat_timeUs = [v["avg_timeUs"] for v in flat_data.values()]
    flat_elapsed = [v["elapsed_ms"] for v in flat_data.values()]
    flat_clusters = [v["avg_clusters"] for v in flat_data.values()]
    summary["Flat"] = {
        "avg_recall": np.mean(flat_recalls),
        "std_recall": np.std(flat_recalls),
        "avg_distComps": np.mean(flat_distComps),
        "avg_pagePins": np.mean(flat_pagePins),
        "avg_timeUs": np.mean(flat_timeUs),
        "avg_elapsed_ms": np.mean(flat_elapsed),
        "avg_clusters": np.mean(flat_clusters),
    }

    # Hierarchical per epsilon
    for eps in hier_data:
        recalls = [v["recall"] for v in hier_data[eps].values()]
        distComps = [v["avg_distComps"] for v in hier_data[eps].values()]
        pagePins = [v["avg_pagePins"] for v in hier_data[eps].values()]
        timeUs = [v["avg_timeUs"] for v in hier_data[eps].values()]
        elapsed = [v["elapsed_ms"] for v in hier_data[eps].values()]
        clusters = [v["avg_clusters"] for v in hier_data[eps].values()]
        summary[f"Tree(ε={eps})"] = {
            "avg_recall": np.mean(recalls),
            "std_recall": np.std(recalls),
            "avg_distComps": np.mean(distComps),
            "avg_pagePins": np.mean(pagePins),
            "avg_timeUs": np.mean(timeUs),
            "avg_elapsed_ms": np.mean(elapsed),
            "avg_clusters": np.mean(clusters),
        }

    # Print summary table
    print("\n" + "=" * 90)
    print(f"{'Config':<18} {'Recall':>8} {'±σ':>6} {'DistComps':>10} {'PagePins':>9} "
          f"{'NavTimeUs':>10} {'Clusters':>9} {'ElapsedMs':>10}")
    print("-" * 90)
    for name, s in summary.items():
        print(f"{name:<18} {s['avg_recall']:>7.2%} {s['std_recall']:>6.3f} "
              f"{s['avg_distComps']:>10.0f} {s['avg_pagePins']:>9.0f} "
              f"{s['avg_timeUs']:>10.0f} {s['avg_clusters']:>9.0f} {s['avg_elapsed_ms']:>10.0f}")
    print("=" * 90)

    return summary


def save_results(summary, flat_data, hier_data):
    """Save raw results to JSON."""
    output = {
        "config": {
            "K": K,
            "num_queries": NUM_QUERIES,
            "min_probe_fraction": MIN_PROBE_FRACTION,
            "k_multiplier": K_MULTIPLIER,
            "hierarchical_epsilons": HIERARCHICAL_EPSILONS,
        },
        "summary": {k: {kk: float(vv) for kk, vv in v.items()} for k, v in summary.items()},
        "flat_per_query": {str(k): v for k, v in flat_data.items()},
        "hier_per_query": {
            str(eps): {str(pk): v for pk, v in pks.items()}
            for eps, pks in hier_data.items()
        },
    }
    path = os.path.join(OUTPUT_DIR, "flat_vs_tree_results.json")
    with open(path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"Raw results saved to {path}")


def plot_results(summary):
    """Generate comparison bar charts."""
    configs = list(summary.keys())
    colors = ["#2196F3"] + ["#FF9800"] * len(HIERARCHICAL_EPSILONS)

    # --- Plot 1: Recall ---
    fig, ax = plt.subplots(figsize=(10, 5))
    recalls = [summary[c]["avg_recall"] for c in configs]
    stds = [summary[c]["std_recall"] for c in configs]
    bars = ax.bar(configs, recalls, yerr=stds, color=colors, capsize=5, edgecolor="black", linewidth=0.5)
    ax.set_ylabel("Recall@100")
    ax.set_title(f"Recall: Flat vs Hierarchical VTree (K={K}, probe={MIN_PROBE_FRACTION})")
    ax.set_ylim(0, 1.05)
    for bar, val in zip(bars, recalls):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.02,
                f"{val:.2%}", ha="center", va="bottom", fontsize=9)
    ax.axhline(y=1.0, color="green", linestyle="--", alpha=0.3, label="Perfect recall")
    plt.xticks(rotation=15, ha="right")
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "recall.png"), dpi=150)
    plt.close()

    # --- Plot 2: Distance Computations ---
    fig, ax = plt.subplots(figsize=(10, 5))
    distComps = [summary[c]["avg_distComps"] for c in configs]
    bars = ax.bar(configs, distComps, color=colors, edgecolor="black", linewidth=0.5)
    ax.set_ylabel("Avg Distance Computations")
    ax.set_title("Cluster Selection: Distance Computations")
    for bar, val in zip(bars, distComps):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1,
                f"{val:.0f}", ha="center", va="bottom", fontsize=9)
    plt.xticks(rotation=15, ha="right")
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "dist_computations.png"), dpi=150)
    plt.close()

    # --- Plot 3: Page Pins ---
    fig, ax = plt.subplots(figsize=(10, 5))
    pagePins = [summary[c]["avg_pagePins"] for c in configs]
    bars = ax.bar(configs, pagePins, color=colors, edgecolor="black", linewidth=0.5)
    ax.set_ylabel("Avg Page Pins")
    ax.set_title("Cluster Selection: Page Pins (I/O)")
    for bar, val in zip(bars, pagePins):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
                f"{val:.0f}", ha="center", va="bottom", fontsize=9)
    plt.xticks(rotation=15, ha="right")
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "page_pins.png"), dpi=150)
    plt.close()

    # --- Plot 4: Navigation Time ---
    fig, ax = plt.subplots(figsize=(10, 5))
    timeUs = [summary[c]["avg_timeUs"] for c in configs]
    bars = ax.bar(configs, timeUs, color=colors, edgecolor="black", linewidth=0.5)
    ax.set_ylabel("Avg Navigation Time (μs)")
    ax.set_title("Cluster Selection: Navigation Latency")
    for bar, val in zip(bars, timeUs):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 10,
                f"{val:.0f}", ha="center", va="bottom", fontsize=9)
    plt.xticks(rotation=15, ha="right")
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "nav_time.png"), dpi=150)
    plt.close()

    # --- Plot 5: Candidate Clusters ---
    fig, ax = plt.subplots(figsize=(10, 5))
    clusters = [summary[c]["avg_clusters"] for c in configs]
    bars = ax.bar(configs, clusters, color=colors, edgecolor="black", linewidth=0.5)
    ax.set_ylabel("Avg Candidate Clusters (after ε filter)")
    ax.set_title("Clusters Selected for Probing")
    for bar, val in zip(bars, clusters):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                f"{val:.0f}", ha="center", va="bottom", fontsize=9)
    plt.xticks(rotation=15, ha="right")
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "clusters.png"), dpi=150)
    plt.close()

    # --- Plot 6: Combined Recall vs Cost (scatter) ---
    fig, ax = plt.subplots(figsize=(8, 6))
    for i, c in enumerate(configs):
        ax.scatter(summary[c]["avg_distComps"], summary[c]["avg_recall"],
                   s=150, color=colors[i], edgecolors="black", linewidth=0.5, zorder=5)
        ax.annotate(c, (summary[c]["avg_distComps"], summary[c]["avg_recall"]),
                    textcoords="offset points", xytext=(8, 5), fontsize=8)
    ax.set_xlabel("Avg Distance Computations (cost)")
    ax.set_ylabel("Avg Recall@100")
    ax.set_title("Recall vs Navigation Cost Tradeoff")
    ax.set_ylim(0, 1.05)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "recall_vs_cost.png"), dpi=150)
    plt.close()

    print(f"\nPlots saved to {OUTPUT_DIR}/")


if __name__ == "__main__":
    run_experiment()
