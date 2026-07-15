#!/usr/bin/env python3
"""Flat vs Tree experiment: varying min_probe_fraction with fixed epsilon=0.5.

Compares recall across different probe fractions for both flat and hierarchical navigation.
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
from asterixdb_client import AsterixDBClient, parse_time_ms

# --- Configuration ---
ASTERIXDB_URL = "http://localhost:19002/query/service"
DATAVERSE = "MovieDB"
DATASET = "Movies"
VECTOR_FIELD = "embedding"
SIMILARITY = "euclidean_squared"
PK_FIELD = "id"
DEBUG_LOG = os.path.join(os.path.dirname(__file__), "..", "..", "debug.log")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "results_probe")

K = 100
NUM_QUERIES = 15
QUERY_START_IDX = 50
K_MULTIPLIER = 2
FLAT_EPSILON = 1.0  # Match index default epsilon
TREE_EPSILONS = [0.3, 0.5]

PROBE_FRACTIONS = [0.1, 0.2, 0.3, 0.4]


def run_ann_query(client, qvec, use_flat, min_probe_fraction, epsilon):
    qvec_str = json.dumps(qvec)
    flat_arg = 1 if use_flat else 0
    stmt = f"""
    USE {DATAVERSE};
    LET qvec = {qvec_str}
    FROM {DATASET} m
    LET dist = ann_distance(m.{VECTOR_FIELD}, qvec, "{SIMILARITY}", {min_probe_fraction}, {K_MULTIPLIER}, {flat_arg}, {epsilon})
    SELECT m.{PK_FIELD} AS {PK_FIELD}, dist
    ORDER BY dist
    LIMIT {K};
    """
    result = client.execute(stmt, "ann_query")
    metrics = result.get("metrics", {})
    return {
        "results": result.get("results", []),
        "elapsed_ms": parse_time_ms(metrics.get("elapsedTime", "0ms")),
    }


def run_knn_query(client, qvec):
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
    }


def clear_debug_log(log_path):
    with open(log_path, "w") as f:
        f.write("")


def parse_nprobe_logs(log_path, marker_str):
    entries = []
    with open(log_path, "r") as f:
        for line in f:
            if marker_str in line:
                m = re.search(
                    r"clusters=(\d+), epsilon=([0-9.]+), nprobe=(\d+), "
                    r"distComps=(\d+), pagePins=(\d+), timeUs=(\d+)", line)
                if m:
                    entries.append({
                        "clusters": int(m.group(1)),
                        "nprobe": int(m.group(3)),
                        "distComps": int(m.group(4)),
                        "pagePins": int(m.group(5)),
                        "timeUs": int(m.group(6)),
                    })
    return entries[-1] if entries else {}


def compute_recall(ann_results, knn_results):
    ann_ids = set(r.get(PK_FIELD) for r in ann_results)
    knn_ids = set(r.get(PK_FIELD) for r in knn_results)
    return len(ann_ids & knn_ids) / K if K > 0 else 0.0


def run_experiment():
    client = AsterixDBClient(ASTERIXDB_URL, timeout=300)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Fetch query vectors from DB
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
    print(f"Loaded {len(query_vectors)} query vectors")

    # Ground truth
    print("\n=== Brute-force KNN (ground truth) ===")
    knn_results = {}
    for pk, qvec in query_vectors.items():
        knn = run_knn_query(client, qvec)
        knn_results[pk] = knn
        print(f"  KNN id={pk}: {len(knn['results'])} results, {knn['elapsed_ms']:.0f}ms")

    # data[config_key][probe] = {avg_recall, ...}
    # Configs: "Flat" (one, large epsilon), "Tree(ε=0.3)", "Tree(ε=0.5)"
    data = {}

    # Build list of configs: flat once, tree per epsilon
    configs = [("Flat", True, FLAT_EPSILON)]
    for eps in TREE_EPSILONS:
        configs.append((f"Tree(ε={eps})", False, eps))

    for config_key, use_flat, eps in configs:
        data[config_key] = {}
        marker = "[NprobeStrategy FLAT]" if use_flat else "[NprobeStrategy]"

        for probe in PROBE_FRACTIONS:
            recalls = []
            distComps_list = []
            pagePins_list = []
            nprobe_list = []
            clusters_list = []
            elapsed_list = []

            print(f"\n  --- {config_key}, probe={probe} ---")
            for pk, qvec in query_vectors.items():
                clear_debug_log(DEBUG_LOG)
                time.sleep(0.2)
                ann = run_ann_query(client, qvec, use_flat, probe, eps)
                time.sleep(0.3)
                logs = parse_nprobe_logs(DEBUG_LOG, marker)
                recall = compute_recall(ann["results"], knn_results[pk]["results"])
                recalls.append(recall)
                elapsed_list.append(ann["elapsed_ms"])
                if logs:
                    distComps_list.append(logs["distComps"])
                    pagePins_list.append(logs["pagePins"])
                    nprobe_list.append(logs["nprobe"])
                    clusters_list.append(logs["clusters"])
                print(f"    {config_key}(p={probe}) id={pk}: recall={recall:.0%}, "
                      f"distComps={logs.get('distComps', '?')}, "
                      f"nprobe={logs.get('nprobe', '?')}, "
                      f"clusters={logs.get('clusters', '?')}, "
                      f"{ann['elapsed_ms']:.0f}ms")

            data[config_key][probe] = {
                "avg_recall": np.mean(recalls),
                "std_recall": np.std(recalls),
                "avg_distComps": np.mean(distComps_list) if distComps_list else 0,
                "avg_pagePins": np.mean(pagePins_list) if pagePins_list else 0,
                "avg_nprobe": np.mean(nprobe_list) if nprobe_list else 0,
                "avg_clusters": np.mean(clusters_list) if clusters_list else 0,
                "avg_elapsed": np.mean(elapsed_list),
            }

    # Print summary
    print("\n" + "=" * 100)
    print(f"{'Config':<28} {'Recall':>8} {'±σ':>6} {'DistComps':>10} {'PagePins':>9} "
          f"{'Nprobe':>7} {'Clusters':>9} {'ElapsedMs':>10}")
    print("-" * 100)
    for config_key in data:
        for probe in PROBE_FRACTIONS:
            s = data[config_key][probe]
            label = f"{config_key},p={probe}"
            print(f"{label:<28} {s['avg_recall']:>7.2%} {s['std_recall']:>6.3f} "
                  f"{s['avg_distComps']:>10.0f} {s['avg_pagePins']:>9.0f} "
                  f"{s['avg_nprobe']:>7.0f} {s['avg_clusters']:>9.0f} {s['avg_elapsed']:>10.0f}")
    print("=" * 100)

    # Save raw results
    save_path = os.path.join(OUTPUT_DIR, "probe_results.json")
    with open(save_path, "w") as f:
        json.dump({
            "config": {"K": K, "flat_epsilon": FLAT_EPSILON, "tree_epsilons": TREE_EPSILONS,
                       "k_multiplier": K_MULTIPLIER,
                       "probe_fractions": PROBE_FRACTIONS, "num_queries": NUM_QUERIES},
            "data": {ck: {str(p): {k: float(v) for k, v in vals.items()}
                          for p, vals in probes.items()}
                     for ck, probes in data.items()},
        }, f, indent=2)
    print(f"Raw results saved to {save_path}")

    # --- Plots ---
    plot_results(data)
    print(f"Plots saved to {OUTPUT_DIR}/")


def plot_results(data):
    config_keys = list(data.keys())
    x = np.arange(len(PROBE_FRACTIONS))
    n_configs = len(config_keys)
    width = 0.8 / n_configs
    labels = [f"min_probe_fraction={p}" for p in PROBE_FRACTIONS]
    colors = ["#2196F3", "#FF9800", "#FFB74D", "#4CAF50"][:n_configs]

    def grouped_bar(ax, metric, ylabel, title, filename, fmt=".0f", offset=1):
        for i, ck in enumerate(config_keys):
            vals = [data[ck][p][metric] for p in PROBE_FRACTIONS]
            if metric.endswith("recall"):
                stds = [data[ck][p]["std_recall"] for p in PROBE_FRACTIONS]
                bars = ax.bar(x + (i - n_configs / 2 + 0.5) * width, vals, width,
                              yerr=stds, label=ck, color=colors[i], capsize=3,
                              edgecolor="black", linewidth=0.5)
            else:
                bars = ax.bar(x + (i - n_configs / 2 + 0.5) * width, vals, width,
                              label=ck, color=colors[i], edgecolor="black", linewidth=0.5)
            for bar, val in zip(bars, vals):
                fmt_str = f"{val:{fmt}}" if "%" not in fmt else f"{val:.0%}"
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + offset,
                        fmt_str, ha="center", va="bottom", fontsize=7)
        ax.set_ylabel(ylabel)
        ax.set_title(title)
        ax.set_xticks(x)
        ax.set_xticklabels(labels)
        ax.legend(fontsize=8)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, filename), dpi=150)
        plt.close()

    # Plot 1: Recall
    fig, ax = plt.subplots(figsize=(12, 5))
    for i, ck in enumerate(config_keys):
        vals = [data[ck][p]["avg_recall"] for p in PROBE_FRACTIONS]
        stds = [data[ck][p]["std_recall"] for p in PROBE_FRACTIONS]
        bars = ax.bar(x + (i - n_configs / 2 + 0.5) * width, vals, width,
                      yerr=stds, label=ck, color=colors[i], capsize=3,
                      edgecolor="black", linewidth=0.5)
        for bar, val in zip(bars, vals):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.02,
                    f"{val:.0%}", ha="center", va="bottom", fontsize=7)
    ax.set_ylabel("Recall@100")
    ax.set_title(f"Recall vs min_probe_fraction (K={K})")
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylim(0, 1.15)
    ax.legend(fontsize=8)
    ax.axhline(y=1.0, color="green", linestyle="--", alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "recall_by_probe.png"), dpi=150)
    plt.close()

    # Plot 2: Clusters (simple bar, one per config — not grouped by probe)
    fig, ax = plt.subplots(figsize=(8, 5))
    cluster_vals = [data[ck][PROBE_FRACTIONS[0]]["avg_clusters"] for ck in config_keys]
    bars = ax.bar(config_keys, cluster_vals, color=colors, edgecolor="black", linewidth=0.5)
    for bar, val in zip(bars, cluster_vals):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 2,
                f"{val:.0f}", ha="center", va="bottom", fontsize=10)
    ax.set_ylabel("Candidate Clusters")
    ax.set_title("Candidates Discovered (total leaf centroids = 308)")
    ax.axhline(y=308, color="green", linestyle="--", alpha=0.4)
    ax.text(0.02, 315, "Total centroids (308)", color="green", fontsize=9, transform=ax.get_yaxis_transform())
    ax.set_ylim(0, 350)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "clusters_by_probe.png"), dpi=150)
    plt.close()

    # Plot 3: Nprobe
    fig, ax = plt.subplots(figsize=(12, 5))
    grouped_bar(ax, "avg_nprobe", "Nprobe", "Clusters Probed vs probe", "nprobe_by_probe.png")

    # Plot 4: Elapsed
    fig, ax = plt.subplots(figsize=(12, 5))
    grouped_bar(ax, "avg_elapsed", "Elapsed Time (ms)", "Query Latency vs probe", "elapsed_by_probe.png", offset=5)

    # Plot 5: Recall vs Nprobe line chart
    fig, ax = plt.subplots(figsize=(9, 6))
    markers = ["o", "D", "s", "^"]
    for i, ck in enumerate(config_keys):
        nprobes = [data[ck][p]["avg_nprobe"] for p in PROBE_FRACTIONS]
        recalls = [data[ck][p]["avg_recall"] for p in PROBE_FRACTIONS]
        ax.plot(nprobes, recalls, f"{markers[i]}-", color=colors[i], label=ck, markersize=8, linewidth=2)
        for j, p in enumerate(PROBE_FRACTIONS):
            ax.annotate(f"p={p}", (nprobes[j], recalls[j]),
                        textcoords="offset points", xytext=(6, 4 if i % 2 == 0 else -12),
                        fontsize=7, color=colors[i])
    ax.set_xlabel("Nprobe (clusters probed)")
    ax.set_ylabel("Recall@100")
    ax.set_title("Recall vs Nprobe")
    ax.set_ylim(0, 1.1)
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "recall_vs_nprobe.png"), dpi=150)
    plt.close()


if __name__ == "__main__":
    run_experiment()
