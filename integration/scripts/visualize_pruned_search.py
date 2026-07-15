#!/usr/bin/env python3
"""
Visualize pruned search behavior from JVM diagnostic logs.

Parses [PrunedSearch] and [BiDirCursor] WARN-level log lines emitted by
LSMVCTreeBlockedCursor and VectorClusteringBidirectionCursor, then generates
matplotlib charts grouped by (num_clusters, disk component).

Usage:
    # Capture logs during experiment
    python gist_pruned_search_experiment.py run 2>&1 | tee experiment.log

    # Generate visualizations (one disk component per config)
    python visualize_pruned_search.py experiment.log

    # Save to specific directory
    python visualize_pruned_search.py experiment.log --output-dir ./plots

    # Filter by disk component
    python visualize_pruned_search.py experiment.log --component 0_0_vct

    # Text-only (no matplotlib)
    python visualize_pruned_search.py experiment.log --text-only
"""

import argparse
import os
import re
import sys
from collections import defaultdict

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False


# ── Log Parsing ──────────────────────────────────────────────────────────────

# Pattern: [PrunedSearch] [component] Cluster search done: cid=X, D(q,C)=Y, ...
RE_CLUSTER_DONE = re.compile(
    r"\[PrunedSearch\]\s+\[([^\]]+)\]\s+Cluster search done:\s+"
    r"cid=(\d+),\s*D\(q,C\)=([\d.eE+-]+),\s*topK_size=(\d+),\s*"
    r"right_processed=(\d+),\s*left_processed=(\d+),\s*topK_threshold=([\w.eE+-]+)"
)

RE_RIGHT_TERM = re.compile(
    r"\[PrunedSearch\]\s+\[([^\]]+)\]\s+RIGHT terminated:\s+"
    r"cluster_cid=(\d+),\s*D\(x',C\)=([\d.eE+-]+)\s*>\s*threshold=([\d.eE+-]+)\s+"
    r"\(max_dqx=([\d.eE+-]+)\s*\+\s*D\(q,C\)=([\d.eE+-]+)\),\s*tuples_processed_right=(\d+)"
)

RE_RIGHT_EXHAUST = re.compile(
    r"\[PrunedSearch\]\s+\[([^\]]+)\]\s+RIGHT exhausted \(no pruning\):\s+"
    r"cluster_cid=(\d+),\s*tuples_processed_right=(\d+)"
)

RE_LEFT_TERM = re.compile(
    r"\[PrunedSearch\]\s+\[([^\]]+)\]\s+LEFT terminated:\s+"
    r"cluster_cid=(\d+),\s*D\(x',C\)=([\d.eE+-]+)\s*<\s*threshold=([\d.eE+-]+)\s+"
    r"\(D\(q,C\)=([\d.eE+-]+)\s*-\s*max_dqx=([\d.eE+-]+)\),\s*tuples_processed_left=(\d+)"
)

RE_LEFT_EXHAUST = re.compile(
    r"\[PrunedSearch\]\s+\[([^\]]+)\]\s+LEFT exhausted \(no pruning\):\s+"
    r"cluster_cid=(\d+),\s*tuples_processed_left=(\d+)"
)

RE_MULTI_DONE = re.compile(
    r"\[PrunedSearch\]\s+\[([^\]]+)\]\s+Multi-cluster DONE:\s+"
    r"clusters_explored=(\d+),\s*topK_final_size=(\d+),\s*topK_threshold=([\w.eE+-]+)"
)

# Experiment structure markers
RE_BENCHMARK = re.compile(r"BENCHMARK: num_clusters=(\d+)")
RE_QUERY_IDX = re.compile(r"Query idx=(\d+):")


def parse_log_file(log_path, partition_filter=None):
    """Parse log file and return queries grouped by (config, disk component).

    Returns:
        dict: {num_clusters: {component: [query_data, ...]}}
    """
    active_clusters = defaultdict(list)
    active_right = defaultdict(dict)
    active_left = defaultdict(dict)

    # Result: config -> component -> [queries]
    grouped = defaultdict(lambda: defaultdict(list))

    current_config = None
    current_query_idx = None

    with open(log_path, "r") as f:
        for line in f:
            text = line.strip()
            if text.startswith("[jvm]"):
                text = text[5:].strip()

            # Track which benchmark config we're in
            m = RE_BENCHMARK.search(text)
            if m:
                current_config = int(m.group(1))
                continue

            # Track query vector idx
            m = RE_QUERY_IDX.search(text)
            if m:
                current_query_idx = int(m.group(1))
                continue

            # RIGHT terminated
            m = RE_RIGHT_TERM.search(text)
            if m:
                partition = m.group(1)
                if partition_filter and partition != partition_filter:
                    continue
                cid = int(m.group(2))
                active_right[partition][cid] = {
                    "outcome": "pruned",
                    "dxc_stop": float(m.group(3)),
                    "threshold": float(m.group(4)),
                    "max_dqx": float(m.group(5)),
                    "dqc": float(m.group(6)),
                    "tuples": int(m.group(7)),
                }
                continue

            # RIGHT exhausted
            m = RE_RIGHT_EXHAUST.search(text)
            if m:
                partition = m.group(1)
                if partition_filter and partition != partition_filter:
                    continue
                cid = int(m.group(2))
                active_right[partition][cid] = {
                    "outcome": "exhausted",
                    "tuples": int(m.group(3)),
                }
                continue

            # LEFT terminated
            m = RE_LEFT_TERM.search(text)
            if m:
                partition = m.group(1)
                if partition_filter and partition != partition_filter:
                    continue
                cid = int(m.group(2))
                active_left[partition][cid] = {
                    "outcome": "pruned",
                    "dxc_stop": float(m.group(3)),
                    "threshold": float(m.group(4)),
                    "dqc": float(m.group(5)),
                    "max_dqx": float(m.group(6)),
                    "tuples": int(m.group(7)),
                }
                continue

            # LEFT exhausted
            m = RE_LEFT_EXHAUST.search(text)
            if m:
                partition = m.group(1)
                if partition_filter and partition != partition_filter:
                    continue
                cid = int(m.group(2))
                active_left[partition][cid] = {
                    "outcome": "exhausted",
                    "tuples": int(m.group(3)),
                }
                continue

            # Cluster search done
            m = RE_CLUSTER_DONE.search(text)
            if m:
                partition = m.group(1)
                if partition_filter and partition != partition_filter:
                    continue
                cid = int(m.group(2))
                cluster_event = {
                    "cid": cid,
                    "dqc": float(m.group(3)),
                    "topk_size": int(m.group(4)),
                    "right_processed": int(m.group(5)),
                    "left_processed": int(m.group(6)),
                    "topk_threshold": m.group(7),
                    "right": active_right[partition].pop(cid, {}),
                    "left": active_left[partition].pop(cid, {}),
                }
                active_clusters[partition].append(cluster_event)
                continue

            # Multi-cluster DONE
            m = RE_MULTI_DONE.search(text)
            if m:
                partition = m.group(1)
                if partition_filter and partition != partition_filter:
                    continue
                query_data = {
                    "partition": partition,
                    "config": current_config,
                    "query_idx": current_query_idx,
                    "clusters_explored": int(m.group(2)),
                    "topk_final_size": int(m.group(3)),
                    "topk_threshold": m.group(4),
                    "clusters": list(active_clusters[partition]),
                }
                if current_config is not None:
                    grouped[current_config][partition].append(query_data)
                active_clusters[partition] = []
                continue

    return grouped


# ── Text Report ──────────────────────────────────────────────────────────────

def print_text_report(grouped):
    """Print a text-based summary grouped by config and disk component."""
    print("=" * 80)
    print("  PRUNED SEARCH DIAGNOSTIC REPORT")
    print("=" * 80)

    for nc in sorted(grouped.keys()):
        partitions = grouped[nc]
        total_queries = sum(len(qs) for qs in partitions.values())
        print(f"\n  num_clusters = {nc}  ({total_queries} queries across "
              f"{len(partitions)} disk components)")

        for partition in sorted(partitions.keys()):
            queries = partitions[partition]
            print(f"\n  Component: {partition}  ({len(queries)} queries)")
            print(f"  {'-'*76}")

            for q in queries:
                qidx = q.get("query_idx", "?")
                print(f"\n    Query idx={qidx} (clusters_explored={q['clusters_explored']}, "
                      f"topK={q['topk_final_size']})")
                print(f"    {'CID':>6} {'D(q,C)':>10} {'RIGHT':>8} {'R.out':>8} "
                      f"{'LEFT':>8} {'L.out':>8} {'topK':>6} {'threshold':>12}")
                print(f"    {'-'*72}")

                for c in q["clusters"]:
                    r_out = c["right"].get("outcome", "?")[:5]
                    l_out = c["left"].get("outcome", "?")[:5]
                    thr = c["topk_threshold"]
                    try:
                        thr = f"{float(thr):.4f}"
                    except (ValueError, TypeError):
                        pass
                    print(f"    {c['cid']:>6} {c['dqc']:>10.4f} "
                          f"{c['right_processed']:>8} {r_out:>8} "
                          f"{c['left_processed']:>8} {l_out:>8} "
                          f"{c['topk_size']:>6} {thr:>12}")

                total_right = sum(c["right_processed"] for c in q["clusters"])
                total_left = sum(c["left_processed"] for c in q["clusters"])
                total = total_right + total_left
                pruned_r = sum(1 for c in q["clusters"]
                               if c["right"].get("outcome") == "pruned")
                pruned_l = sum(1 for c in q["clusters"]
                               if c["left"].get("outcome") == "pruned")
                n_clusters = len(q["clusters"])

                print(f"    {'':>6} {'TOTAL':>10} {total_right:>8} "
                      f"{pruned_r:>5}/{n_clusters:<2} "
                      f"{total_left:>8} {pruned_l:>5}/{n_clusters:<2} "
                      f"tuples={total}")


# ── Matplotlib Charts ────────────────────────────────────────────────────────

def _pick_partition(partitions):
    """Pick the first disk component (deterministic)."""
    return sorted(partitions.keys())[0]


def plot_cluster_probing(grouped, output_dir):
    """Per-query cluster probing diagram, one disk component per config."""
    for nc in sorted(grouped.keys()):
        partition = _pick_partition(grouped[nc])
        queries = grouped[nc][partition]
        config_dir = os.path.join(output_dir, f"c{nc}")
        os.makedirs(config_dir, exist_ok=True)

        for q in queries:
            if not q["clusters"]:
                continue

            qidx = q.get("query_idx", "?")
            clusters = q["clusters"]
            n = len(clusters)
            fig, ax = plt.subplots(figsize=(12, max(3, n * 0.5 + 1)))

            labels = [f"c{c['cid']} (D={c['dqc']:.2f})" for c in clusters]
            y_pos = range(n)

            right_vals = [c["right_processed"] for c in clusters]
            right_colors = [
                "#4CAF50" if c["right"].get("outcome") == "pruned" else "#F44336"
                for c in clusters
            ]
            left_vals = [-c["left_processed"] for c in clusters]
            left_colors = [
                "#2196F3" if c["left"].get("outcome") == "pruned" else "#FF9800"
                for c in clusters
            ]

            ax.barh(y_pos, right_vals, color=right_colors, height=0.35,
                    align="edge")
            ax.barh([y + 0.35 for y in y_pos], left_vals, color=left_colors,
                    height=0.35, align="edge")

            ax.set_yticks([y + 0.175 for y in y_pos])
            ax.set_yticklabels(labels)
            ax.set_xlabel("Tuples processed (RIGHT positive, LEFT negative)")
            ax.set_title(f"c={nc}, Query idx={qidx}, Component: {partition}\n"
                         f"Clusters explored: {q['clusters_explored']}, "
                         f"TopK: {q['topk_final_size']}")
            ax.axvline(x=0, color="black", linewidth=0.5)

            patches = [
                mpatches.Patch(color="#4CAF50", label="RIGHT pruned"),
                mpatches.Patch(color="#F44336", label="RIGHT exhausted"),
                mpatches.Patch(color="#2196F3", label="LEFT pruned"),
                mpatches.Patch(color="#FF9800", label="LEFT exhausted"),
            ]
            ax.legend(handles=patches, loc="lower right", fontsize=8)
            ax.invert_yaxis()

            plt.tight_layout()
            path = os.path.join(config_dir, f"q{qidx}_cluster_probing.png")
            plt.savefig(path, dpi=150)
            plt.close()
            print(f"  Saved: {path}")


def plot_pruning_efficiency(grouped, output_dir):
    """Summary bar chart per config, one disk component only."""
    for nc in sorted(grouped.keys()):
        partition = _pick_partition(grouped[nc])
        queries = grouped[nc][partition]

        query_labels = []
        pruned_rates = []
        total_tuples = []

        for q in queries:
            n_clusters = len(q["clusters"])
            if n_clusters == 0:
                continue
            pruned_dirs = sum(
                (1 if c["right"].get("outcome") == "pruned" else 0) +
                (1 if c["left"].get("outcome") == "pruned" else 0)
                for c in q["clusters"]
            )
            total_dirs = n_clusters * 2
            rate = pruned_dirs / total_dirs if total_dirs > 0 else 0

            tuples = sum(c["right_processed"] + c["left_processed"]
                         for c in q["clusters"])

            qidx = q.get("query_idx", "?")
            query_labels.append(f"q{qidx}")
            pruned_rates.append(rate)
            total_tuples.append(tuples)

        if not query_labels:
            continue

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

        colors = ["#4CAF50" if r > 0.5 else "#FF9800" if r > 0.25 else "#F44336"
                  for r in pruned_rates]
        ax1.bar(range(len(query_labels)), pruned_rates, color=colors)
        ax1.set_xticks(range(len(query_labels)))
        ax1.set_xticklabels(query_labels, fontsize=7, rotation=45, ha="right")
        ax1.set_ylabel("Pruning rate (directions pruned / total)")
        ax1.set_title(f"c={nc}, Component: {partition}\nPruning Efficiency")
        ax1.set_ylim(0, 1.05)
        ax1.axhline(y=0.5, color="gray", linestyle="--", linewidth=0.8)

        ax2.bar(range(len(query_labels)), total_tuples, color="#607D8B")
        ax2.set_xticks(range(len(query_labels)))
        ax2.set_xticklabels(query_labels, fontsize=7, rotation=45, ha="right")
        ax2.set_ylabel("Total tuples scanned")
        ax2.set_title(f"c={nc}, Component: {partition}\nWork per Query")

        plt.tight_layout()
        path = os.path.join(output_dir, f"c{nc}_pruning_efficiency.png")
        plt.savefig(path, dpi=150)
        plt.close()
        print(f"  Saved: {path}")


def plot_threshold_evolution(grouped, output_dir):
    """Threshold evolution per query, one disk component per config."""
    for nc in sorted(grouped.keys()):
        partition = _pick_partition(grouped[nc])
        queries = grouped[nc][partition]
        config_dir = os.path.join(output_dir, f"c{nc}")
        os.makedirs(config_dir, exist_ok=True)

        for q in queries:
            if len(q["clusters"]) < 2:
                continue

            qidx = q.get("query_idx", "?")
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 6), sharex=True)

            cluster_labels = [f"c{c['cid']}" for c in q["clusters"]]
            x = range(len(cluster_labels))

            # topK threshold
            thresholds = []
            for c in q["clusters"]:
                try:
                    thresholds.append(float(c["topk_threshold"]))
                except (ValueError, TypeError):
                    thresholds.append(None)

            valid = [(j, t) for j, t in enumerate(thresholds) if t is not None]
            if valid:
                xs, ys = zip(*valid)
                ax1.plot(xs, ys, "o-", color="#1976D2", markersize=6)
                ax1.set_ylabel("TopK threshold (kth distance)")
                ax1.set_title(f"c={nc}, Query idx={qidx}, Component: {partition}")
                ax1.grid(True, alpha=0.3)

                # Mark increases in red
                for j in range(1, len(valid)):
                    if valid[j][1] > valid[j-1][1] + 0.0001:
                        ax1.plot(valid[j][0], valid[j][1], "o",
                                 color="red", markersize=8, zorder=5)

            # D(q,C) per cluster
            dqcs = [c["dqc"] for c in q["clusters"]]
            ax2.bar(x, dqcs, color="#78909C", alpha=0.7)
            ax2.set_ylabel("D(q, C)")
            ax2.set_xlabel("Cluster (probing order)")
            ax2.set_xticks(x)
            ax2.set_xticklabels(cluster_labels, rotation=45, fontsize=8)

            plt.tight_layout()
            path = os.path.join(config_dir, f"q{qidx}_threshold_evolution.png")
            plt.savefig(path, dpi=150)
            plt.close()
            print(f"  Saved: {path}")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Visualize pruned search behavior from JVM diagnostic logs")
    parser.add_argument("log_file",
                        help="Path to log file containing [PrunedSearch] lines")
    parser.add_argument("--output-dir", default="./pruned_search_plots",
                        help="Directory to save plots (default: ./pruned_search_plots)")
    parser.add_argument("--component", default=None,
                        help="Filter by disk component ID (e.g., 0_0_vct)")
    parser.add_argument("--text-only", action="store_true",
                        help="Print text report only (no matplotlib required)")
    args = parser.parse_args()

    if not os.path.exists(args.log_file):
        print(f"Error: log file not found: {args.log_file}")
        sys.exit(1)

    print(f"Parsing {args.log_file}...")
    grouped = parse_log_file(args.log_file, partition_filter=args.component)

    total_queries = sum(
        len(qs) for parts in grouped.values() for qs in parts.values()
    )
    print(f"Found {total_queries} pruned search queries across "
          f"{len(grouped)} configs")

    if not grouped:
        print("No [PrunedSearch] log entries found. Make sure:")
        print("  1. The experiment used SET `compiler.vector.prunedsearch` \"true\"")
        print("  2. The log file captures JVM output (stderr)")
        print("  3. Logging level is WARN or higher for LSMVCTreeBlockedCursor")
        sys.exit(0)

    # Always print text report
    print_text_report(grouped)

    if args.text_only:
        return

    if not HAS_MATPLOTLIB:
        print("\nmatplotlib not installed. Install it for chart generation:")
        print("  pip install matplotlib")
        print("Or use --text-only for text report.")
        return

    os.makedirs(args.output_dir, exist_ok=True)
    print(f"\nGenerating plots in {args.output_dir}/...")
    print(f"Using first disk component per config for plots.")

    for nc in sorted(grouped.keys()):
        partition = _pick_partition(grouped[nc])
        print(f"  c={nc}: plotting component {partition}")

    plot_cluster_probing(grouped, args.output_dir)
    plot_pruning_efficiency(grouped, args.output_dir)
    plot_threshold_evolution(grouped, args.output_dir)

    total_files = sum(
        len(f) for _, _, f in os.walk(args.output_dir)
    )
    print(f"\nDone. {total_files} files in {args.output_dir}/")


if __name__ == "__main__":
    main()
