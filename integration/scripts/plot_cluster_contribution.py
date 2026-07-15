#!/usr/bin/env python3
"""
Plot cluster contribution to top-K for a single query, comparing two mpf values.

x-axis: cluster id (only those that contributed at either mpf)
y-axis: number of records in top-K contributed by that cluster
two bars per cluster: one for each mpf

Usage:
    python plot_cluster_contribution.py \
        --query-json /tmp/movie_5q_mpfsweep.json \
        --log /Users/hongyu/Projects/dev/asterixdb-schema-knn/debug.log \
        --qid 3 --mpf-a 0.3 --mpf-b 0.4 --topk 100 \
        --out /tmp/cluster_contrib_q3.png
"""

import argparse
import json
import re
from collections import Counter

import matplotlib.pyplot as plt
import numpy as np


RESULT_RE = re.compile(r"(\d{2}:\d{2}:\d{2}\.\d+).*\[RESULT\]\s+cid=(-?\d+)\s+dqx=(\S+)")
PRUNE_RE = re.compile(r"(\d{2}:\d{2}:\d{2}\.\d+).*\[PRUNE_STATS\]\s+cid=(-?\d+)")


def to_ms(t):
    h, m, s = t.split(":")
    return int(h) * 3600000 + int(m) * 60000 + int(float(s) * 1000)


def load_blocks(log_path, gap_ms=250):
    events = []
    with open(log_path, "r", errors="replace") as f:
        for line in f:
            m = RESULT_RE.search(line)
            if m:
                events.append((to_ms(m.group(1)), "R", int(m.group(2)), float(m.group(3))))
                continue
            m = PRUNE_RE.search(line)
            if m:
                events.append((to_ms(m.group(1)), "P", int(m.group(2))))
    events.sort()
    blocks, cur, last = [], {"results": [], "probes": []}, -10 ** 12
    for ev in events:
        if ev[0] - last > gap_ms and (cur["results"] or cur["probes"]):
            blocks.append(cur)
            cur = {"results": [], "probes": []}
        if ev[1] == "R":
            cur["results"].append((ev[2], ev[3]))
        else:
            cur["probes"].append(ev[2])
        last = ev[0]
    if cur["results"] or cur["probes"]:
        blocks.append(cur)
    return blocks


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--query-json", required=True)
    p.add_argument("--log", required=True)
    p.add_argument("--qid", type=int, required=True)
    p.add_argument("--mpf-a", type=float, required=True)
    p.add_argument("--mpf-b", type=float, required=True)
    p.add_argument("--topk", type=int, default=100)
    p.add_argument("--out", required=True)
    args = p.parse_args()

    queries = json.load(open(args.query_json))["queries"]
    blocks = load_blocks(args.log)
    if len(blocks) != len(queries):
        print(f"[warn] block count ({len(blocks)}) != query count ({len(queries)})")

    # Find the two target runs for this qid
    contrib = {}  # mpf -> Counter(cid -> count)
    for idx, q in enumerate(queries):
        if q["query_id"] != args.qid:
            continue
        if q["min_probe_fraction"] not in (args.mpf_a, args.mpf_b):
            continue
        topK = blocks[idx]["results"][: args.topk]
        contrib[q["min_probe_fraction"]] = Counter(c for c, _ in topK)

    if args.mpf_a not in contrib or args.mpf_b not in contrib:
        print(f"[error] missing data — found mpf keys: {list(contrib.keys())}")
        return

    # Union of cluster ids appearing in either run, sorted by total contribution descending
    all_cids = set(contrib[args.mpf_a]) | set(contrib[args.mpf_b])
    totals = {c: contrib[args.mpf_a][c] + contrib[args.mpf_b][c] for c in all_cids}
    cids_sorted = sorted(all_cids, key=lambda c: (-totals[c], c))

    counts_a = [contrib[args.mpf_a].get(c, 0) for c in cids_sorted]
    counts_b = [contrib[args.mpf_b].get(c, 0) for c in cids_sorted]

    # Plot
    x = np.arange(len(cids_sorted))
    width = 0.4
    fig, ax = plt.subplots(figsize=(max(10, 0.35 * len(cids_sorted)), 5))
    ax.bar(x - width / 2, counts_a, width, label=f"mpf={args.mpf_a}", color="#3b8bbe")
    ax.bar(x + width / 2, counts_b, width, label=f"mpf={args.mpf_b}", color="#e07b00")
    ax.set_xticks(x)
    ax.set_xticklabels([str(c) for c in cids_sorted], rotation=75, fontsize=8)
    ax.set_xlabel("cluster id")
    ax.set_ylabel(f"records in top-{args.topk}")

    # Pull recall from json for title
    recalls = {q["min_probe_fraction"]: q["recall"]
               for q in queries
               if q["query_id"] == args.qid and q["min_probe_fraction"] in (args.mpf_a, args.mpf_b)}
    ra, rb = recalls.get(args.mpf_a), recalls.get(args.mpf_b)
    ax.set_title(
        f"Query qid={args.qid}: cluster contribution to top-{args.topk}  "
        f"(mpf={args.mpf_a} recall={ra:.0%}, mpf={args.mpf_b} recall={rb:.0%})"
    )
    ax.legend()
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    fig.tight_layout()
    fig.savefig(args.out, dpi=150)
    print(f"[done] wrote {args.out}")
    print(f"  mpf={args.mpf_a}: {len(contrib[args.mpf_a])} contributing clusters, top-{args.topk} total={sum(counts_a)}")
    print(f"  mpf={args.mpf_b}: {len(contrib[args.mpf_b])} contributing clusters, top-{args.topk} total={sum(counts_b)}")


if __name__ == "__main__":
    main()
