#!/usr/bin/env python3
"""Quick sweep: run ANN queries on existing quantized glove index (no index recreation)."""

import os
import json
import requests

ASTERIX_URL = "http://localhost:19002/query/service"
HEADERS = {"Content-Type": "application/x-www-form-urlencoded"}

DATAVERSE = "VectorTest"
DATASET_NAME = "glove_100_angular"
K = 30

NPROBES = [20, 50, 100]
EPSILONS = [0.15, 0.30, 0.45]


def execute_statement(statement, context_id):
    data = {"statement": statement, "pretty": "true", "client_context_id": context_id}
    resp = requests.post(ASTERIX_URL, headers=HEADERS, data=data)
    resp.raise_for_status()
    return resp


def run_ann_query(qvec, nprobe, epsilon):
    stmt = f"""
    USE {DATAVERSE};
    LET qvec = {json.dumps(qvec)}
    FROM {DATASET_NAME} m
    LET dist = ann_distance(m.embedding, qvec, "dot", {nprobe}, {epsilon})
    SELECT m.idx, dist
    ORDER BY dist
    LIMIT {K};
    """
    result = execute_statement(stmt, "ann").json()
    return result.get("results", [])


def run_exact_query(qvec):
    stmt = f"""
    USE {DATAVERSE};
    LET qvec = {json.dumps(qvec)}
    FROM {DATASET_NAME} m
    LET dist = vector_distance(m.embedding, qvec, "dot")
    SELECT m.idx, dist
    ORDER BY dist
    LIMIT {K};
    """
    result = execute_statement(stmt, "exact").json()
    return result.get("results", [])


def compute_recall(ann_results, exact_results):
    ann_ids = set(r["idx"] for r in ann_results)
    exact_ids = set(r["idx"] for r in exact_results)
    overlap = ann_ids & exact_ids
    return len(overlap) / len(exact_ids) if exact_ids else 0.0


def load_query_vector():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(script_dir)
    path = os.path.join(base_dir, "datasets", "glove-100-angular_train.jsonl")
    with open(path) as f:
        record = json.loads(f.readline())
        return record["embedding"]


def main():
    print("Loading query vector...")
    qvec = load_query_vector()

    print("Running exact query (baseline)...")
    exact_results = run_exact_query(qvec)
    print(f"Exact query returned {len(exact_results)} results\n")

    print(f"{'='*60}")
    print(f"  QUANTIZED INDEX (existing, candidateLimit=5*K)")
    print(f"{'='*60}")

    results = {}
    for nprobe in NPROBES:
        for eps in EPSILONS:
            ann = run_ann_query(qvec, nprobe, eps)
            recall = compute_recall(ann, exact_results)
            results[(nprobe, eps)] = recall
            print(f"  nprobe={nprobe:3d}, epsilon={eps:.2f} -> recall@{K} = {recall:.4f} ({int(recall*K)}/{K})")

    print(f"\n{'='*60}")
    print(f"  SUMMARY: GloVe-100 Dot Quantized Recall@{K} (5*K candidates)")
    print(f"{'='*60}")
    print(f"{'nprobe':>8} {'epsilon':>8} {'Recall':>10}")
    print(f"{'-'*30}")
    for nprobe in NPROBES:
        for eps in EPSILONS:
            r = results[(nprobe, eps)]
            print(f"{nprobe:>8} {eps:>8.2f} {r:>10.4f}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
