# Pruned Search Experiment

## Experiment Setting

| Parameter         | Value |
|-------------------|-------|
| **Dataset**       | GIST-960 (`gist-960-euclidean_train.jsonl.limited_100000`) |
| **Records**       | 100,000 vectors |
| **Dimension**     | 960 |
| **Distance metric** | Euclidean |
| **Quantization**  | SQ8 |
| **K (top-K)**     | 10 |
| **Epsilon**       | 0.3 |
| **Train list**    | 10,000 |
| **Query vectors** | 10 |
| **minprobe**      | 20% of num_clusters |
| **num of clusters** | 50, 100, 200 |
| **Deployment**    | In-process cluster via `AsterixHyracksIntegrationUtil` (1 CC + 2 NCs) |

## Statements

### Create Vector Index

```sql
-- Adjust num_clusters as needed: 50, 100, or 200
CREATE VECTOR INDEX gist_vec_idx ON GIST_EXPERIMENT.Gist(embedding)
WITH {
  "dimension": 960,
  "train_list": 10000,
  "similarity": "euclidean",
  "num_clusters": 100
};
```

### ANN Query (No Pruning)

```sql
SET `compiler.vector.prunedsearch` "false";
SELECT row.idx
FROM GIST_EXPERIMENT.Gist row
ORDER BY ann_distance(row.embedding,
    (SELECT VALUE g.embedding FROM GIST_EXPERIMENT.Gist g WHERE g.idx = 0)[0],
    "euclidean", 20, 0.3)
LIMIT 10;
```

### ANN Query (Pruned)

```sql
SET `compiler.vector.prunedsearch` "true";
SELECT row.idx
FROM GIST_EXPERIMENT.Gist row
ORDER BY ann_distance(row.embedding,
    (SELECT VALUE g.embedding FROM GIST_EXPERIMENT.Gist g WHERE g.idx = 0)[0],
    "euclidean", 20, 0.3)
LIMIT 10;
```

### Exact KNN (Brute Force)

```sql
SELECT row.idx
FROM GIST_EXPERIMENT.Gist row
ORDER BY vector_distance(row.embedding,
    (SELECT VALUE g.embedding FROM GIST_EXPERIMENT.Gist g WHERE g.idx = 0)[0],
    "euclidean")
LIMIT 10;
```

> **Notes:**
> - `ann_distance` arguments: `(field, query_vector, similarity, minprobe, epsilon)`
> - minprobe = 20% of num_clusters (c=50→10, c=100→20, c=200→40)

## Results Summary

| Config | Method | Recall@10 | Avg Latency | Speedup vs KNN |
|--------|--------|-----------|-------------|----------------|
| **c=50, minprobe=10** | KNN (exact) | 100.00% | 622.2ms | 1.0x |
| | ANN (no pruning) | 99.00% | 88.6ms | 7.7x |
| | ANN (pruned) | 79.00% | 45.5ms | 14.5x |
| **c=100, minprobe=20** | KNN (exact) | 100.00% | 583.1ms | 1.0x |
| | ANN (no pruning) | 97.00% | 61.8ms | 10.0x |
| | ANN (pruned) | 81.00% | 31.1ms | 19.6x |
| **c=200, minprobe=40** | KNN (exact) | 100.00% | 725.8ms | 1.0x |
| | ANN (no pruning) | 87.00% | 86.9ms | 8.9x |
| | ANN (pruned) | 74.00% | 49.0ms | 16.3x |

## Per-Query Breakdown

### c=50, minprobe=10

| Query ID | ANN Latency | ANN Recall | Pruned Latency | Pruned Recall | KNN Latency |
|----------|-------------|------------|----------------|---------------|-------------|
| 0 | 179.7ms | 100% | 70.6ms | 90% | 747.7ms |
| 1000 | 108.9ms | 100% | 55.3ms | 90% | 605.2ms |
| 5000 | 72.6ms | 100% | 46.6ms | 90% | 546.9ms |
| 10000 | 71.8ms | 100% | 51.1ms | 50% | 583.5ms |
| 20000 | 64.8ms | 90% | 36.5ms | 90% | 578.1ms |
| 30000 | 69.5ms | 100% | 27.1ms | 20% | 583.3ms |
| 50000 | 115.5ms | 100% | 43.2ms | 100% | 585.6ms |
| 70000 | 69.9ms | 100% | 33.5ms | 90% | 597.1ms |
| 90000 | 62.4ms | 100% | 54.9ms | 70% | 706.6ms |
| 99000 | 70.6ms | 100% | 36.4ms | 100% | 687.9ms |

### c=100, minprobe=20

| Query ID | ANN Latency | ANN Recall | Pruned Latency | Pruned Recall | KNN Latency |
|----------|-------------|------------|----------------|---------------|-------------|
| 0 | 65.3ms | 100% | 30.3ms | 60% | 568.4ms |
| 1000 | 71.2ms | 100% | 39.3ms | 90% | 560.0ms |
| 5000 | 49.6ms | 100% | 28.6ms | 100% | 573.7ms |
| 10000 | 92.6ms | 100% | 28.7ms | 60% | 569.7ms |
| 20000 | 49.0ms | 80% | 32.0ms | 70% | 587.3ms |
| 30000 | 41.5ms | 90% | 19.9ms | 80% | 573.2ms |
| 50000 | 46.1ms | 100% | 42.0ms | 90% | 623.1ms |
| 70000 | 64.9ms | 100% | 24.1ms | 80% | 608.8ms |
| 90000 | 67.4ms | 100% | 32.2ms | 90% | 582.7ms |
| 99000 | 69.9ms | 100% | 33.6ms | 90% | 584.4ms |

### c=200, minprobe=40

| Query ID | ANN Latency | ANN Recall | Pruned Latency | Pruned Recall | KNN Latency |
|----------|-------------|------------|----------------|---------------|-------------|
| 0 | 139.4ms | 60% | 68.9ms | 60% | 887.0ms |
| 1000 | 86.8ms | 100% | 68.5ms | 70% | 741.9ms |
| 5000 | 72.2ms | 80% | 46.2ms | 80% | 841.2ms |
| 10000 | 72.4ms | 60% | 41.1ms | 50% | 672.4ms |
| 20000 | 120.3ms | 100% | 44.8ms | 90% | 664.7ms |
| 30000 | 57.5ms | 100% | 28.4ms | 90% | 752.6ms |
| 50000 | 85.7ms | 100% | 80.0ms | 100% | 703.2ms |
| 70000 | 87.4ms | 80% | 35.8ms | 40% | 675.4ms |
| 90000 | 80.9ms | 100% | 44.3ms | 90% | 641.7ms |
| 99000 | 66.2ms | 90% | 32.2ms | 70% | 678.1ms |

## Key Observations

- **Pruned search** achieves ~1.5-2x speedup over non-pruned ANN, at a cost of ~15-20% recall loss.
- **Best config**: c=100 — best balance of recall (81%) and speed (31.1ms, 19.6x vs KNN).
- **c=200** degrades both ANN and pruned recall, likely due to smaller clusters and SQ8 quantization errors amplified across more clusters.
- **RIGHT pruning never fires** — the triangle inequality threshold `D(x',C) > max_dqx + D(q,C)` is structurally too loose; `D(x,C)` is bounded by cluster radius and never exceeds the threshold.
- **LEFT pruning** accounts for 60-74% of cluster visits but mostly on distant clusters with trivial tuple counts.
- **Recall variance**: Pruned search recall ranges from 20% to 100% per query, indicating sensitivity to query position relative to cluster boundaries.

## Why RIGHT Early Termination Is Structurally Unreachable

### Background

The bidirectional cursor scans data pages sorted by `D(x, C)` (distance from vector to its centroid). Starting from the **pivot** where `D(x, C) ≈ D(q, C)`, it scans RIGHT (increasing `D(x, C)`) and LEFT (decreasing `D(x, C)`) simultaneously.

The early termination conditions use the triangle inequality:

- **RIGHT**: stop when `D(x', C) > max_dqx + D(q, C)`
- **LEFT**: stop when `D(x', C) < D(q, C) - max_dqx`

where `max_dqx` is the current top-K threshold (kth-largest `D(q, x)` in the heap).

### Concrete Example

From the experiment logs — **Query idx=0, Cluster CID=20, c=50, component `0_0_vct`**:

```
D(q, C)        = 1.2844    (distance from query to centroid 20)
max_dqx        = 1.4552    (top-K threshold after scanning this cluster)
RIGHT scanned  = 171 tuples → EXHAUSTED (never pruned)
LEFT scanned   = 257 tuples → EXHAUSTED
```

The RIGHT termination threshold is:

```
threshold = max_dqx + D(q, C)
          = 1.4552 + 1.2844
          = 2.7396
```

For RIGHT to prune, a tuple must have `D(x', C) > 2.7396`. But with c=50 clusters over 100k vectors (~2000 vectors per cluster), the maximum `D(x, C)` at the cluster boundary is typically **~1.5–2.0** (the cluster radius in 960-dimensional normalized Euclidean space).

```
RIGHT cursor scans:  D(x,C) = 1.28 → 1.35 → ... → 1.8 → 2.0 → END OF DATA
Threshold needed:    D(x,C) > 2.74
Gap:                 2.74 - 2.0 = 0.74 → condition NEVER satisfied
```

The cursor exhausts all 171 right-side tuples before ever reaching the pruning threshold.

### Why This Is Structural (Not a Bug)

The threshold `max_dqx + D(q, C)` is the sum of two distances that are individually comparable to the cluster radius:

| Value | Typical range | Meaning |
|-------|--------------|---------|
| `D(q, C)` | 1.0 – 1.9 | Query is ~1 cluster radius from centroid |
| `max_dqx` | 1.0 – 1.7 | Kth-nearest neighbor is ~1 cluster radius away |
| **Threshold** | **2.0 – 3.6** | Sum exceeds max `D(x, C)` by design |
| `max D(x, C)` | 1.5 – 2.0 | Bounded by cluster radius |

The threshold is approximately **2× the cluster radius**, while `D(x, C)` is bounded by **1× the cluster radius**. The condition is geometrically impossible for well-clustered data.

### Contrast with LEFT (Which Does Prune)

For a distant cluster like CID=38 (`D(q, C) = 1.4632`):

```
LEFT threshold = D(q, C) - max_dqx
               = 1.4632 - 1.4439
               = 0.0193
```

Vectors near the centroid center have `D(x, C) ≈ 0.0`, so the LEFT condition fires immediately after **1 tuple** — trivially pruned. However, this is mostly useless: distant clusters contribute few relevant vectors, and pruning 1 tuple saves negligible work.

### Across All Configs

| Config | RIGHT prune rate | LEFT prune rate | LEFT avg tuples when pruned |
|--------|-----------------|----------------|-----------------------------|
| c=50   | 0% (0/140)      | 64% (90/140)   | ~1 tuple                    |
| c=100  | 0% (0/200)      | 68% (136/200)  | ~1 tuple                    |
| c=200  | 0% (0/400)      | 74% (296/400)  | ~1 tuple                    |

RIGHT pruning is 0% across all configurations. LEFT pruning rate increases with more clusters (more distant clusters to trivially prune), but the work saved is negligible.
