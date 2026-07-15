# VTree Structure & Navigation Analysis

## Dataset

- ~100K movie records with 384-dimensional embeddings
- Index: VTree with SQ8 quantization, `epsilon=1.0`, `train_list_fraction=0.1`
- Single storage partition, 308 leaf centroids

## Tree Structure

The VTree has **3 levels** with **308 leaf centroids** across **50 pages**:

```
L0 (Root):     1 page  | 4 centroids -> points to 4 subtrees
L1 (Interior): 5 pages | 17 centroids across 4 clusters
L2 (Leaf):     44 pages| 308 centroids across 17 clusters
Flat copy:     35 pages| 308 centroids in a single-level chain (starts at page 50)
```

### Cluster Distribution

**L0 -> L1**: The 4 root centroids each point to a L1 cluster:

| L1 Cluster | Children (leaf clusters) |
|------------|------------------------|
| Cluster 0  | 3 leaf clusters        |
| Cluster 1  | 1 leaf cluster         |
| Cluster 2  | 12 leaf clusters       |
| Cluster 3  | 1 leaf cluster         |

**Highly skewed**: Cluster 2 owns 12 of 17 leaf clusters (~70% of subtree), while clusters 1 and 3 own just 1 each.

**L1 -> L2**: The 17 leaf clusters have widely varying sizes:

```
[27, 4, 1, 30, 34, 1, 1, 1, 13, 5, 43, 10, 36, 46, 12, 22, 22]
```

- Smallest: 1 centroid
- Largest: 46 centroids
- Median: ~13 centroids
- 46x imbalance between smallest and largest cluster

This skewness means tree pruning decisions at L0/L1 have outsized impact on recall.

## Query Analysis (epsilon=0.3, min_probe_fraction=0.3)

### Flat Navigation

```
L0(LEAF): 308 centroids evaluated, 35 page pins
-> 111 candidates after epsilon filter, nprobe=33
```

Flat scans all 308 centroids in one pass:
- Root is a leaf page + 34 overflow pages = 35 page pins
- 308 distance computations (one per centroid)
- Epsilon post-filter (`closestDistance + 0.3`) retains 111 of 308 candidates
- `nprobe = floor(111 * 0.3) = 33` clusters actually probed for data

No centroids are missed. Recall is limited only by how many of the 111 candidates are probed.

### Hierarchical Tree Navigation

```
L0(INTERIOR): 4 evaluated, 2 pass epsilon  ->  4 distComps,   1 page pin
L1(INTERIOR): 15 evaluated, 10 pass epsilon -> 15 distComps,  3 page pins
L2(LEAF):     247 collected from 10 clusters-> 247 distComps, 32 page pins
                                               ─────────────  ────────────
Total:                                         266 distComps, 36 page pins
-> 90 candidates after epsilon filter, nprobe=27
```

**L0 (Root)**: Evaluates all 4 root centroids. The query vector is within `closestDistance + 0.3` of 2 centroids. The other 2 subtrees are pruned — their entire contents (7 leaf clusters, 61 centroids) become unreachable.

**L1 (Interior)**: Within the 2 explored subtrees, 15 interior centroids are evaluated across 2 pages (+ 1 overflow page). 10 of 15 pass the epsilon threshold, pointing to 10 of 17 leaf clusters.

**L2 (Leaf)**: Collects 247 centroids from 10 leaf clusters across 32 page pins (10 cluster root pages + 22 overflow pages). The remaining 7 leaf clusters with 61 centroids are never visited.

After global epsilon filter: 90 of 247 candidates survive. `nprobe = floor(90 * 0.3) = 27`.

## Comparison

| Metric | Flat | Tree (epsilon=0.3) | Difference |
|--------|------|--------------------|------------|
| Distance computations | 308 | 266 | Tree saves 42 (14%) |
| Page pins | 35 | 36 | Nearly equal |
| Leaf centroids reachable | 308 | 247 | Tree misses 61 (20%) |
| Candidates after filter | 111 | 90 | Tree has 21 fewer |
| Nprobe (clusters probed) | 33 | 27 | Tree probes 6 fewer |

### Where Distance Computations Come From (Tree)

```
             distComps  % of total
L0 interior:     4         1.5%    (root centroid evaluation)
L1 interior:    15         5.6%    (interior centroid evaluation)
L2 leaf:       247        92.9%    (leaf centroid collection)
             ─────       ─────
Total:         266       100.0%
```

Interior-level overhead is small (7.1%). The vast majority of work is at the leaf level. The tree's savings come from reaching fewer leaf pages (10 vs 17 clusters), not from cheap interior navigation.

### Where Page Pins Come From (Tree)

```
              pagePins  % of total
L0 interior:     1         2.8%    (1 root page)
L1 interior:     3         8.3%    (2 cluster pages + 1 overflow)
L2 leaf:        32        88.9%    (10 cluster pages + 22 overflow)
              ─────      ─────
Total:          36       100.0%
```

Similar story: leaf overflow pages dominate. The 3 extra interior page pins (vs flat's 0) are offset by reaching fewer leaf pages, resulting in nearly equal total page pins.

## Key Takeaway

With 308 leaf centroids and a skewed 3-level tree:
- **Interior navigation overhead is negligible** (~7% of distComps, ~11% of page pins)
- **Tree's pruning saves 14% of distance computations** but misses 20% of leaf centroids
- **Page pin cost is nearly identical** because leaf overflow pages dominate both approaches
- **Tree recall loss comes from L0 pruning**: 2 of 4 subtrees are cut, losing access to 61 centroids that may contain relevant results
- The **skewed cluster distribution** amplifies pruning errors — a bad L0 decision can cut off the large cluster (12 children, ~253 centroids)
- For this moderate cluster count (~300), flat is preferred. Tree pruning would provide more benefit with thousands of centroids where full scans become expensive.
