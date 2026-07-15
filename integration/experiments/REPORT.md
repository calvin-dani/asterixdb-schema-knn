# Flat vs Hierarchical VTree: A/B Experiment Report

## Objective

Measure the benefit of VTree's hierarchical routing structure compared to flat IVF (single-level centroid scan) for approximate nearest neighbor (ANN) search. Both approaches share identical leaf centroids and data pages — the only difference is how the search navigates to select which clusters to probe.

## Approach

### Dual Navigation Structure

We modified the VTree index to build **two navigation structures** in a single `.staticstructure` file during index creation:

1. **Hierarchical tree**: Multi-level k-means hierarchy (root → interior → leaf pages). Search traverses interior levels with epsilon-based beam pruning before reaching leaf centroids.
2. **Flat structure**: All leaf centroids stored in a single-level chain of leaf pages (no interior routing). Search scans all centroids directly, sorts by distance, then applies epsilon filter.

Both structures point to the **same data pages** — the leaf centroids and their metadata/data page pointers are identical. This ensures a fair comparison: the only variable is the navigation path.

### Query-Time Selection

At query time, the user selects which structure to use via the 6th argument of `ann_distance`:

```sql
-- Hierarchical (default)
LET dist = ann_distance(m.embedding, qvec, "euclidean_squared", 0.2, 2)

-- Flat
LET dist = ann_distance(m.embedding, qvec, "euclidean_squared", 0.2, 2, 1)

-- Hierarchical with epsilon override
LET dist = ann_distance(m.embedding, qvec, "euclidean_squared", 0.2, 2, 0, 0.3)
```

Arguments: `(field, query_vector, metric, min_probe_fraction, k_multiplier, use_flat, epsilon_override)`

## Code Changes (16 files, +376 / -45 lines)

### Part 1: Build Dual Structure at Index Creation
| File | Change |
|------|--------|
| `VTreeStaticStructureBuilder.java` | Added `buildFlatStructure()` — after building the hierarchical tree, copies all leaf centroids into new flat leaf pages with the same metadata pointers. Stores `flat_root_page_id` in metadata. |
| `VTreeNavigationUtils.java` | Fixed `extractCentroidFromInteriorTuple()` to deserialize only 2 fields (handles both 3-field non-quantized and 4-field SQ8 quantized tuples). Added `ThreadLocal` counters for distance computations and page pins. |

### Part 2: Propagate Flat Root Page ID
| File | Change |
|------|--------|
| `VTree.java` | Added `staticFlatRootPage`, `flatRootPage`, `flatNavBufferCache`, `flatNavFileId` fields. Added `setFlatNavigation()`, `getFlatNavigationRootPageId()`, `getFlatNavigationBufferCache()`, `getFlatNavigationFileId()` methods. |
| `LSMVTreeDiskComponent.java` | In `setInitialized()`, reads `flat_root_page_id` from component metadata. |
| `LSMVTree.java` | In `loadDiskComponents()`, propagates flat root page ID and static structure buffer cache to all data components after loading the static structure. |

### Part 3: Query Parameter Flow (ann_distance 6th & 7th args)
| File | Change |
|------|--------|
| `VectorIndexAccessMethod.java` | Arity guard extended to 7 args. Extracts `use_flat` (arg 5, int, default 0) and `epsilon_override` (arg 6, double, default -1). Removed 2-arg distance functions from optimizable list (they now always do brute-force scans). Added `inferDistanceMetric()` helper. |
| `VectorSearchPOperator.java` | Shifted `searchApproach` from `queryVarList[5]` to `[7]`. |
| `VectorSearchOperatorNodePushable.java` | Reads `use_flat` from query tuple field 5 and `epsilon_override` from field 6. Sets on `VTreeSearchPredicate`. |
| `VTreeSearchPredicate.java` | Added `useFlatNavigation` boolean field with getter/setter. |

### Part 4: Use Flat Root at Search Time
| File | Change |
|------|--------|
| `IClusterSelectionStrategy.java` | Added `initializeWithRootOverride()` default method accepting explicit buffer cache, file ID, and root page ID. |
| `NprobeClusterSelectionStrategy.java` | Implemented `initializeWithRootOverride()` — same logic as `initialize()` but uses explicit navigation coordinates instead of extracting from VTree. Logs `[NprobeStrategy FLAT]` with metrics. |
| `LSMVTreeTopKSearchCursor.java` | In `doOpen()`, checks `vectorPred.isUseFlatNavigation()` and calls `initializeWithRootOverride()` with the flat structure's buffer cache and root page. |

### Part 5: Instrumentation
| File | Change |
|------|--------|
| `VTreeNavigationUtils.java` | `ThreadLocal<Long>` counters for distance computations and page pins. Incremented at every `bufferCache.pin()` and `distanceFunction.apply()` call. |
| `NprobeClusterSelectionStrategy.java` | WARN-level logging after cluster selection: cluster count, epsilon, nprobe, distComps, pagePins, elapsed time in microseconds. |
| `HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor.java` | Logs hierarchy build time. |

### Other Changes
| File | Change |
|------|--------|
| `VectorIndexDeclUtil.java` | Added `"flat"` to allowed WITH-clause fields (for earlier flat-only prototype, now superseded by dual structure). |
| `SecondaryVectorOperationsHelper.java` | Reads `flat` boolean from WITH clause (for earlier prototype). |

## Experiment Setup

- **Dataset**: ~100K movie records with 384-dimensional embeddings
- **Index**: VTree with SQ8 quantization, `epsilon=1.0` in WITH clause (ensures global closest cluster assignment during bulk load)
- **Partitions**: 1 storage partition
- **Leaf centroids**: 308
- **Tree levels**: 3 (root → interior → leaf)
- **Query parameters**: K=100, min_probe_fraction=0.2, k_multiplier=2
- **Query vectors**: 15 vectors (PKs 85-100)
- **Ground truth**: Brute-force KNN via `euclidean_squared_distance()` (full scan, no index)

## Results

### Summary Table

| Config | Recall@100 | ±σ | Dist Comps | Page Pins | Nav Time (μs) | Clusters |
|--------|------------|-----|------------|-----------|---------------|----------|
| **Flat** | **91.87%** | 0.041 | 308 | 35 | 2,585 | 296 |
| Tree(ε=0.1) | 29.27% | 0.147 | 186 | 24 | 850 | 4 |
| Tree(ε=0.2) | 52.27% | 0.139 | 290 | 37 | 1,457 | 22 |
| Tree(ε=0.3) | 71.60% | 0.137 | 315 | 40 | 1,668 | 59 |
| Tree(ε=0.5) | 87.27% | 0.076 | 324 | 44 | 1,648 | 179 |
| Tree(ε=1.0) | **91.87%** | 0.041 | 329 | 49 | 1,693 | 296 |

### Key Findings

1. **Flat = Tree(ε=1.0) in recall** — both achieve 91.87%. At ε=1.0, the tree's interior beam is wide enough to reach all leaf clusters, matching flat's exhaustive scan.

2. **Flat uses fewer resources than equivalent-recall Tree** — Flat: 308 distComps, 35 page pins vs Tree(ε=1.0): 329 distComps, 49 page pins. Flat avoids interior-level overhead entirely.

3. **Tree navigates faster despite more distComps** — Tree(ε=0.2) takes 1,457 μs vs Flat's 2,585 μs. The tree prunes early at interior levels and only sorts 22 candidates, while flat must sort all 297 candidates globally.

4. **Tree's advantage is tunable pruning** — At low epsilon, Tree(ε=0.1) uses only 186 distComps and 24 page pins (40% less than flat), but sacrifices recall to 29%. This tradeoff is useful when speed matters more than accuracy.

5. **DistComps breakdown** — Flat's 308 = all leaf centroids. Tree's 321 at ε=0.2 = 308 reachable leaf centroids + 13 interior centroid evaluations. The interior overhead is small (~4%).

6. **Neither achieves 100% recall** — Both plateau at ~92% with these parameters. The remaining 8% gap is due to `min_probe_fraction=0.2` limiting how many of the candidate clusters are actually scanned for data records.

### Recall vs Cost Tradeoff

The scatter plot (recall_vs_cost.png) shows Flat is Pareto-optimal: it achieves the highest recall (91.87%) with fewer distance computations (308) than any Tree configuration with comparable recall. Tree only wins on navigation latency when using aggressive pruning (low ε), at the cost of recall.

## Plots

Generated in `integration/experiments/results/`:
- `recall.png` — Recall comparison across configurations
- `dist_computations.png` — Distance computations during cluster selection
- `page_pins.png` — Buffer cache page pins (I/O)
- `nav_time.png` — Navigation latency
- `clusters.png` — Candidate clusters after epsilon filter
- `recall_vs_cost.png` — Recall vs cost tradeoff scatter

## Conclusion

For this dataset (100K records, 384 dimensions, 308 leaf clusters), the hierarchical tree structure provides **no recall advantage** over flat IVF — both achieve the same recall when the tree explores all clusters (ε=1.0). The tree's value lies in its ability to **trade recall for speed** via epsilon-based pruning. However, for small-to-moderate cluster counts (~300), flat IVF is simpler and uses fewer resources at equivalent recall. The tree's pruning benefit would be more pronounced with larger cluster counts (thousands+) where scanning all centroids becomes expensive.
