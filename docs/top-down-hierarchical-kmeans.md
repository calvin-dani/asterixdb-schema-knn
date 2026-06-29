# BKT-Style Top-Down Hierarchical Clustering (VTree Index Build)

This document explains the **top-down** hierarchical clustering path in the VTree static-structure build pipeline. The legacy **bottom-up** path remains available via `WITH {"top_down":"false"}`.

## Top-Down Approach (BKT-style, SPANN-inspired)

1. Computes **leaf page capacity** from the index's real leaf frame and quantized routing tuple size (SQ8/SQ4 from DDL `quantization`).
2. **Dynamic fan-out** per node: `dynamicK = min(N / leafPageCapacity + 1, 32)` (max 32 children).
3. **Leaf stop:** when a bag has `N <= leafPageCapacity`, emit each record as its own real-pivot centroid (no further split). Before `maxLevel`, each childless centroid is **promoted** to the next level as a 1-vector batch (itself) so routing height stays aligned with the static-structure builder’s Nth-centroid→cluster-N rule.
4. **Lambda-balanced k-means** on a sample (≤1000 records): `score = distance + λ · priorCount`; λ auto-tuned once per partition (or fixed via SET).
5. **Full-data assign** with λ=0, then **real pivots** = nearest actual record per cluster (not arithmetic means).
6. Repeats level-by-level until cumulative centroid count ≥ `num_clusters` or `maxLevel` cap.
7. Emits centroids **level 0 = root → leaves** with explicit `parentClusterId` links.

## Files Touched

| File | Role |
|------|------|
| `asterixdb/asterix-runtime/.../HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor.java` | BKT top-down algorithm |
| `asterixdb/asterix-metadata/.../SecondaryVectorOperationsHelper.java` | Passes quantization bits + lambda tuning |
| `asterixdb/asterix-runtime/.../VTreeStaticStructureCreatorOperatorDescriptor.java` | **Unchanged** — consumes same 4-field output |

## Configuration (DDL / WITH clause)

```sql
CREATE INDEX ... TYPE vtree
WITH {
  "num_clusters": 1000,
  "dimension": 384,
  "similarity": "euclidean",
  "quantization": "SQ8",
  "top_down": "true"
};
```

### Key parameters

| Parameter | Meaning |
|-----------|---------|
| `num_clusters` | Stop at first level whose centroid count ≥ target |
| `quantization` | `SQ8` or `SQ4` — used to size leaf page capacity |
| `top_down` | `"true"` (default) = BKT top-down; `"false"` = legacy bottom-up |

### Build constants (operator)

| Constant | Default | Meaning |
|----------|---------|---------|
| `BKT_KMEANS_K` | 32 | Max children per split |
| `BKT_SAMPLES` | 1000 | Per-node optimization sample cap |
| `DEFAULT_TOPDOWN_MAX_LEVEL` | 4 | Deepest level index (levels 0..4) |
| `DEFAULT_TOPDOWN_LAMBDA_FACTOR` | -1 | Auto-tune λ; set >0 to fix |

### Leaf page capacity

```
perEntry = leafFrame.getBytesRequiredToWriteTuple([centroidId, embedding, quantizedBytes, childPageId])
leafPageCapacity = max(1, floor((pageSize - leafHeader) / perEntry))
```

## Algorithm (step by step)

```
1. resolve D (config or probe first vector)
2. leafPageCapacity = computeLeafPageCapacity(dim, quantizationBits)
3. lambdaFactor = auto-tune on sample OR fixed from SET
4. Root:
     if total <= leafPageCapacity → emit each record as level-0 centroids;
       if maxLevel > 0 → promote each centroid as a 1-vector batch and continue at level 1
       else stop
     else clusterRunFile(dynamicK(total)) → split sample → register level-0 centroids only for non-empty buckets
5. Child run files from root split (skipped when root was a promoted leaf bag)
6. Loop level = 1..maxLevel:
     For each parent batch:
       if recCount <= leafPageCapacity → emit each record as centroids at this level;
         if level < maxLevel → enqueue each centroid as a 1-vector promotion batch (parentClusterId = local id)
       else clusterRunFile(dynamicK(recCount)) → split → register centroids and child batches only for non-empty buckets
     Stop if level centroid count >= num_clusters OR level >= maxLevel
     At level == maxLevel, leaf-bag centroids are terminal (no promotion)
7. assign centroid ids by scanning levels 0..maxLevel (same as top-down), then emit **`outputBottomUpForStaticStructure`** (leaf level first, root last) for `VTreeStaticStructureBuilder`
```

### Empty k-means buckets

λ-balanced k-means may return `k` centroids while full-data assignment leaves some buckets empty (`min=0` in `[TopDown] BKT full assign` logs). BuildHead **does not emit routing centroids** for empty buckets — the same policy as scratch BKT SelectHead. This keeps the invariant required by `VTreeStaticStructureBuilder`: **total centroids at level L = cluster count at level L+1** (the Nth centroid at L points to the Nth cluster at L+1).

K-means optimization and assignment are unchanged; only empty-bucket pivots are omitted from the routing tree.

## Output contract (unchanged)

```
<treeLevel, centroidId, parentClusterId, embedding>
```

- `parentClusterId`: parent's **local index** among emitted centroids at the previous level (contiguous after empty-bucket filtering), not the raw k-means bucket index
- Centroid embeddings are **real sample vectors** (pivot records)
- Tuple stream order for the static-structure build is **leaf-first** (levels `maxLevel` → `0`); the 4-field wire format is unchanged

## Session SET parameters

See `appendix_2_parameters.md`:
- `compiler.vector.topdown.lambdaFactor` — optional fixed λ factor (omit for auto-tune)
- `compiler.vector.topdown.maxlevel` — height cap

Deprecated (no effect on BKT build): `compiler.vector.topdown.v`, `compiler.vector.topdown.gamma`

## SPANN SelectHead + BuildHead (default)

By default, the static-structure build runs **SelectHead** then **BuildHead** (SPANN Part 1–2) with BKT head selection. The routing structure is built **only from selected head vectors**, not the full training sample.

```
sample run file → scratch BKT → SelectHead walk → HeadSelectionTaskState
head indices    → materializeHeadRunFile → buildTopDownHierarchicalKMeans (|H| only)
              → 4-field hierarchical tuples → VTreeStaticStructureCreator
```

- Head indices are **real sample record indices** (pivots), stored in `HeadSelectionTaskState`.
- **BuildHead** re-tunes λ on |H| (separate from SelectHead scratch-BKT λ when auto-tune is enabled).
- With SelectHead enabled (default), **`num_clusters` is ignored** for BuildHead structure stop.
- **BuildHead split fan-out:** fixed `K = min(32, segmentSize)` (SPANN `dynamicK=false`), not `dynamicK`.
- **BuildHead global stop:** after each level, stop when **no pending bucket** has `recCount > leafPageCapacity` (`stopReason: all buckets fit one leaf page`). Promotion may still run on the last productive level; the loop does not continue to `maxLevel` when every bucket already fits one leaf page.
- **`maxLevel`** remains a **safety cap** if some buckets never shrink below `leafPageCapacity`.
- Set `compiler.vector.selecthead.enabled` to `false` to use the full-sample top-down path (`num_clusters` + `dynamicK` apply).

### SelectHead SET parameters

Use `SET` **before** `CREATE INDEX` in the same request (see `appendix_2_parameters.md`):

| SET key | Default | Meaning |
|---------|---------|---------|
| `compiler.vector.selecthead.enabled` | `true` | Run SelectHead + BuildHead (BKT routing from heads) |
| `compiler.vector.selecthead.headRatio` | `0.15` | Target \|H\| ≈ ratio × sampleCount |
| `compiler.vector.selecthead.headCount` | unset | If set, overrides ratio |
| `compiler.vector.selecthead.selectType` | `bkt` | `bkt` or `random` |

Example:

```sql
SET `compiler.vector.selecthead.headRatio` "0.2";

CREATE INDEX vecIdx ON myDataset(embedding) TYPE vctree
WITH {
  "dimension": 384,
  "similarity": "euclidean",
  "num_clusters": 1000,
  "quantization": "SQ8"
};
```

### Scratch BKT vs BuildHead tree

| | Scratch BKT (SelectHead) | BuildHead (default) |
|--|--------------------------|---------------------|
| Input | Full sample N | Selected heads \|H\| only |
| Purpose | Temporary partition for head picking | Routing centroids for VTree |
| Split fan-out | `dynamicK(N, leafPageCapacity)` | **Fixed `K = min(32, n)`** |
| Per-bucket split stop | `N <= leafPageCapacity` | `recCount <= leafPageCapacity` |
| Global stop | Stack empty (recursive build) | No bucket needs split (`recCount > leafPageCapacity`); **`maxLevel` safety fallback** |
| Output | In-memory tree, discarded after walk | 4-field hierarchical tuples downstream |
