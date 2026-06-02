# Top-Down Hierarchical K-Means (VTree Index Build)

This document explains the **top-down** hierarchical clustering path added alongside the existing **bottom-up** algorithm in the VTree static-structure build pipeline. It is written for the next agent or developer picking up this work.

## Branch

- **Feature branch:** `gerrit-build-top-down` (forked from `gerrit-build`)
- **Legacy path:** still available via `WITH {"top_down":"false"}` on the index DDL

## Problem / Motivation

The original **bottom-up** build in `HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor`:

1. Materializes all sampled vectors to a run file.
2. Runs k-means|| on raw data to get `K` leaf centroids.
3. Repeatedly clusters **centroids** (not raw points) upward with √K reduction until centroids fit in one frame.
4. Emits the tree in **BFS root-first** order even though levels were built bottom-up.

The new **top-down** approach (Approach 2) instead:

1. Computes the **page-fit fan-out** from the index's real on-disk geometry: **`P`** = routing centroids that fit on one interior page (root), and **`K = P·(1+V)`** = interior fan-out (primary page + `V` overflow pages, `V = 100`).
2. Clusters the full sample into `P` root groups with **k-means++ then FSCL**, splits into **one run file per root cluster**.
3. For each level, clusters every parent run file into exactly `K` children (k-means++ + FSCL), splits into child run files, and repeats until cumulative cluster count ≥ `num_clusters` (target `K_target`) **or** the strict height cap (`TOPDOWN_MAX_LEVEL = 4`, i.e. levels 0..4).
4. Deletes intermediate run files after each level (parent files consumed; try/finally on failure).
5. Emits centroids **level 0 = root → increasing toward leaves**, with explicit parent links known at build time.

**FSCL (Frequency-Sensitive Competitive Learning)** replaces Lloyd refinement: each point is assigned by a balancing penalty `score_i = distance(x, c_i) · exp(γ · (f_i / μ))` where `f_i` is the running headcount of cluster `i` and `μ = recordCount / k` is the fair share (`γ = 3.0`, `I = 20` epochs). The penalty pushes cluster sizes toward `μ`, producing height-balanced nodes. The distance is the operator's configured metric (euclidean for L2, angular for cosine).

This matches the product idea: predictable depth, page-fit branching, and parent-child relationships natural because clustering is top-down. FSCL is expected to keep every cluster populated; an empty cluster or a degenerate (early-leaf) split **throws**, and a run file with fewer records than `K` **stops the build** and returns the height-balanced tree built so far.

## Files Touched

| File | Role |
|------|------|
| `asterixdb/asterix-runtime/.../HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor.java` | Top-down algorithm, strategy flag, output |
| `asterixdb/asterix-metadata/.../SecondaryVectorOperationsHelper.java` | Reads `top_down` from WITH clause, passes flag to operator |
| `asterixdb/asterix-runtime/.../VCTreeStaticStructureCreatorOperatorDescriptor.java` | **Unchanged** — consumes same 4-field output |

## Configuration (DDL / WITH clause)

```sql
CREATE INDEX ... TYPE vtree
WITH {
  "num_clusters": 1000,      -- target leaf centroid count (K); stop when cumulative >= this
  "dimension": 384,          -- optional; used for P/K sizing if set, else inferred from first vector
  "similarity": "euclidean", -- distance metric (same as before)
  "top_down": "true"         -- default true; set "false" for legacy bottom-up
};
```

### Key parameters

| Parameter | Meaning |
|-----------|---------|
| `num_clusters` | Target **K_target** — stop at the **first level** whose total centroid count ≥ target (overshoot allowed) |
| `P` | **Not a DDL field** — root fan-out, computed at runtime as routing centroids fitting one interior **disk page** |
| `K` | **Not a DDL field** — interior fan-out `P·(1+V)`, `V = 100` (constant) |
| `top_down` | `"true"` (default) = new path; `"false"` = legacy bottom-up |

### P / K formula (Approach 2)

Computed from the index's real interior frame and buffer-cache page size (so build-time fan-out matches the static-structure layout exactly):

```
perEntry  = interiorFrame.getBytesRequiredToWriteTuple([centroidId:int, embedding:double[D], childPageId:int])  // includes slot
header    = interiorFrame.getPageHeaderSize()
P = max(2, floor((pageSize - header) / perEntry))
K = P * (1 + V)        // V = 100
```

`P` is the fan-out at the root (level 0); `K` is the fan-out at every interior level. Constants live in the operator: `TOPDOWN_V_OVERFLOW_PAGES = 100`, `TOPDOWN_FSCL_GAMMA = 3.0`, `TOPDOWN_FSCL_EPOCHS = 20`, `TOPDOWN_MAX_LEVEL = 4`.

## Operator Architecture (unchanged two-activity model)

```
Activity 1: StoreCentroidsActivity
  └─ MaterializerTaskState (sampleUUID) — append all input frames to run file
  └─ TupleCountState — count tuples for indexing

Activity 2: FindCandidatesActivity  [blocking edge from Activity 1]
  └─ if topDown → buildTopDownHierarchicalKMeans(...)
     else       → performMemoryEfficientHierarchicalKMeans(...)
  └─ emit via outputTopDownStructure(...) or outputHierarchicalStructure(...)
```

The **sample run file** (`sampleUUID`) is owned by `MaterializerTaskState` and is **not** deleted by top-down code. Only **intermediate per-cluster** run files created during the build are deleted.

## Top-Down Algorithm (step by step)

```
1. resolve D (config or probe first vector)
2. [P, K] = computeFanOut(ctx, partition, D)   // P=computeP(...), K=computeK(P, V)
3. Root: clusterRunFile(sample, kRoot=min(P, total)) with k-means++ + FSCL
4. Throw if root produced < kRoot centroids (degenerate/early leaf)
5. Store level 0 centroids (parentClusterId = -1)
6. If cumulative >= num_clusters OR TOPDOWN_MAX_LEVEL <= 0 → done (root is leaf level)
7. Split sample into P child run files (splitRunFileByAssignment)
8. Loop level = 1..TOPDOWN_MAX_LEVEL:
     Feasibility: if ANY parent run file recordCount < K → stop build, keep tree so far
     For each parent run file p (ascending parent index):
       clusterRunFile(p, K) → child centroids (k-means++ + FSCL)
       throw if it produced < K centroids (degenerate/early leaf)
       child.parentClusterId = parent index p
     cumulative = total child centroids this level
     Stop if cumulative >= num_clusters OR level >= TOPDOWN_MAX_LEVEL
     else split each parent into K child run files; deleteRunFiles(parent files)
9. emit outputTopDownStructure (root level 0 first)
```

### Example (P = 10, K = 1010, num_clusters = 1000, large sample)

| Level | Clusters | Notes |
|-------|----------|-------|
| 0 (root) | 10 | P, from full sample |
| 1 | 10100 | 10 × K → stop (≥ num_clusters, overshoot kept) |

With `V = 100`, interior fan-out `K` is large, so trees are typically shallow (root + one interior level). A run file that cannot supply `K` records stops the build, leaving the previous level as the (height-balanced) leaf level.

## New Methods (FindCandidatesActivity inner class)

| Method | Purpose |
|--------|---------|
| `RunFileSource` | Functional interface: `openReader()` for multi-pass streaming |
| `SplitResult` | Holds child `RunFileWriter` list + per-cluster record counts |
| `streamUpdateMinDist(...)` | D² update for k-means++ seeding over a run file |
| `clusterRunFile(...)` | k-means++ init + streaming **FSCL** on **one** run file; throws on empty cluster |
| `splitRunFileByAssignment(...)` | Assign each record to nearest centroid; write to per-cluster run files |
| `deleteRunFiles(...)` | Best-effort `FileReference.delete()` cleanup |
| `computeFanOut(ctx, partition, dim)` | Open LSMVTree, size a routing entry exactly, return `{P, K}` |
| `buildTopDownHierarchicalKMeans(...)` | Level-by-level orchestrator (P root, K interior, height cap, feasibility stop) |

Static helpers on `HierarchicalClusterStructure`:

| Method | Purpose |
|--------|---------|
| `computeP(pageSize, interiorHeaderSize, perEntryBytes)` | Compute root fan-out P |
| `computeK(p, v)` | Compute interior fan-out `P·(1+V)` |
| `outputTopDownStructure(...)` | Emit level 0 → maxLevel with global centroid ids |

## Downstream Contract (must preserve)

Output tuple format is **unchanged** (4 fields):

```
<treeLevel, centroidId, parentClusterId, embedding>
```

- `treeLevel`: 0 = root, increases toward leaves; **maxLevel = leaf** (quantized in `VCTreeStaticStructureCreator`)
- `centroidId`: global id in emission order
- `parentClusterId`: **parent's position index within the previous level's emission order** (not global id). Downstream groups children by this key and sorts ascending to match parent order.
- `embedding`: `AOrderedList<DOUBLE>`

Consumer: `VCTreeStaticStructureCreatorOperatorDescriptor` — reads all tuples, derives `clustersPerLevel` / `centroidsPerCluster` from `parentClusterId` grouping.

**Weights** (per-cluster record counts from split) are computed internally but **not** emitted — adding them would require a 5th field and downstream changes.

## Bottom-Up vs Top-Down Comparison

| Aspect | Bottom-up (legacy) | Top-down (new, default) |
|--------|-------------------|-------------------------|
| First clustering | k-means|| on all raw data → K leaves | k-means++ + FSCL on sample → P root |
| Upper levels | Cluster centroids in memory | Cluster each parent's run file separately (K) |
| Refinement | Lloyd | FSCL (balancing penalty) |
| Fan-out | √K reduction toward root | P at root, K = P·(1+V) at interior |
| Stop condition | Next level fits in one frame | cumulative ≥ num_clusters OR height cap (level 4); also stop if any parent < K |
| Level numbering in structure | Internal negative/positive levels, BFS emit | Level 0 = root, emit ascending |
| Run files | Single sample file, multi-pass | Sample + per-cluster files per level (deleted after use) |
| Parent links | From Lloyd assignments on centroids | Known when storing each level (parent index) |

## K-Means Details Per File

**All levels (root + interior):** `clusterRunFile` — k-means++ seeding (in-memory minDist array) followed by streaming **FSCL** refinement. Root uses `k = min(P, totalTupleCount)`; interior levels use `k = K`. The legacy k-means|| path (`performInitialKMeansPlusPlus`) is used **only** by the bottom-up algorithm now.

**FSCL:** per epoch, stream the file tracking running headcount `f_i`; assign each point to `argmin_i distance(x,c_i)·exp(γ·(f_i/μ))` with `μ = recordCount/k`, `γ = 3.0`; recompute centroids from assigned sums; up to `I = 20` epochs or until convergence (`1e-4`). Throws if any cluster ends an epoch empty.

**Split:** single assignment pass after centroids are finalized; copies raw tuples (secondaryRecDesc layout) into child `RunFileWriter`s.

## Known Limitations / Follow-ups

1. **No unit/integration tests yet** for top-down path — add tests with small synthetic run files.
2. **Root and interior both use k-means++ + FSCL** (`clusterRunFile`); root on the full sample may be heavier than the legacy k-means|| seeding for very large samples.
3. **Tiny branches:** if any parent run file has `recordCount < K`, the build stops and returns the height-balanced tree built so far; a degenerate split (fewer centroids than requested) or an empty FSCL cluster throws.
4. **Per-partition independence:** each Hyracks partition builds its own tree (same as before).
5. **Optional:** expose `V` / `P` / `K` or the FSCL `γ`, `I`, and height cap in the WITH clause instead of operator constants.
6. **Optional:** emit cluster weights (record counts) as 5th output field if downstream needs them.
7. **Compile verification:** run Maven on `asterix-runtime` and `asterix-metadata` modules after changes.

## Quick Code Navigation

```
HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor.java
├── boolean topDown                          // constructor flag
├── HierarchicalClusterStructure
│   ├── computeP() / computeK()
│   └── outputTopDownStructure()
└── FindCandidatesActivity
    ├── initialize()                         // branches on topDown
    ├── computeFanOut()                      // {P, K} from LSMVTree page geometry
    ├── buildTopDownHierarchicalKMeans()     // orchestrator
    ├── clusterRunFile()                     // k-means++ + FSCL
    ├── splitRunFileByAssignment()
    └── performMemoryEfficientHierarchicalKMeans()  // legacy

SecondaryVectorOperationsHelper.java
└── createStaticStructureJob()               // topDown from WITH clause
```

## Suggested Next Steps for Agents

1. Run/build index creation with `top_down: true` and verify `VCTreeStaticStructureCreator` accepts output.
2. Compare tree depth and centroid counts vs `num_clusters` for various dimensions/frame sizes.
3. Add regression test or logging for P/K, level counts, and cumulative stop.
4. Profile disk use with many levels (intermediate run file churn).
5. If Gerrit review requests bottom-up as default, flip default in `SecondaryVectorOperationsHelper` only.
