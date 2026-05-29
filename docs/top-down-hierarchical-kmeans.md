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

The new **top-down** approach instead:

1. Computes how many centroids fit in one Hyracks frame → **`r_cluster`** (root branching factor).
2. Clusters the full sample into `r_cluster` root groups, splits into **one run file per root cluster**.
3. For each level, clusters every parent run file into up to `r_cluster` children, splits into child run files, and repeats until cumulative cluster count ≥ `num_clusters` (target `K`).
4. Deletes intermediate run files after each level (parent files consumed; try/finally on failure).
5. Emits centroids **level 0 = root → increasing toward leaves**, with explicit parent links known at build time.

This matches the product idea: predictable depth from `num_clusters` and frame-fit branching, and parent-child relationships are natural because clustering is top-down.

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
  "dimension": 384,          -- optional; used for r_cluster if set, else inferred from first vector
  "similarity": "euclidean", -- distance metric (same as before)
  "top_down": "true"         -- default true; set "false" for legacy bottom-up
};
```

### Key parameters

| Parameter | Meaning |
|-----------|---------|
| `num_clusters` | Target **K** — stop at the **first level** whose total centroid count ≥ K (overshoot allowed, e.g. 1000 when target is 900) |
| `r_cluster` | **Not a DDL field** — computed at runtime as max centroids fitting one frame for DOUBLE embeddings |
| `top_down` | `"true"` (default) = new path; `"false"` = legacy bottom-up |

### r_cluster formula

Reuses existing tuple size estimate in `HierarchicalClusterStructure`:

```
tupleSize = 38 + 13 * dimension
perCentroid = tupleSize + 4   (tuple + offset slot)
r_cluster = max(2, floor((frameSize - 9) / perCentroid))
```

Same value is used as **k at every level** (root and each per-cluster file), capped by `min(r_cluster, recordCountInFile)`.

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
1. r_cluster = computeRootBranching(dim, frameSize)
2. Root: k-means|| on full sample → r_cluster centroids (performInitialKMeansPlusPlus)
3. Split sample into r_cluster child run files (splitRunFileByAssignment)
4. Store level 0 centroids (parentClusterId = -1)
5. If r_cluster >= K → done (root is leaf level)
6. Loop level = 1, 2, ...:
     For each parent run file p (in ascending parent index):
       kp = min(r_cluster, recordCount(p))
       clusterRunFile(p, kp) → child centroids
       (optional) split p into child run files for next level
       child.parentClusterId = parent index p
     cumulative = total child centroids this level
     Stop if cumulative >= K OR level cap (100) OR no growth
     deleteRunFiles(parent files)
7. emit outputTopDownStructure (root level 0 first)
```

### Example (r_cluster = 10, K = 1000)

| Level | Clusters | Notes |
|-------|----------|-------|
| 0 (root) | 10 | from full sample |
| 1 | 100 | 10 × 10 |
| 2 | 1000 | 100 × 10 → stop (≥ K) |

## New Methods (FindCandidatesActivity inner class)

| Method | Purpose |
|--------|---------|
| `RunFileSource` | Functional interface: `openReader()` for multi-pass streaming |
| `SplitResult` | Holds child `RunFileWriter` list + per-cluster record counts |
| `streamUpdateMinDist(...)` | D² update for k-means++ seeding over a run file |
| `clusterRunFile(...)` | k-means++ init + streaming Lloyd on **one** run file |
| `splitRunFileByAssignment(...)` | Assign each record to nearest centroid; write to per-cluster run files |
| `deleteRunFiles(...)` | Best-effort `FileReference.delete()` cleanup |
| `buildTopDownHierarchicalKMeans(...)` | Level-by-level orchestrator |

Static helpers on `HierarchicalClusterStructure`:

| Method | Purpose |
|--------|---------|
| `computeRootBranching(dim, frameSize)` | Compute r_cluster |
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
| First clustering | k-means|| on all raw data → K leaves | k-means|| on sample → r_cluster root |
| Upper levels | Cluster centroids in memory | Cluster each parent's run file separately |
| Stop condition | Next level fits in one frame | cumulative clusters ≥ num_clusters |
| Level numbering in structure | Internal negative/positive levels, BFS emit | Level 0 = root, emit ascending |
| Run files | Single sample file, multi-pass | Sample + per-cluster files per level (deleted after use) |
| Parent links | From Lloyd assignments on centroids | Known when storing each level (parent index) |

## K-Means Details Per File

**Root level:** uses existing `performInitialKMeansPlusPlus` (k-means|| + streaming Lloyd) — same as legacy level-0.

**Child levels:** `clusterRunFile` — k-means++ seeding (in-memory minDist array) + streaming Lloyd; no k-means|| (files are smaller).

**Split:** single assignment pass after centroids are finalized; copies raw tuples (secondaryRecDesc layout) into child `RunFileWriter`s.

## Known Limitations / Follow-ups

1. **No unit/integration tests yet** for top-down path — add tests with small synthetic run files.
2. **Root still uses k-means||** on full sample; child levels use simpler k-means++ — intentional for large root, may differ in quality vs bottom-up leaves.
3. **Empty / tiny branches:** `k = min(r_cluster, recordCount)`; empty parent files skipped; stop if no growth.
4. **Per-partition independence:** each Hyracks partition builds its own tree (same as before).
5. **Optional:** expose `r_cluster` or branching factor in WITH clause instead of frame-fit only.
6. **Optional:** emit cluster weights (record counts) as 5th output field if downstream needs them.
7. **Compile verification:** run Maven on `asterix-runtime` and `asterix-metadata` modules after changes.

## Quick Code Navigation

```
HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor.java
├── boolean topDown                          // constructor flag
├── HierarchicalClusterStructure
│   ├── computeRootBranching()
│   └── outputTopDownStructure()
└── FindCandidatesActivity
    ├── initialize()                         // branches on topDown
    ├── buildTopDownHierarchicalKMeans()     // orchestrator
    ├── clusterRunFile()
    ├── splitRunFileByAssignment()
    └── performMemoryEfficientHierarchicalKMeans()  // legacy

SecondaryVectorOperationsHelper.java
└── createStaticStructureJob()               // topDown from WITH clause
```

## Suggested Next Steps for Agents

1. Run/build index creation with `top_down: true` and verify `VCTreeStaticStructureCreator` accepts output.
2. Compare tree depth and centroid counts vs `num_clusters` for various dimensions/frame sizes.
3. Add regression test or logging for r_cluster, level counts, and cumulative stop.
4. Profile disk use with many levels (intermediate run file churn).
5. If Gerrit review requests bottom-up as default, flip default in `SecondaryVectorOperationsHelper` only.
