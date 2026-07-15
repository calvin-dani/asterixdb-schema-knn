---
name: bug-kmeans-leaf-level-dropped
description: "OPEN (2026-07-03) — hierarchical k-means drops the K trained leaf centroids whenever K>=4; index gets ~sqrt(K) leaf clusters, num_clusters not honored"
metadata: 
  node_type: memory
  type: project
  originSessionId: f0dbccc0-9888-49c9-9fe5-a679580772e1
---

**OPEN — documented, deliberately not fixed yet (goes through Gerrit as 3760 amendment).**

In `HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor#performMemoryEfficientHierarchicalKMeans`
(asterix-runtime): the K finest-level centroids are stored at `levelCentroids[0]` (~line 1582),
then the first hierarchy iteration calls `initializeParentLevel(currentLevel=0)` which
**overwrites key 0**, and `buildLevelFromAssignments(parentLevel=0, childLevel=-1)` re-stores
the true leaves at **key -1**. `outputHierarchicalStructure` iterates `L = 0..maxLevel`, so
key -1 never emits.

**Why:** producer/consumer off-by-one on the level-map keys.
**Impact:** for K>=4 the emitted "leaf" level is the first parent level (~sqrt(K) centroids) —
the built static structure has ~sqrt(K) leaf clusters, not `num_clusters`. K<=3 escapes
(hierarchy skipped). Your integration runs don't set num_clusters → K defaults to
`sqrt(cardinality/numPartitions)`, so e.g. glove-50k/4 partitions: ~111 requested → ~10
effective leaves per partition. **Past nprobe/epsilon/recall benchmarks may have measured a
much coarser tree than intended** — check `staticstructure_*` dumps (`num_leaf_centroids` in
the metadata page) before trusting old numbers.

**Found by:** `HierarchicalKMeansPlusPlusCentroidsOperatorDescriptorTest` structural-invariant
test (2026-07-03); verified empirically K=36 over 80 vectors → only 8 tuples emitted (6 mid +
2 roots). runtimets goldens are clustering-independent by design, so they pass despite it.

**Fix sketch:** emit the key -1 level as the true leaf level (or keep leaves at a stable key),
preserving bottom-up emission order + BFS-from-root centroid ids in
`outputHierarchicalStructure` (~lines 335-378); then tighten the unit test to assert K leaf
tuples. A pending task chip exists with full instructions. See
[[project-vtree-trainseed-and-runtimets]]. Related minor finds: `resetRunFileReader` leaks a
reader handle per pass; mixed local/global aggregate state prefers the local branch.
