---
name: Flat IVF vs VTree Comparison Plan
description: Planned feature to add flat=true WITH-clause parameter for comparing flat IVF index against hierarchical VTree, measuring tree topology benefits
type: project
originSessionId: 2ba8a838-37a4-467a-a51b-fbcd15376ba6
---
Plan to add a `flat` boolean parameter to CREATE VECTOR INDEX WITH clause, enabling A/B comparison between flat IVF (single-level, all centroids as leaves) and VTree (hierarchical tree with interior navigation levels).

**Why:** To empirically measure what advantage VTree's tree topology provides — CPU (distance computations), IO (page pins), build time, and recall.

**How to apply:** The implementation is minimal — a single `if (flat) return structure;` short-circuit in `HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor.performMemoryEfficientHierarchicalKMeans()` after Level 0 centroids are built. Everything downstream (static structure builder, bulk loader, search cursors) already handles a 1-level structure correctly. Plus instrumentation counters in `VTreeNavigationUtils` and `NprobeClusterSelectionStrategy` for metrics.

**Full plan file:** `/Users/hongyu/.claude/plans/logical-crunching-tome.md`

**Key files to modify:**
1. `asterixdb/asterix-lang-common/.../VectorIndexDeclUtil.java` — add "flat" parameter
2. `asterixdb/asterix-metadata/.../SecondaryVectorOperationsHelper.java` — pass flat through
3. `asterixdb/asterix-runtime/.../HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor.java` — skip hierarchy when flat
4. `hyracks-fullstack/.../vector/utils/VTreeNavigationUtils.java` — instrumentation counters
5. `hyracks-fullstack/.../lsm/vector/impls/NprobeClusterSelectionStrategy.java` — log metrics
