---
name: project-bottom-up-static-structure
description: "VTree static structure is built bottom-up (append-only) — leaves at low page ids, root at the highest. Centroid IDs preserve BFS-from-root numbering."
metadata: 
  node_type: memory
  type: project
  originSessionId: e8da6b5b-eb59-469f-9d56-05ade6986d72
---

VTreeStaticStructureBuilder is bottom-up streaming as of 2026-06-11 on `experiment/bottom-up-static-structure`. Tuples arrive leaf-first (level numLevels-1) and walk up to the root (level 0). Each page is written to disk as soon as its forward pointers are known (overflow chain within a cluster, sibling chain across leaf clusters via setNextLeaf, child pointers for interior tuples), so the builder keeps at most one confiscated page at a time — no in-memory TreeMap of all pages, no end-of-build patching pass.

Page id layout: leaves at low ids (0, 1, 2, …), interior in the middle, root at the highest. Allocated via `freePageManager.takePage(metaFrame)` in arrival order; no upfront `totalClusters` pre-allocation.

**Centroid ID convention is Option I (BFS-from-root, unchanged from top-down era):** root gets ids 0..N_root-1, leaves keep the highest ids, `firstLeafCentroidId` is non-zero. To preserve this despite bottom-up emission, `HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor.outputHierarchicalStructure()` pre-computes `idOffset[L]` and starts each emitted level's counter at that offset. Test fixture `VectorTestStructure.buildCentroidTuples()` iterates its internal BFS-from-root storage in reverse level order to feed the builder bottom-up — internal storage and PK strings (`pk_c_10_*`) are unchanged.

**Leaf metadata pointer = -1 sentinel.** The builder doesn't try to predict directory page ids. `VTreeBulkLoader.end()` overwrites leaf metadata pointers per-cluster (existing centroidId-based logic at the leaf-pointer-overwrite loop). For the memory component path, the sentinel is fine because the static structure's leaf pointer is not used directly — search uses `centroidDirPageMap` for memory components.

**Root translation in VTreeBulkLoader:** captures `vtree.getRootPageId()` in constructor as `staticStructureRootPage`. At `end()`, the data component's root is `staticBasePageId + staticStructureRootPage` (not `staticBasePageId` — that was the old "root at page 0" assumption).

**Consumers that still work unchanged:**
- `VTree.staticRootPage` is set from `freePageManager.getRootPageId()` on activate, which my builder set to the real root.
- `VTreeSearchCursor` uses `!= 0` as "uninitialized" sentinel — bottom-up root ids are always ≥ 1 so the check stays valid.
- `buildLocalCentroidDirPageMap()` BFS from root finds all leaves via interior child pointers (not via leaf sibling chain), works in both orientations.

**Verified 2026-06-11:** 17/17 unit tests pass; integration parts 1-4 (no-Gist) match baseline (13 PASS + 1 WARN + 0 FAIL — recall variance moved between glove_cosine and movie_esq_filter but both within typical fluctuation).

**Files:** `VTreeStaticStructureBuilder.java`, `VTreeBulkLoader.java`, `HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor.java`, test fixture `VectorTestStructure.java`. Branch `experiment/bottom-up-static-structure` tagged `pre-bottom-up-baseline` for top-down baseline rollback.

Related: [[project_dual_nav_experiment]] benefits — bottom-up layout makes append-only feasible, which is a prereq for the dual-root experiment's two-tree-in-one-file layout.
