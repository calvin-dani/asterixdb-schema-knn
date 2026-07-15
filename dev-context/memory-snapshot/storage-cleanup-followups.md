---
name: storage-cleanup-followups
description: "Open work items left over after the 2026-06-02 VTree/storage cleanup pass — what was deferred, why, and what still needs doing."
metadata: 
  node_type: memory
  type: project
  originSessionId: 2655393a-a843-47af-ad80-f946bd38bbce
---

After several rounds of cleanup on `hyracks-storage-am-vtree` + `hyracks-storage-am-lsm-vtree` (commits not yet pushed), the following items are queued for follow-up rounds. Working tree is on `gerrit/vtree-runtime`; rollback anchor is git tag `safety/before-3754-amend`.

## Deferred by explicit user direction

- **Replace reflection in `VTree.search()`** with proper `IVTreeDistanceFunctionFactory` + `IVTreeQuantizerFactory` interfaces in the api/ module. Cannot be done without touching asterix-runtime (the factory object on the other side must `implements` the new interface). Queued for the asterix-runtime round.
- **Delete the dead `printStaticStructureBFS` chain** in `asterix-runtime/.../VTreeStaticStructureCreatorOperatorDescriptor.java` (~170 lines, zero callers). Currently keeps `VectorUtils` alive as its single non-test consumer. Same round as above.
- **Push the accumulated cleanup**: ~80 files modified in working tree across both 3754 patches and 3771; nothing committed since the original 3754 PS 6/PS 8 + 3771 PS 2 push.

## Deferred by scope / risk

- **Pin/unpin helper sweep** (`withReadLatched`/`withWriteLatched`) across ~60 sites in `VTree.java` — needs per-site review because patterns vary (different latches, mid-block early returns, ctx state crossing the latch). Dedicated round.
- **`VTreeSearchCursor` setter-init protocol refactor** — 32 fields, ~10 setters called externally before `open()` by `VTree.VTreeAccessor.configureCursor()` and LSM wrappers. Replace with config object/builder; sequence after the pin/unpin sweep.
- **Split `VTreeSearchCursor` into `FullScanCursor` + `DFSCursor`** — `open()` has two distinct code paths sharing 32 fields; downstream methods branch on `fullScanMode`. Do after the setter refactor.
- **`createSearchCursor(boolean exclusive, boolean fullScanMode)` — remove unused `exclusive`** — sibling 1-arg `@Override` from `ITreeIndexAccessor` mandates `boolean exclusive`, so dropping it from the 2-arg collides with the override signature. Needs either rename or split into `createSearchCursor` + `createFullScanCursor` (cross-module ripple: `LSMVTreeSearchCursor.java:406` is the only external caller).
- **`buildLocalCentroidDirPageMap` full BFS rebuild per cursor open** — cache the map on the tree to skip rebuild.
- **`closeCurrentPage` / latch-lifetime audit** across the 8 methods that touch `currentPage` in `VTreeSearchCursor`.

## Likely real bugs (flagged but NOT changed)

These came out of multiple cleanup-agent reviews and need human investigation before touching:

- **`CreateVectorIndexStatement.java` is entirely dead** — parser emits `CreateIndexStatement` via the factory, never this class; `accept()` returns `null` (would NPE any visitor). `Statement.Kind.CREATE_VECTOR_INDEX` is correspondingly orphaned.
- **`CheckFullParallelSortRule.java` is not registered** in any `RuleCollections.java` entry. Either wire it in or delete.
- **`PhysicalOperatorTag.VECTOR_SEARCH` inserted mid-enum** — shifted ordinals of all subsequent entries. Verify no code persists or switches on ordinals before merging.
- **`AOrderedListVectorBinaryAccessor.getDimension()` lost its `@Override` annotation** while the parent interface still declares the method.
- **`DatasetRewriter.java:85`** reads argument index 4 as `Boolean isSample` and then as `String sampleIndexName` — likely arity/index bug.
- **`SQLPP.jj` adds `<INC : "inc">`** token never used in any production — silently makes `inc` a reserved word.
- **`VectorCheckTuple.equals()`** overridden without companion `hashCode()` — breaks hashed-collection invariants.

## Dead code kept due to signature/scope rules

- `VTreeLeafCentroid` — 5-arg and 6-arg constructors are dead, only 7-arg is called.
- `LSMVTreeCursorInitialState.getCursors()` — returns `null`, no callers.
- `LSMVTreeDiskComponent.isStaticStructure()` / `setStaticStructure(boolean)` + backing field — no readers.

## 3760-origin smells (not 3754/3771)

Found while reviewing `HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor.java` and `VTreeStaticStructureCreatorOperatorDescriptor.java`:

- Empty `catch (Exception)` blocks (×3 in static-structure creator).
- `COSINE_FORMAT` ≡ `COSINE_ALIAS` (duplicate constants, both `"cosine"`).
- `new Random()` without seed — non-deterministic static-structure builds.
- `performKMeansParallel` ~400-line method.

Related: [[vtree_patch1_review]] (earlier 48-file review), [[feedback-user-rebuilds-manually]].
