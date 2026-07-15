# Anatomy of one LSMVTree index instance

> **Status:** current
> **Verified against:** `111bfcd146` (2026-07-05)
> **Scope:** the complete structure of a single VTree index instance on one NC — files on disk,
> the component model (including the special static-structure component), how each component's
> page space is organized, the resource JSON, and the accessor/op-context machinery.

This doc is the storage-engine deep dive under the patch walkthroughs
([3754a](../80-patches/3754a-storage-layer-p1.md) = the single-tree engine,
[3754b](../80-patches/3754b-storage-layer-p2.md) = the LSM wrapper) and the
[creation-pipeline overview](../20-creation-pipeline/overview.md) (which explains *how* these
files come to exist). Byte-level page/tuple layouts live in [page-formats.md](page-formats.md).

## 1. The file set on disk

One VTree index instance = one directory per partition, managed by `LSMVTreeFileManager`
(`hyracks-fullstack/hyracks/hyracks-storage-am-lsm-vtree/.../lsm/vector/impls/LSMVTreeFileManager.java`).
Naming constants (lines 50–51): component suffix `vct`, static-structure name `.staticstructure`;
the delimiter is the LSM-common `_` (`AbstractLSMIndexFileManager#DELIMITER`).

Example directory after CREATE INDEX, two flushes, and one merge:

```
<iodevice>/storage/partition_0/MyDb/MyDataverse/MyDataset/0/ix_embedding/
├── .metadata           ← LSMVTreeLocalResource JSON (AsterixDB StorageConstants.METADATA_FILE_NAME)
├── .staticstructure    ← the ONE shared static-structure component (trained centroid tree)
├── 0_0_vct             ← first data disk component (Job-3 bulk load; sequence 0)
├── 1_1_vct             ← flushed component (sequence 1)
└── 0_1_vct             ← merged component covering sequences 0..1 (replaces 0_0_vct/1_1_vct)
```

Naming rules, all from `LSMVTreeFileManager`:

- **Data components**: `<seqStart>_<seqEnd>_vct`. Flush components have `seqStart == seqEnd`
  (`IndexComponentFileReference#getFlushSequence`, line 122); a merge product spans
  `<firstStart>_<lastEnd>` (`getMergeSequence`, line 126). `getRelFlushFileReference()` (line 94)
  builds the name as `baseName + "_" + "vct"`.
- **`.staticstructure`** is a literal file name (not a suffix on a sequence):
  `baseDir.getChild(STATIC_STRUCTURE_SUFFIX)` (line 74). Exactly one per index; every data
  component shares it. The component-file filter `vTreeFilter` (line 55) skips names starting
  with `.`, so neither `.staticstructure` nor `.metadata` is ever mistaken for a component.
- **`.metadata`** is not written by this layer at all — it is the serialized
  `LSMVTreeLocalResource` persisted by AsterixDB's `PersistentLocalResourceRepository`
  (`asterixdb/asterix-common/.../utils/StorageConstants.java` line 48). See §5.
- **Memory components have no file**: each gets a *virtual* `FileReference` named
  `<baseDir>_virtual_<i>` (`LSMVTree` constructor, line 167) that exists only to key the
  virtual buffer cache.

Two oddities worth knowing:

- `getNextComponentSequence()` (lines 65–82) special-cases "static structure exists but no data
  files yet" to return sequence 0 — but the general path `getFlushSequence(maxSeq + 1)` with
  `maxSeq == -1` produces the same `"0_0"`, so the branch is redundant (kept, presumably, for
  documentation value).
- `getRelMergeFileReference()` (lines 101–105) mints a per-merge static-structure reference
  (`<seq>_.staticstructure`) and stuffs it into the *bloom-filter* slot of
  `LSMComponentFileReferences` — but `LSMVTree#doMerge` only ever uses
  `getInsertIndexFileReference()`, so that file is never created. Dead code.

### Recovery-time validation

`cleanupAndGetValidFiles()` (lines 108–139) is the crash-recovery gate, and it is deliberately
static-structure-centric:

1. Collect all `*vct` files and sort **descending** (newest first — the order the LSM component
   list expects).
2. Validate the shared `.staticstructure` — but note `validateStaticStructureFile()` (lines
   183–200) only checks *existence* via `ioManager.exists()`; there is no checksum or
   metadata-page validity probe.
3. If the static structure is missing/invalid, **every** `vct` file is deleted
   (`cleanupOrphanedVTreeFile`, line 207) and the index comes up empty — data components are
   useless without the navigation tree that routed their contents.
4. Otherwise each `vct` file becomes a valid component file-reference triple
   `(vct, null, .staticstructure)`. `areHolesAllowed()` returns `false` (line 143): component
   sequences must be contiguous.

## 2. The component model

`LSMVTree` (`hyracks-storage-am-lsm-vtree/.../impls/LSMVTree.java`) extends `AbstractLSMIndex`
and holds three kinds of components:

| Kind | Class | Backing | How many |
|---|---|---|---|
| memory | `LSMVTreeMemoryComponent` wrapping a `VTree` on a VBC | virtual buffer cache pages | one per `IVirtualBufferCache` (AsterixDB default 2), built in the constructor (lines 165–178) |
| disk (data) | `LSMVTreeDiskComponent` wrapping a `VTree` on a `vct` file | disk buffer cache | `diskComponents` deque, newest first |
| static structure | `LSMVTreeDiskComponent` with `isStaticStructure == true` | the `.staticstructure` file | exactly one, in the dedicated `staticStructure` field (line 126) — **not** in `diskComponents` |

### The static-structure component's lifecycle

The `isStaticStructure` flag lives on `LSMVTreeDiskComponent` (line 68) and drives dispatch at
every lifecycle point:

- **Creation (Job 2 of CREATE INDEX).** `LSMVTree#createBulkLoader` (lines 257–288) sniffs the
  operation parameters: the presence of `PARAM_NUM_LEVELS` / `PARAM_CLUSTERS_PER_LEVEL` /
  `PARAM_CENTROIDS_PER_CLUSTER` (declared on `LSMVTreeDiskComponent`, lines 54–58) selects the
  static-structure path, which calls `createStaticStructure()` (lines 220–232): the component is
  built over `componentFileRefs.getStaticStructureFileReference()` (i.e. its inner `VTree`'s
  *file* is `.staticstructure` itself), flagged, and activated with `createComponent=true`.
  Inside the component, `createBulkLoader` (LSMVTreeDiskComponent lines 119–177) wires a
  `VTreeStaticStructureBuilder`; the data path instead requires
  `PARAM_STATIC_STRUCTURE_COMPONENT` and wires a `VTreeBulkLoader` fed by an accessor onto the
  static component (lines 184–195).
- **Registration.** When the load finishes, `addBulkLoadedDiskComponent` (lines 234–244)
  dispatches on the flag: static structure → `setStaticStructure()` (bypasses the component list
  and `validateComponentIds()`); data component → `setInitialized()` + `diskComponents.addFirst`.
  `setInitialized()` (LSMVTreeDiskComponent lines 223–226) reads the root page id out of the
  component's metadata page and installs it on the inner `VTree`.
- **Activation / recovery.** `LSMVTree#activate` (lines 647–655) overrides the base class:
  `loadDiskComponents()` (lines 668–679) re-creates every data component from
  `cleanupAndGetValidFiles()` (with `createComponent=false`), then `loadStaticStructure()`
  (lines 657–666) asks the file manager for the `.staticstructure` reference and re-creates the
  static component the same way. `setStaticStructure(component)` (lines 181–187) then re-wires
  all memory components (§4).
- **Deactivation / destruction.** The static component is *not* managed by the base class, so
  `deactivate` (lines 616–630) calls `staticStructure.deactivateAndPurge()` **after**
  `super.deactivate(flush)` — order matters because `doFlush` needs the static structure to copy
  its pages. `destroy` (lines 632–645) destroys it before delegating up. Both are best-effort
  (exceptions swallowed with a trace log).

### Flush and merge in one breath

(Full treatment in [3754b](../80-patches/3754b-storage-layer-p2.md); here only what shapes the
component structure.)

- `doFlush` (lines 361–409) copies **all** VBC pages of the flushing memory tree to the new
  component with identity page mapping via `VTreeFlushLoader#copyPage`, then appends a
  pointer-adjusted copy of the static structure (`copyStaticStructure`) and finalizes with the
  memory tree's leaf-centroid metadata. `doFlush` records `flushLoader.copyStaticStructure(...)`'s
  return value as the component's root page id; `copyStaticStructure` returns
  `staticBasePageId + staticTree.getRootPageId()` (`VTreeFlushLoader.java` line 199), i.e. the
  true root at the static structure's own root page id within the copied range — matching
  `VTreeBulkLoader#end`, which records `staticBasePageId + staticStructureRootPage` the same way
  (`VTreeBulkLoader.java` lines 547–553). (Historical note: this previously returned bare
  `staticBasePageId` — the first copied page, a **leaf** under the bottom-up builder — and was
  fixed 2026-07-04; see [bug-archive.md](../60-quality/bug-archive.md).)
- `doMerge` (lines 411–469) is "search-and-reload": a full-scan `LSMVTreeSearchCursor`
  (`fullScanMode=true`, sequential cluster order, antimatter-reconciled) feeds a regular
  `VTreeBulkLoader` on the merge target. Antimatter tuples are dropped only when the merge
  includes the oldest disk component (`createMergeOperation`, lines 575–599).

One more delegation quirk: `LSMVTree#getFileId` / `getPageManager` / `getRootPageId` /
`getLeafFrameFactory` etc. (lines 533–559) all delegate to the **current mutable memory
component**, not to any disk component.

## 3. Page space inside each component

All four component flavors are `VTree`s, but their internal page maps differ. The page-manager
metadata page ("index metadata page") is a different animal from VTree's per-cluster *directory*
("metadata frame") pages — see [page-formats.md §7](page-formats.md) for its bytes.

**`.staticstructure` component** (written by `VTreeStaticStructureBuilder`, pages allocated via
`AppendOnlyLinkedMetadataPageManager#takePage` which starts at 0):

```
page 0 .. L-1      leaf pages, cluster by cluster (intra-cluster overflow chained,
                   clusters linked by nextLeaf sibling pointers with overflow=false)
page L .. R-1      interior pages, one cluster per page (+overflow), bottom level first
page R             root page (always the HIGHEST takePage id — bottom-up build)
page R+1 (last)    index metadata page (appended at close by the append-only page manager;
                   holds rootPageId=R, num_leaf_centroids, first_leaf_centroid_id)
```

Leaf tuples carry `metaPtr = -1` sentinels — the structure has no data yet
(`VTreeStaticStructureBuilder.java` lines 86–87, 215–227).

**Bulk-loaded data component (Job 3 / merge output)** (written by `VTreeBulkLoader`):

```
[cluster c0 data pages, chained next_page]  [c0 directory pages, chained]
[cluster c1 data pages]                     [c1 directory pages]
...
[static-structure copy: leaves .. interiors .. root]   ← pointers offset by base id;
                                                         leaf metaPtr patched to real
                                                         clusterFirstDirPageId[c]
[index metadata page]                                  ← last physical page
```

Data pages take real ids immediately; directory pages are confiscated with `INVALID_DPID` and
get ids only at cluster end, so *within a cluster* data ids < directory ids
(`VTreeBulkLoader.java` lines 103–110, 378–420, 468–553).

**Flushed data component** (written by `VTreeFlushLoader`): identity copy first, static tail
second:

```
page 0 .. maxVbc   verbatim copies of VBC pages 0..maxVbc (see memory layout below) —
                   no pointer rewriting needed because ids are preserved
page maxVbc+1 ..   static-structure copy (child/next-leaf pointers offset by base;
                   leaf metaPtr rewritten from the memory tree's centroidDirPageMap,
                   which is valid unchanged thanks to identity mapping)
last page          index metadata page
```

(`VTreeFlushLoader.java` lines 79–94, 106–193.)

**Memory component (VBC page space)** — `VirtualFreePageManager` hands out ids starting at 2
(pages 0 and 1 are reserved by BTree convention: virtual metadata page 0, unused root page 1;
`VirtualFreePageManager.java` lines 40–54, 83–86):

```
page 0             reserved (virtual metadata; createMetadataFrame() returns null)
page 1             reserved root slot (unused by VTree — navigation uses the shared
                   static structure instead)
page 2 .. 2+C-1    C empty directory pages, one per leaf centroid, pre-created by
                   VTree#setStaticStructure at component (re)initialization
page 2+C ..        data pages and directory overflow pages, allocated on demand by inserts
```

There are **no interior/leaf pages in a memory component** — that is the point of the shared
static structure.

## 4. How memory components wire to the shared static structure

`VTree` carries a dual personality controlled by `staticBufferCache`
(`hyracks-storage-am-vtree/.../impls/VTree.java` lines 109–120):

- **Disk component**: `staticBufferCache == null`; navigation root = its own `rootPage`
  (embedded static copy). `getNavigationRootPageId()` line 878.
- **Memory component**: `staticBufferCache != null`; navigation reads the `.staticstructure`
  component's pages directly (`staticFileId`, `staticRootPage`), while all *writes* land in VBC
  pages found through `centroidDirPageMap`.

The wiring happens in `VTree#setStaticStructure(VTreeAccessor)` (lines 943–992):

1. Cache `staticBufferCache` / `staticFileId` / `staticRootPage` from the static component.
2. Read `num_leaf_centroids` and `first_leaf_centroid_id` (8-byte longs keyed by
   `VTreeMetadataKeys`) off the static component's metadata page (lines 960–970).
3. Pre-create one **empty directory page per leaf centroid** in the VBC and record the mapping
   `centroidDirPageMap[cid - firstLeafCentroidId] = vbcPageId` (lines 972–989).

Cluster access then splits on component kind in `prepareClusterAccess` (lines 1066–1093): memory
components take the directory page id straight from `centroidDirPageMap`; disk components pin
the leaf page found by navigation and read the tuple's `metaPtr` field.

Lifecycle triggers for this wiring (`LSMVTree` lines 181–218, `LSMVTreeMemoryComponent` lines
70–84): `setStaticStructure` on the LSM index → all memory components;
`allocateMemoryComponents` → same; a recycled (post-flush) component runs
`vtree.resetInitialization()` in `cleanup()` and re-wires in `doAllocate()` — so the directory
pages are re-created fresh in the recycled VBC.

## 5. The resource JSON (`.metadata`)

`LSMVTreeLocalResource`
(`hyracks-storage-am-lsm-vtree/.../lsm/vector/dataflow/LSMVTreeLocalResource.java`,
`serialVersionUID = 2`) is the index's restart-safe genome. `createInstance()` (lines 186–228)
rebuilds the entire `LSMVTree` from it via `LSMVTreeUtils.createLSMTree`. Every key it persists:

**Inherited from `LsmResource#appendToJson`**
(`hyracks-storage-am-lsm-common/.../dataflow/LsmResource.java` lines 177–236) — plus the
registry's class-identifier envelope written by `toJson`:

| Key | Content |
|---|---|
| `path` | partition-relative index path |
| `storageManager` | serialized `IStorageManager` |
| `typeTraits` | array — the **data-tuple** type traits (see [page-formats.md §6](page-formats.md)) |
| `cmpFactories` | array — data-tuple comparator factories (raw double, raw int, then ADM PK cmps) |
| `filterTypeTraits`, `filterCmpFactories`, `filterFields` | LSM filter config (null / empty for VTree today) |
| `opTrackerProvider`, `ioOpCallbackFactory`, `pageWriteCallbackFactory` | serialized factories |
| `metadataPageManagerFactory` | → `AppendOnlyLinkedMetadataPageManager` for disk components |
| `vbcProvider`, `ioSchedulerProvider`, `mergePolicyFactory`, `mergePolicyProperties` | LSM machinery |
| `durable`, `nullTypeTraits`, `nullIntrospector` | misc |

**VTree-specific** (`appendToJson`, lines 237–275; read back in `fromJson`, lines 277–327):

| Key | Written when | Meaning / read-back behavior |
|---|---|---|
| `vectorDimensions` | always | embedding dimensionality |
| `vectorFields` | always | index field positions |
| `filterFields` | always | (re-written, overwrites the base-class entry with identical content) |
| `atomic` | always | atomic-transaction dataset flag |
| `distanceMetric` | if non-null | e.g. `"euclidean"`; **absent → legacy default `euclidean`** |
| `crossPollinationM`, `rngFactor`, `epsilon` | only when M > 1 | DML replication placement; absent → `CrossPollinationConfig.LEGACY` (M=1) |
| `vectorAccessorFactory` | always | nested registry JSON (`AOrderedListVectorBinaryAccessorFactory`); **missing → `createInstance` throws** ("resource is corrupted") |
| `distanceFunctionFactory` | always | nested registry JSON; missing → throws likewise |
| `confidenceInterval`, `minQuantile`, `maxQuantile`, `alpha`, `bits`, `sampleCount` | only when quantized (set by Job 1's `QuantizedIndexCreate` through `IQuantizedResource#setQuantizationParameters`, lines 410–433) | the six quantization constants |
| `numPrimaryKeyFields` | always (default 1 on read) | PK count in the data tuple |
| `numIncludeFields` | always (default 0 on read) | INCLUDE column count |

Read-back subtleties: `isQuantized` is **inferred as `minQuantile != null`** (line 310), and the
`dataTupleCreatorFactory` is *not* persisted — it is reconstructed as
`new VTreeDataTupleCreatorFactory(numIncludeFields, isQuantized)` (lines 311–312).
`createInstance` re-packs the six constants into the `float[6]`
`{minQuantile, maxQuantile, alpha, confidenceInterval, bits, sampleCount}` (lines 210–214) that
threads down through `VTreeFactory` into every `VTree`.

## 6. Accessors and operation contexts

Three nested layers, mirroring LSM B+tree:

- **`LSMVTreeIndexAccessor`** (extends `LSMTreeIndexAccessor`): the harness-facing accessor.
  Default `createSearchCursor` yields the streaming `LSMVTreeSearchCursor` (merge-only in
  practice); production ANN queries pass `LSMVTreeTopKSearchCursor.IAP_KEY = true` in the
  index-access parameters to get the top-K cursor
  (`LSMVTreeIndexAccessor.java` lines 48–56, `LSMVTree#createTopKSearchCursor` line 612).
- **`LSMVTreeOpContext`** (`.../impls/LSMVTreeOpContext.java`): created per accessor. At
  construction (lines 67–106) it pre-creates, for **every** memory component, a
  `VTree.VTreeAccessor` plus its `VTreeOpContext`; holds the matter (`insertDataFrame`) and
  antimatter (`deleteDataFrame`) frame pair and their factories; builds the ADM-aware
  `MultiComparator`; and owns the reusable `LSMVTreeCursorInitialState`.
  `setCurrentMutableComponentId` (lines 124–143) switches the active accessor and flips the
  active data frame: INSERT/UPSERT → matter frame factory, DELETE/PHYSICALDELETE → antimatter
  frame factory — this is how one op-context serves both tuple polarities.
- **`VTreeOpContext`** (`hyracks-storage-am-vtree/.../impls/VTreeOpContext.java`, lines 50–115):
  the single-tree working set — one instance of each of the four frames (interior, leaf,
  directory, data), the page-manager metadata frame, the `MultiComparator`, and the
  `IVTreeDataTupleCreator` instantiated with the quantization `float[6]`. It also implements
  `IExtraPageBlockHelper` and tracks `pageLsns`/`smPages` for structure modifications.

`modify()` on `LSMVTree` (lines 308–335) routes INSERT/DELETE/PHYSICALDELETE/UPSERT to the
current mutable `VTreeAccessor`; `search()` (lines 345–352) resets the shared
`LSMVTreeCursorInitialState` with the operational components and opens the cursor on it.

## 7. One instance, end to end

```
 index directory (one per partition)
 ┌──────────────────────────────────────────────────────────────────────────────┐
 │ .metadata            .staticstructure          0_0_vct        1_1_vct        │
 │ (resource JSON)             │                     │              │           │
 └─────────────────────────────┼─────────────────────┼──────────────┼───────────┘
        creates                │                     │              │
        LSMVTree ◄─────────────┘ (activate)          │              │
 ┌──────────────────────────────────────────────────────────────────────────────┐
 │ LSMVTree                                                                     │
 │                                                                              │
 │  staticStructure ──► LSMVTreeDiskComponent(isStaticStructure=true)           │
 │     ▲    ▲             VTree over .staticstructure:                          │
 │     │    │             [leaf pgs][interior pgs][root pg][meta pg]            │
 │     │    │             leaf metaPtr = -1 (sentinel)                          │
 │     │    │                                                                   │
 │     │    │ navigation (read-only, shared)                                    │
 │     │    │                                                                   │
 │  memoryComponents[0..1] ──► LSMVTreeMemoryComponent                          │
 │     │                        VTree over VBC "_virtual_i":                    │
 │     │                        [pg0 rsvd][pg1 rsvd][dir pg per leaf cid]       │
 │     │                        [data pgs + dir overflow, on demand]            │
 │     │                        centroidDirPageMap: cid → VBC dir page          │
 │     │                                                                        │
 │     │ copied into every data component at flush/bulk-load                    │
 │     ▼                                                                        │
 │  diskComponents (newest first) ──► LSMVTreeDiskComponent                     │
 │                                     VTree over N_N_vct:                      │
 │                                     [c0 data][c0 dir][c1 data][c1 dir]...    │
 │                                     [static copy: leaf..root, ptrs offset]   │
 │                                     [index metadata page (last)]             │
 └──────────────────────────────────────────────────────────────────────────────┘
   per-page byte layouts: see page-formats.md
```
