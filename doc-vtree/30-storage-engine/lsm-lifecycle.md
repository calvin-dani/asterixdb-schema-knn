# LSM lifecycle — VTree flush and merge, end to end

> **Status:** current
> **Verified against:** `9c5fd8c704` (2026-07-07, post merge-bug fixes; stack reworked — p2=`250230d228`, p3=`10703e9c19`)
> **Scope:** every path that turns a VTree memory component into a disk component (FLUSH) and
> disk components into fewer disk components (MERGE / `COMPACT DATASET`) — triggers, harness
> sequencing, loader mechanics, file naming, and the invariants a reviewer must check.

This doc owns the *lifecycle plumbing*. What happens to individual matter/antimatter tuples
is [dml.md §4](dml.md); the component model and page-space maps are
[index-instance-anatomy.md](index-instance-anatomy.md); byte layouts are
[page-formats.md](page-formats.md); the patch-level overview is
[3754b](../80-patches/3754b-storage-layer-p2.md). Line numbers below are against the
verified commit and rot fastest — trust class#method names first.

---

## 1. Flush triggers

A VTree is always a *secondary* index (`needKeyDupCheck=false` →
`LSMVTree#isPrimaryIndex()` returns false), so it never decides to flush on its own. Flushes
are **dataset-partition-wide**: the primary index's operation tracker schedules a flush on
*every* open index of the partition — primary BTree, VTree, and any other secondaries — so
their component boundaries stay aligned. Four routes end at the same
`accessor.scheduleFlush()` call.

### 1.1 Memory component full (the organic path)

1. **VBC watermark.** On every modification exit,
   `AbstractLSMMemoryComponent#threadExit` (MODIFICATION case,
   `hyracks-storage-am-lsm-common/.../impls/AbstractLSMMemoryComponent.java` ~line 188)
   checks `vbc.isFull(this)`; if the component's virtual-buffer-cache budget is exhausted the
   component state flips `READABLE_WRITABLE → READABLE_UNWRITABLE`. (Entry-side, ~line 133,
   modifications are refused while `vbc.isFull()` — writers stall until a flush frees pages.)
2. **Flush request flag.** `LSMHarness#exitOperationalComponents` (~line 341) sees the
   mutable component in `READABLE_UNWRITABLE` after a modification and calls
   `lsmIndex.changeFlushStatusForCurrentMutableCompoent(true)` — an `AtomicBoolean` per
   memory component on `AbstractLSMIndex` (`flushRequests[]`, read back by
   `hasFlushRequestForCurrentMutableComponent()`).
3. **Operation tracker.** When the last active modification on the partition completes,
   `PrimaryIndexOperationTracker#completeOperation` →
   `flushIfNeeded()` → `flushIfRequested()`
   (`asterixdb/asterix-common/.../context/PrimaryIndexOperationTracker.java` lines 110–217):
   any index with a pending flush request (or `flushOnExit`) triggers the sequence. It marks
   every index's current memory component unwritable, then writes a **FLUSH log record**
   (`TransactionUtil.formFlushLogRecord`) for durable datasets.
4. **Log-driven callback.** When the LogFlusher persists that record it calls back
   `triggerScheduleFlush(logRecord)` (line 220): refresh the component-id generator, build a
   `flushMap` with `KEY_FLUSH_LOG_LSN` + `KEY_NEXT_COMPONENT_ID`, and for **each open index
   of the partition** create a `NoOpIndexAccessParameters` accessor, install the map on its
   op-context, and call `accessor.scheduleFlush()` (line 255).
5. **Harness + scheduler.** `LSMHarness#scheduleFlush` (line 520) allocates memory
   components if needed, calls `AbstractLSMIndex#createFlushOperation(ctx)` (line 461) under
   the opTracker monitor — this picks the target file from
   `fileManager.getRelFlushFileReference()`, marks the flushing component
   `schedule(FLUSH)`, clears the flush request, and **rotates the mutable component**
   (`changeMutableComponent()`) so writers proceed into the next memory component — then
   hands the operation to the `ILSMIOOperationScheduler` (asterix default: the greedy
   asynchronous scheduler per `storage.io.scheduler`). The IO thread later runs
   `LSMHarness#flush` (line ~640): wait to enter the component
   (`READABLE_UNWRITABLE_FLUSHING`, zero writers), `doIo(operation)` →
   **`LSMVTree#doFlush`** (§2), then `exitComponents` publishes the result (§2.4).

`LSMVTree#createFlushOperation` (line 481) itself is trivial: it wraps the accessor and
`componentFileRefs.getInsertIndexFileReference()` in a `LSMVTreeFlushOperation`
(`.../lsm/vector/impls/LSMVTreeFlushOperation.java`) — single-file component, no bloom
filter, no delete file.

### 1.2 Explicit flush — `FlushDatasetUtil.flushDataset`

`asterixdb/asterix-app/.../utils/FlushDatasetUtil.java` builds a one-operator job:
empty-tuple source → `FlushDatasetOperatorDescriptor`
(`asterixdb/asterix-runtime/.../operators/std/FlushDatasetOperatorDescriptor.java`), whose
`close()` takes an S lock on the dataset and calls
`DatasetLifecycleManager#flushDataset(datasetId, asyncFlush=false)` →
`flushDatasetOpenIndexes` (`asterixdb/asterix-common/.../context/DatasetLifecycleManager.java`
line 547): per partition, `primaryOpTracker.setFlushOnExit(true)` + `flushIfNeeded()`, then
**synchronously waits** on all scheduled `FlushOperation`s (`LSMIndexUtil.waitFor`). This is
the flush that `QueryTranslator#doCreateIndexImpl` runs between vector-index creation jobs
(after Job 1, before training) — see
[creation-pipeline overview](../20-creation-pipeline/overview.md).

### 1.3 Shutdown / restart (what integration part 4 exercises)

`DatasetLifecycleManager#stop` (line 634) → `closeAllDatasets()` → `closeDataset()`: a
synchronous `flushDatasetOpenIndexes`, `dsInfo.waitForIO()` (drains merges too), then
`closeIndex` → `LSMVTree#deactivate(flush=true)`. `LSMVTree` overrides `deactivate`
(line 617) to run the inherited flushing deactivate **first** and only then
`staticStructure.deactivateAndPurge()` — `doFlush` needs the static structure alive to copy
its pages. So on a graceful shutdown/SIGINT every non-empty memory component becomes a disk
component before the process exits; integration part 4's restart then re-activates the index
purely from disk (`LSMVTree#activate` → `loadDiskComponents` → `loadStaticStructure`, lines
647–679). The 2026-07-04 flush-root bug showed up exactly here: pre-fix, components written
by this shutdown flush had a leaf recorded as root, and after restart deleted PKs resurfaced
(351 resurrected deletes on the baseline branch; zero after the fix).

### 1.4 Recovery interplay

- On the flush path, `LSMIOOperationCallback#afterOperation`
  (`asterixdb/asterix-common/.../ioopcallbacks/LSMIOOperationCallback.java` lines 108–120)
  stamps the new component's metadata page with the flush LSN (`KEY_FLUSH_LOG_LSN`) and
  component id (`KEY_FLUSHED_COMPONENT_ID`); merges stamp the max LSN of the merged
  components. `afterFinalize` marks the component valid (`markAsValid` — durability fsync +
  validity bit), and the index-checkpoint manager records the flushed sequence.
- On activate, `AbstractLSMIndexFileManager#cleanupAndGetValidFiles` (overridden by
  `LSMVTreeFileManager`, §4.3) deletes invalid/incomplete component files; recovery replays
  log records newer than the persisted component LSNs into fresh memory components, which
  later flush normally. VTree adds one twist: if the shared `.staticstructure` file is
  missing or invalid, **all** `*_vct` components are deleted as orphans
  (`LSMVTreeFileManager` lines 120–129). That validation is existence-only — a truncated
  static file passes; see the latent-risk entry in the
  [bug archive](../60-quality/bug-archive.md).

---

## 2. Flush mechanics — `LSMVTree#doFlush` + `VTreeFlushLoader`

`LSMVTree#doFlush`
(`hyracks-storage-am-lsm-vtree/.../impls/LSMVTree.java` lines 361–409) does no tuple-level
work at all. Steps:

1. `createDiskComponent(componentFactory, flushOp.getTarget(), …, createComponent=true)` —
   a fresh `LSMVTreeDiskComponent` over the `N_N_vct` target file.
2. `new VTreeFlushLoader(pageWriteCallback, diskTree, memTree)`
   (`hyracks-storage-am-vtree/.../impls/VTreeFlushLoader.java`).
3. **Identity page copy.** For VBC page 0..`maxPageId` (from the memory tree's page
   manager): `flushLoader.copyPage(sourcePage)` — `takePage` on the disk free-page manager,
   `confiscatePage`, raw `System.arraycopy` of the whole buffer, FIFO write. Because pages
   are allocated on the disk side in the same 0..N order, **VBC page id N lands at disk page
   id N**, so every intra-component pointer — directory `nextPage` chains, data-page
   `nextPage` chains, and the directory page ids recorded in `centroidDirPageMap` — is
   already correct byte-for-byte. No pointer rewriting, no re-routing, no re-sorting;
   **antimatter tuples travel verbatim** inside their data pages
   ([dml.md §4.2](dml.md)).
4. **Static-structure append** — `flushLoader.copyStaticStructure(staticAccessor)`,
   the only pointer-aware part:
   - allocate `numStaticPages` fresh disk pages starting at `staticBasePageId`, then
     stream the shared static component **one page at a time**: pin the source page, copy
     it into a single confiscated destination page, release the source, patch pointers,
     write, move on — bounded memory (no up-front byte[] snapshot, no simultaneous
     confiscation of every destination page);
   - **interior pages** (`level > 0`): every child pointer `+= staticBasePageId`; overflow
     `nextPage` likewise;
   - **leaf pages** (`level == 0`): each slot's directory pointer is set from the *memory
     tree's* `centroidDirPageMap[cid − firstLeafCid]` — indexed by the slot's **centroid
     id**, not traversal order (physical page order need not match the `nextLeaf` chain);
     identity mapping in step 3 is what makes those VBC directory page ids valid disk ids.
     `nextLeaf` gets the `staticBasePageId` shift; resolved graph-neighbor entries get the
     same shift via `offsetLeafNeighborPointers` (provisional entries untouched);
   - returns **`staticBasePageId + staticTree.getRootPageId()`** — the fixed root
     arithmetic. Bottom-up layout puts leaves first and the root at the highest static page
     id, so returning bare `staticBasePageId` (the pre-2026-07-04 behavior) persisted a
     *leaf* as component root; regression = `LSMVTreeFlushSearchTest`. See the FIXED entry
     in the [bug archive](../60-quality/bug-archive.md) and §6 for the parity rule.
5. **Finalize** — `flushLoader.end(numLeafCentroid, firstLeafCentroidId, rootPageId)`
   (lines 235–247): installs the root on the tree and the metadata page manager, and writes
   `NUM_LEAF_CENTROIDS` / `FIRST_LEAF_CENTROID_ID` into the component metadata page
   ([page-formats.md §7.1](page-formats.md)) using the *memory tree's* values
   (`getNumLeafCentroidMem`/`getFirstLeafCentroidIdMem`). On any failure the loader
   `abort()`s (returns all pages, aborts the compressed writer).

The result is a **self-contained** disk component: navigation copy + directories + data, no
runtime dependency on the shared `.staticstructure` (which only memory components navigate
through).

### 2.4 Publication and memory-component recycling

Back in `LSMHarness#exitComponents` after `doIo`:

- `exitOperation` (FLUSH case) → `lsmIndex.addDiskComponent(newComponent)` (newest-first
  list) and `mergePolicy.diskComponentAdded(lsmIndex, false)` — which may immediately
  schedule a merge (§3.3).
- `flushingComponent.flushed()` resets flush bookkeeping; the flushed memory component is
  `UNREADABLE_UNWRITABLE` and, once its last concurrent reader exits, is queued for cleanup.
- **Cleanup/recycle**: `LSMVTreeMemoryComponent#cleanup`
  (`.../impls/LSMVTreeMemoryComponent.java` lines 70–76) calls
  `vtree.resetInitialization()` **before** `super.cleanup()` — clearing `initialized`,
  `centroidDirPageMap`, `directoryPageIds` so nothing dangles over freed VBC pages. When
  the component is next allocated, `doAllocate` →
  `LSMVTree#reinitializeMemoryComponent` → `VTree#setStaticStructure(staticAccessor)`
  (`hyracks-storage-am-vtree/.../impls/VTree.java` lines 943–992): re-read
  `NUM_LEAF_CENTROIDS`/`FIRST_LEAF_CENTROID_ID` from the shared static component's metadata
  page and create one fresh, empty directory page per leaf centroid in the VBC. Details of
  this wiring: [index-instance-anatomy.md §4](index-instance-anatomy.md).
- **Searches concurrent with a flush** survive: `LSMVTreeSearchCursor` (and the top-K
  cursor) periodically call `replaceMemoryComponentWithDiskComponentIfNeeded()` (every
  `SWITCH_COMPONENT_CYCLE` `hasNext` calls) to swap a component that went
  `UNREADABLE_UNWRITABLE` mid-search for its flushed disk twin and re-open the sub-search.

---

## 3. `COMPACT DATASET` → merge scheduling

### 3.1 Statement path

Syntax: `COMPACT DATASET|COLLECTION <qualified-name>;` — `CompactStatement()` in
`asterixdb/asterix-lang-sqlpp/src/main/javacc/SQLPP.jj` (~line 3872).

`QueryTranslator#handleCompactStatement`
(`asterixdb/asterix-app/.../translator/QueryTranslator.java` line 5642) →
`doCompactStatement` (line 5661): under the compact lock it builds **one job per index** —

- `DatasetUtil.compactDatasetJobSpec`
  (`asterixdb/asterix-metadata/.../utils/DatasetUtil.java` line 402) for the primary index;
- `IndexUtil.buildSecondaryIndexCompactJobSpec`
  (`asterixdb/asterix-metadata/.../utils/IndexUtil.java` line 225 →
  `SecondaryIndexOperationsHelper#buildCompactJobSpec`) for each secondary, **including the
  VTree** (it is not a sample index, so the filter at QueryTranslator line 5687 keeps it) —

and runs them sequentially. Every one of these jobs contains a single source-less
`LSMTreeIndexCompactOperatorDescriptor`; its pushable
(`hyracks-storage-am-lsm-common/.../dataflow/LSMIndexCompactOperatorNodePushable.java`)
opens the local index per partition and calls **`accessor.scheduleFullMerge()`**. There is
no VTree-specific compact operator — the generic one works because everything
index-specific hides behind `createMergeOperation`/`doMerge`.

### 3.2 Harness scheduling

- `LSMHarness#scheduleFullMerge` (line 651): set `fullMergeIsRequested`, put **all** disk
  components into `ctx.getComponentsToBeMerged()`, call
  `AbstractLSMIndex#createMergeOperation` (line 487). Guards: index deactivating or fewer
  than 2 components → `NoOpIoOperation`; any candidate already `READABLE_MERGING` → NoOp
  (the flag stays set and the full merge is re-attempted when the in-flight merge
  completes). Then `ioScheduler.scheduleOperation`.
- The base `createMergeOperation` builds a dedicated merge op-context whose
  `componentHolder` is exactly the merging components, derives the target name via
  `getMergeFileReferences(first, last)` — `LSMVTree`'s override (line 562) feeds the first
  and last components' file names to `LSMVTreeFileManager#getRelMergeFileReference`, giving
  `<firstSeq>_<lastSeq>_vct` — and delegates to `LSMVTree#createMergeOperation` (line 575):

  - **Antimatter decision** (`returnDeletedTuples`): default **true** (preserve). It flips
    to **false** (drop during merge) iff the merging set includes the **oldest** disk
    component (`mergingComponents.getLast() == diskComponents.getLast()`) — nothing older
    could still hold a matter twin, so unmatched antimatter is garbage. A full merge from
    COMPACT always includes the oldest, so **COMPACT physically removes deletes**.
  - Creates the merge cursor up front: `new LSMVTreeSearchCursor(opCtx,
    returnDeletedTuples, fullScanMode=true, stats)` and wraps everything in a
    `LSMVTreeMergeOperation` (single-file refs, like flush).
- `LSMHarness#merge` on the IO thread enters the components (they flip to
  `READABLE_MERGING`) and calls `doIo` → **`LSMVTree#doMerge`**.

### 3.3 Merge policy — who merges when nobody types COMPACT

The policy instance is owned per index: dataset DDL stores a compaction policy
(`Dataset#getCompactionPolicy`; settable via the dataset-level `WITH`
`merge-policy` / legacy `compaction` options), `DatasetUtil#getMergePolicyFactory`
(line 268) instantiates the factory, and it rides through
`VTreeResourceFactoryProvider#getResourceFactory` into the persisted
`LSMVTreeLocalResource`, whose `createInstance` calls
`mergePolicyFactory.createMergePolicy(...)` and passes the policy into
`LSMVTreeUtils.createLSMTree` → the `LSMVTree` constructor. So the VTree obeys the *same*
policy as its dataset — nothing VTree-specific.

- **Production default** (`StorageConstants.DEFAULT_COMPACTION_POLICY_NAME`):
  `size-bounded-concurrent` (`SizeBoundedConcurrentMergePolicy(Factory)`,
  `asterixdb/asterix-common/.../storage/`) with `min-merge-component-count=3`,
  `max-merge-component-count=10`, `size-ratio=1.2`, `max-component-count=30`. Its
  `diskComponentAdded` — invoked by the harness after **every** flush and merge — picks a
  size-bounded run of adjacent components and calls `accessor.scheduleMerge(components)`.
  Note it may select a run that does **not** include the oldest component → a
  `returnDeletedTuples=true` merge, which must preserve antimatter through the merge
  bulk loader (see §6.6 — FIXED 2026-07-07 with the copy tuple writer).
- **JUnit tests** use `NoMergePolicy` (`LSMVTreeTestHarness` line 103): merges happen only
  when a test calls `scheduleMerge`/`scheduleFullMerge` explicitly (e.g.
  `LSMVTreeMergeTest`). The in-process integration runs (via
  `AsterixHyracksIntegrationUtil`) use the production default unless the dataset says
  otherwise, so background merges *can* fire there.

---

## 4. Merge mechanics — `LSMVTree#doMerge`

(`LSMVTree.java` lines 411–469.) Merge = "search everything, bulk-load the survivors":

1. **Predicate**: `new VTreeSearchPredicate()` with `setEpsilon(0.0)`. Epsilon/nprobe are
   irrelevant — the cursor was constructed in full-scan mode, which installs
   `SequentialClusterSelectionStrategy`
   (`.../impls/SequentialClusterSelectionStrategy.java`): no level-wise/DFS selection, no
   early termination (`shouldStopAdvancing` always false). Since 2026-07-07 the predicate
   also carries the layout-correct `pkStartField`
   (`VTreeDataTupleConstants.getPkStartField(isQuantized())` — 4 for quantized, 2
   otherwise), so the reconciliation key is exactly ⟨distance, PK…⟩ (§6.7).
2. **Search**: `search(mergeOp.getAccessor().getOpContext(), cursor, mergePred)` — the
   op-context's `componentHolder` is exactly the merging components, so the cursor sees
   nothing else (no memory component, no non-participating disk components).
3. **Full-scan cursor behavior** (`LSMVTreeSearchCursor`, full-scan branches +
   `VTreeSearchCursor#navigateToFirstCluster`, lines 405–485):
   - each per-component `VTreeSearchCursor` walks its own embedded static copy to the
     leftmost leaf and collects **every** leaf slot's directory pointer into
     `allDirectoryPageIds` — including `-1` sentinels for clusters that got no data (legal
     in merge mode; a WARN only in query mode). Because every component embeds a copy of
     the *same* static structure, position *i* in every component's list is the *same
     centroid* — this is what makes lock-step advancement meaningful;
   - components advance **in lock-step**: only when *all* components have exhausted cluster
     *i* does `advanceAllComponentsToNextCluster()` move everyone to *i+1*
     (full-scan branch: each cursor steps its own `allDirectoryPageIds`, since directory
     page ids are component-local);
   - within a cluster, tuples merge through the priority queue keyed
     ⟨distance (field 0), then the `numPrimaryKeyFields` PK fields from `pkStartField`⟩ —
     secondary fields and INCLUDE fields are excluded — with newest-component-first
     tie-break; antimatter is reconciled (`returnDeletedTuples=false`: matter+antimatter
     pairs cancel, orphan antimatter is silently dropped) or passed through
     (`returnDeletedTuples=true`) — the same machinery described in
     [dml.md §4.1](dml.md).
4. **Target component + loader**: `createDiskComponent(componentFactory,
   mergeOp.getTarget(), …)`; the op-context parameters get
   `PARAM_STATIC_STRUCTURE_COMPONENT → getStaticStructure()`;
   `mergedComponent.createBulkLoader(...)` →
   `LSMVTreeDiskComponent#createBulkLoader` (lines 118–143) sees no structure keys → data
   path `createVTreeBulkLoader`: an accessor over the **shared static
   component** feeds `VTree#createComponentBulkLoader` → a regular **`VTreeBulkLoader`**
   (`hyracks-storage-am-vtree/.../impls/VTreeBulkLoader.java`) — the same class Job 3 of
   CREATE INDEX uses, except that for MERGE operations its data frames are created from an
   **antimatter-preserving copy frame factory** (`createCopyDataFrameFactory()` →
   `LSMVTreeCopyTupleWriter`, §6.6) so preserved antimatter survives the rewrite. `numElementsHint` is `getNumberOfElements(mergedComponents)`, which
   actually sums component *file sizes in bytes* (`getComponentSize`) — harmless, it is
   only a hint, and `VTreeBulkLoader` ignores it.
5. **Drain**: the `while (cursor.hasNext()) … componentBulkLoader.add(tuple)` loop. The
   loader reads only static-structure *metadata* at construction (page contents are
   streamed later, in `end()`); `add()` routes each tuple by its
   **centroidId field (index 1)** — a change of centroid triggers
   `loadToNextLeafCluster(cid − firstLeafCentroidId)`, which seals the previous cluster's
   data-page chain and directory pages. Data pages are written immediately with
   pre-allocated `nextPage` forward pointers; directory pages are held per cluster and
   finalized/chained/written on cluster switch (`finalizeClusterDirectory`).
6. **`end()`**: seal the last cluster, then stream the static pages from the still-open
   source component to the tail one source/destination page pair at a time (pass 1 builds
   an O(K) cid → (finalPageId, slot) map from the source leaf pages; pass 2 copies, patches
   and writes each page before touching the next), with the identical pointer-offset logic
   as the flush loader (interior children, `nextLeaf`, leaf directory pointers from
   `clusterFirstDirPageId[cid − first]`, neighbor-list resolution against the pass-1 map),
   then record root =
   **`staticBasePageId + staticStructureRootPage`** and write the two leaf-centroid
   metadata keys. Same self-contained component layout as a flushed component; only the
   page-space interleaving differs (data/dir pages first, static copy last — bulk-load
   layout, see [index-instance-anatomy.md §3](index-instance-anatomy.md)).
7. **Publication + old-component retirement** (harness, not VTree code):
   `exitComponents` (MERGE case) → `AbstractLSMIndex#subsumeMergedComponents` (line 723)
   swaps the merged run for the new component *in place* in `diskComponents`. The old
   components go `INACTIVE` once their last reader exits, land in
   `getInactiveDiskComponents()`, and are physically deleted lazily by
   `scheduleCleanup` → `LSMCleanupOperation` when their buffer-cache file reference count
   drops to 1. A crash between merge completion and cleanup is safe: on the next activate,
   `cleanupAndGetValidFiles` recognizes the merged file's `<first>_<last>` range subsumes
   the old sequences and deletes the leftovers.

### 4.5 File naming (`LSMVTreeFileManager`)

(`.../lsm/vector/impls/LSMVTreeFileManager.java`)

| File | Producer | Naming |
|---|---|---|
| `.staticstructure` | CREATE INDEX Job 2 | one per index, shared, **never merged, never renamed** |
| `<s>_<s>_vct` | flush / initial bulk load | `getRelFlushFileReference` → next sequence; first data component after the static structure is forced to `0_0` (lines 64–82) |
| `<first>_<last>_vct` | merge | `getRelMergeFileReference` → `IndexComponentFileReference.getMergeSequence` |

Quirks:
- `getRelFlushFileReference` returns a `LSMVTreeComponentFileReferences` (4-slot subclass
  carrying the shared `.staticstructure` ref); `getRelMergeFileReference` returns a plain
  `LSMComponentFileReferences` that mints a **per-merge** `<seq>_.staticstructure` name in
  the bloom-filter slot — **dead plumbing**: `doMerge` only ever uses
  `getInsertIndexFileReference()`, so that file is never created. Already catalogued as a
  latent risk in the [bug archive](../60-quality/bug-archive.md) and
  [index-instance-anatomy.md §1](index-instance-anatomy.md).
- `areHolesAllowed() = false`: component sequences must be contiguous; on recovery a gap
  invalidates the newer files.

---

## 5. Worked end-to-end trace

Setup: quantized VTree, one partition, empty index after CREATE INDEX (shared
`.staticstructure` + bulk-loaded component `0_0_vct` from Job 3). Follow record pk `"X"`
(matter) and its later delete (antimatter). Assume `cross_pollination_m=1` for brevity —
with M>1 everything below happens once per replica cluster ([dml.md §5](dml.md)).

1. **Insert pk X.** Routed into memory component M0's cluster `c7`; matter tuple
   `⟨d=0.42, cid7, qd, qv, "X"⟩` lands sorted in a VBC data page reachable from
   `centroidDirPageMap[7 − first]`.
2. **Flush #1** (VBC fills, or `FlushDatasetUtil`, or shutdown). Tracker → harness →
   `doFlush`: M0's pages 0..N copied identically into `1_1_vct`; static copy appended at
   `staticBase`; leaf slot for `c7` points at the same directory page id the VBC used; root
   = `staticBase + staticRoot`; metadata keys written; component added at the head:
   `[1_1, 0_0]`. The matter tuple's bytes did not move relative to their page. M0 is
   recycled — `resetInitialization()` then, on next allocation, fresh empty directory pages
   from the shared static component.
3. **Delete pk X.** New memory component M0′: `tryPhysicalDelete` finds nothing local (the
   matter is on disk), so an **antimatter** twin `⟨0.42, cid7, …, "X"⟩•` is inserted at the
   same key ([dml.md §3.5](dml.md)). Any top-K query now sees antimatter (M0′) adjacent to
   matter (`1_1`) in the ⟨distance,PK⟩ merge and cancels both.
4. **Flush #2.** M0′ → `2_2_vct`, antimatter copied verbatim. Components:
   `[2_2, 1_1, 0_0]`.
5. **`COMPACT DATASET ds;`** → per-index compact jobs →
   `LSMIndexCompactOperatorNodePushable` → `scheduleFullMerge`. All three components merge;
   the set includes the oldest (`0_0`) → `returnDeletedTuples=false`.
6. **doMerge.** Three `VTreeSearchCursor`s in full-scan mode iterate clusters 0,1,2,… in
   lock-step. At cluster `c7`, the PQ pops `2_2`'s antimatter first (newest tie-break),
   holds it, sees `1_1`'s matter with the identical key → **cancellation**: neither reaches
   the bulk loader. Every other tuple streams through grouped by centroid into
   `VTreeBulkLoader` on `0_2_vct`; `end()` appends the static copy and records
   `staticBase + staticRoot` as root.
7. **Aftermath.** `subsumeMergedComponents` → components = `[0_2]`; `2_2`, `1_1`, `0_0`
   deactivate and are deleted once unreferenced. pk X has left no trace; `.staticstructure`
   is byte-identical to when it was trained.

Contrast: if step 5 had been a *policy* merge of only `[2_2, 1_1]` (oldest `0_0` excluded),
`returnDeletedTuples=true` — antimatter must be preserved because `0_0` could hold an older
matter twin. See §6 for why that path is currently suspect.

---

## 6. Invariants and review checklist

Check these on any patch touching flush/merge:

1. **Root arithmetic parity.** `VTreeFlushLoader#copyStaticStructure` returns
   `staticBasePageId + staticTree.getRootPageId()`; `VTreeBulkLoader#end` records
   `staticBasePageId + staticStructureRootPage`. These two must stay in lock-step — the
   flush-root bug was exactly this pair diverging
   ([bug archive](../60-quality/bug-archive.md), FIXED 2026-07-04, regression
   `LSMVTreeFlushSearchTest`).
2. **Self-contained components.** Every data component embeds its own pointer-adjusted
   static copy; only memory components navigate the shared `.staticstructure`. Anything
   that makes a disk component chase pointers into another file breaks recovery and the
   lazy-delete protocol.
3. **Identity mapping is a load-bearing assumption.** `copyPage` order = VBC page order.
   If flush ever skips or reorders pages, every directory pointer written from
   `centroidDirPageMap` (and every chain pointer inside copied pages) silently corrupts.
4. **Sorted/grouped input into `VTreeBulkLoader`.** `add()` assumes tuples arrive
   *contiguously grouped by centroidId* and distance-sorted within a cluster. The merge
   cursor guarantees this only because (a) every component's leaf enumeration lists the
   same centroids in the same order (same static structure ⇒ lock-step index *i* = same
   centroid, with `-1` sentinels holding empty positions), and (b) data pages/directories
   are sorted ([dml.md §6](dml.md) — the historical unsorted-directory-append bug).
   Revisiting an already-sealed cluster would overwrite `clusterFirstDirPageId` and orphan
   its directory pages.
5. **Antimatter drop rule.** Drop (reconcile) only when the merge includes the **oldest**
   disk component; otherwise preserve. Enforced solely in `LSMVTree#createMergeOperation`
   — `doFlush` never drops (byte copy), queries always reconcile.
6. **Antimatter *preservation* through a partial merge — FIXED (2026-07-07, red-green
   verified).** Pre-fix, with `returnDeletedTuples=true` the cursor handed antimatter
   tuples to `VTreeBulkLoader`, whose data frame came from the disk tree's single
   `dataFrameFactory` — wired in `LSMVTreeUtils.createLSMTree` to the **insert** (matter)
   tuple-writer. `LSMVTreeDataTupleWriter#writeTuple` only sets the antimatter bit from its
   own `isAntimatter` flag, so a preserved antimatter tuple was re-written as **matter** —
   a partial merge resurrected the delete. Unreachable under `NoMergePolicy` tests and
   under COMPACT (always full), but reachable under the production
   `size-bounded-concurrent` policy. **Fix** (LSMBTree's copy-tuple-writer pattern):
   `LSMVTreeCopyTupleWriter(Factory)` preserves the SOURCE tuple's antimatter bit;
   `LSMVTreeDiskComponent#createVTreeBulkLoader` installs it (via
   `createCopyDataFrameFactory()` → the `dataFrameFactoryOverride` parameter of
   `VTree#createComponentBulkLoader`/`VTreeBulkLoader`) **iff the operation is a MERGE**;
   initial bulk load keeps the matter writer. Regression:
   `LSMVTreeMergeAntimatterTest` (bulk load matter → flush filler → flush antimatter →
   `scheduleMerge` excluding the oldest → search; pre-fix the deleted PK resurrected).
   Archived in the [bug archive](../60-quality/bug-archive.md).
7. **Merge compare key included `quantized_distance` — FIXED (2026-07-07, red-green
   verified).** Pre-fix, `doMerge`'s default predicate left `pkStartField = 2`, so for
   quantized components the reconciliation comparator covered fields 2/3
   (`quantized_distance`, `quantized_embedding`) before the PKs. Field 2 has different
   write semantics on the two sides (bulk load: quantized-space distance; DML
   insert/delete: full-precision duplicate — see the field-2 entry in the
   [bug archive](../60-quality/bug-archive.md)), so a bulk-loaded matter tuple and its
   DML antimatter twin compared unequal and **missed cancellation during COMPACT** — the
   deleted PK survived (the orphan antimatter was dropped, `returnDeletedTuples=false`).
   **Fix**: `doMerge` now sets
   `mergePred.setPkStartField(VTreeDataTupleConstants.getPkStartField(isQuantized()))`
   (`LSMVTree#isQuantized()` = `quantizationParams != null ||
   dataTupleCreatorFactory.isQuantized()`), and `LSMVTreeSearchCursor`'s cancellation and
   priority-queue keys are now capped to **exactly ⟨distance (field 0), PK fields⟩** —
   `Math.min(cmp.length − pkStartField, numPrimaryKeyFields)` — so trailing INCLUDE fields
   are excluded from the key as well. Regression: `LSMVTreeCompactQuantizedTest`
   (quantized bulk load with field 2 in quantized space → DML delete → flush →
   `scheduleFullMerge` → search; pre-fix the deleted PKs were returned first). Archived in
   the [bug archive](../60-quality/bug-archive.md).
8. **Open bugs adjacent to this path** (memory notes /
   [3754b caveats](../80-patches/3754b-storage-layer-p2.md)): the delete-frame-corruption
   AIOOBE (`tryPhysicalDelete` under large buffer caches — note `VTree#tryInsertIntoDataPage`
   now carries a compact-before-insert fix for the fragmented `SUFFICIENT_SPACE` case) and
   the post-COMPACT recall anomaly at `fraction=0.4` (identical query fine pre-compact).
   The latter was NOT directly reproduced by the 6/7 regression tests: 6/7 make deleted
   PKs *survive* (extra tuples), not disappear. §6.7's failure mode can still contribute
   indirectly — resurrected deletes consume the K/candidate budget and push out true
   neighbors — so the recall anomaly should be re-tested now that 6/7 are fixed.
9. **Never re-upload a page** (cloud append-only rule): both loaders mutate confiscated
   copies in memory and write each page exactly once; keep it that way.
