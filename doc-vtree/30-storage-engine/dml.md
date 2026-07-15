# DML — insert, delete, and upsert through a VTree index

> **Status:** current
> **Verified against:** `111bfcd146` (2026-07-05)
> **Scope:** the complete end-to-end path of one INSERT / DELETE / UPSERT against a dataset
> with a VTree index — from the optimizer's maintenance branch down to the bytes on a data
> page, and what happens to those bytes at read, flush, and merge time.

Companions: [index-instance-anatomy.md](index-instance-anatomy.md) (component model, shared
static structure), [page-formats.md](page-formats.md) (byte layouts referenced throughout),
[3754a](../80-patches/3754a-storage-layer-p1.md) (single-tree engine),
[3754b](../80-patches/3754b-storage-layer-p2.md) (LSM wrapper, antimatter),
[creation-pipeline overview](../20-creation-pipeline/overview.md) (the bulk-load twin of this
path).

The one-sentence version: **a DML write hands the storage index a raw
`[vector, includes…, pk…]` tuple and the index does all the routing itself** — unlike bulk
load, where the dataflow pre-computes `[dist, cid, qDist, qEmbed, …]` before the loader ever
sees a tuple. Everything below is the consequence of that division of labor.

## 1. Compile time — the maintenance branch

`IntroduceSecondaryIndexInsertDeleteRule`
(`asterixdb/asterix-algebra/src/main/java/org/apache/asterix/optimizer/rules/IntroduceSecondaryIndexInsertDeleteRule.java`)
matches the `assign → insert-delete-upsert → SINK` pattern of every DML plan and splices one
`IndexInsertDeleteUpsertOperator` per secondary index between the primary-index modification
operator and the commit/sink. VTree indexes take the `case VTREE:` branches:

- **Field-access injection** (`injectFieldAccessesForIndexes`, `case VTREE:` ~lines 875–913):
  builds the secondary-key name list as **`[vector_field, include_1 … include_N]`**, resolving
  each type from the record schema via `recType.getSubFieldType(...)` — `ANY` for open fields
  — with `RECORD_INDICATOR` sources. This materializes an `AssignOperator` of field-access
  expressions above the primary op.
- **Key expressions** (main switch, `case VTREE:` ~lines 321–358): `secondaryKeyFields` =
  vector field first, then INCLUDE fields in declaration order (types/sources handled the
  same way). The PKs are *not* part of the secondary-key list — they ride on the operator as
  cloned `primaryKeyExpressions`; the runtime field permutation concatenates them, so the
  tuple the index ultimately receives is **`[vector, include…, pk…]`**.
- **The operator** (~lines 481–503): `IndexInsertDeleteUpsertOperator(dataSourceIndex,
  primaryKeyExprs, secondaryExpressions, filterExpression, …, operation, isBulkload, …)`. It
  inherits the DML kind (INSERT/DELETE/UPSERT) of the primary op and sits directly above it;
  multiple secondary indexes chain, each feeding the next, with the SINK on top.
- **Upsert prev-keys**: for `Kind.UPSERT` the rule runs `injectFieldAccessesForIndexes` a
  second time over `getBeforeOpRecordVar()` (the *previous* record, ~lines 284–293) and
  installs the result via `setBeforeOpSecondaryKeyExprs(...)` — so at runtime each frame
  tuple carries both the new and the old `[vector, include…]` projections side by side.

No vector-specific machinery exists at this layer beyond field selection: the injected
factories (vector accessor, distance function, quantizer) come from the index's persisted
`LSMVTreeLocalResource` — written at CREATE INDEX time by `VTreeResourceFactoryProvider` /
`SecondaryVectorOperationsHelper` (`asterixdb/asterix-metadata/.../utils/`) — not from the
DML plan (see [index-instance-anatomy §5](index-instance-anatomy.md)).

## 2. Runtime → LSM

Two operator flavors feed the index, depending on the statement kind:

- **INSERT / DELETE statements** run through `LSMInsertDeleteOperatorNodePushable`
  (`asterixdb/asterix-common/.../dataflow/LSMInsertDeleteOperatorNodePushable.java`), which
  extends the generic `LSMIndexInsertUpdateDeleteOperatorNodePushable`
  (`hyracks-storage-am-lsm-common/.../dataflow/`): per tuple, `lsmAccessor.tryInsert(tuple)`
  (or `tryDelete`) with a flush-partial-frame + blocking `insert`/`delete` retry when the
  memory component is full.
- **UPSERT statements** run through `LSMSecondaryUpsertOperatorNodePushable`
  (`asterixdb/asterix-runtime/.../operators/LSMSecondaryUpsertOperatorNodePushable.java`),
  which *decomposes* the upsert per tuple using the operation tag produced upstream:
  - `UPSERT_NEW` → `forceInsert(newTuple)`;
  - `UPSERT_EXISTING` → compare new vs. prev secondary tuples
    (`TupleUtils.equalTuples(tuple, prevTuple, numberOfFields)`); if different,
    `forceDelete(prevTuple)` **then** `forceInsert(newTuple)`; if byte-equal, no-op;
  - `DELETE_EXISTING` → `forceDelete(prevTuple)`.

  Consequence: **the storage index never sees an UPSERT operation.**
  `LSMVTree#modify`'s default case forwards to `VTreeAccessor#upsert`, which throws
  `UnsupportedOperationException` — dead by construction, kept as a tripwire.

The accessor call lands in `LSMHarness#modify` / `#forceModify`
(`hyracks-storage-am-lsm-common/.../impls/LSMHarness.java`): allocate memory components if
needed, `getAndEnterComponents` (opTracker admission, component latching; the current mutable
component is always first in `ctx.getComponentHolder()`), then `lsmIndex.modify(ctx, tuple)`,
`mutableComponent.setModified()`, and `exitComponents` — which is where a full memory
component gets its flush scheduled.

`LSMVTree#modify`
(`hyracks-fullstack/hyracks/hyracks-storage-am-lsm-vtree/.../lsm/vector/impls/LSMVTree.java`,
~line 309) routes on the op-context's operation:

```java
switch (ctx.getOperation()) {
    case PHYSICALDELETE: ctx.getCurrentMutableVTreeAccessor().delete(indexTuple); break;
    case INSERT:         insert(indexTuple, ctx); break;   // → accessor.insert
    case DELETE:         delete(indexTuple, ctx); break;   // → accessor.delete
    default:             ctx.getCurrentMutableVTreeAccessor().upsert(indexTuple); // throws
}
```

`LSMVTreeOpContext` (`.../impls/LSMVTreeOpContext.java`) pre-creates one
`VTree.VTreeAccessor` + `VTreeOpContext` per memory component at construction;
`setCurrentMutableComponentId` picks the active pair **and flips the data-frame polarity**:
INSERT/UPSERT → `setInsertMode()` (matter `VTreeDataFrameFactory`), DELETE/PHYSICALDELETE →
`setDeleteMode()` (antimatter factory). This flip is the *only* thing that distinguishes the
bytes an insert and a delete eventually write (§3.5). Note `PHYSICALDELETE` and `DELETE`
converge on the same `VTreeAccessor#delete` → `deleteVector` code path.

## 3. Inside VTree — `insertVector` / `deleteVector`

Everything below is `hyracks-fullstack/hyracks/hyracks-storage-am-vtree/.../vector/impls/VTree.java`
running against the **current mutable memory component** (a VTree over a virtual buffer
cache; layout in [index-instance-anatomy §3](index-instance-anatomy.md)).

### 3.1 Vector extraction

`VTreeTupleUtils.extractVectorFromTuple(tuple, 0, vectorAccessorFactory)` — the injected
`IVTreeBinaryAccessorFactory` (production: `AOrderedListVectorBinaryAccessorFactory`, from
the resource JSON) decodes field 0 of the operator tuple into a `double[]`. A null vector is
an `INDEX_NOT_UPDATABLE` error.

### 3.2 `findReplicaClusters` — routing (~line 1040)

Returns the list of leaf clusters this record belongs to:

- **M = 1 (legacy, `CrossPollinationConfig.LEGACY = (m=1, rngFactor=1.0, epsilon=0.3)`)**:
  `findClosestClusterFromRoot` — single greedy root-to-leaf descent
  (`VTreeNavigationUtils#findClosestCentroid`).
- **M > 1 (cross-pollination)**: `findCloseCentroidsLevelWiseGlobalSortFromRoot(vector,
  distanceFunction, epsilon)` gathers epsilon-windowed candidates level by level, then
  `RngAcceptanceFilter.accept(candidates, distanceFunction, rngFactor, m, null)` applies the
  SPTAG relative-neighborhood rule — candidate `cᵢ` is vetoed when an already-accepted
  replica `r` satisfies `rngFactor · dist(cᵢ, r) < dist(x, cᵢ)` — keeping up to M *diverse*
  clusters. Falls back to the single closest if everything is vetoed.

`m` / `rngFactor` / `epsilon` come from the `CrossPollinationConfig` record threaded through
the resource JSON (persisted only when M > 1). **The lock-step invariant:** Job 3's
`VTreeBulkLoaderAndGroupingOperatorDescriptor` routes with the *same*
`findCloseCentroidsLevelWiseGlobalSort` + RNG-filter pair over the *same immutable* static
structure with the *same* config — so bulk load, insert, and delete always compute the
identical replica set for a given vector (§6).

### 3.3 `prepareClusterAccess` — resolving the directory page (~line 1073)

Per replica cluster, the write needs the cluster's first **directory** page. Two branches:

- **Memory component** (`staticBufferCache != null`): navigation above ran against the
  **shared `.staticstructure` component's** pages — `VTreeNavigationUtils` was handed
  `staticBufferCache` / `staticFileId` / `staticRootPage`, cached by
  `VTree#setStaticStructure` at component wiring time. The resulting centroid id is then
  mapped **locally**: `metadataPageId = centroidDirPageMap[centroidId −
  firstLeafCentroidIdMem]` — one pre-created empty VBC directory page per leaf centroid, no
  leaf page pinned at all. (Full wiring in
  [index-instance-anatomy §4](index-instance-anatomy.md).)
- **Disk component** (bulk-load path / merge target): pin the leaf page navigation found,
  read the tuple's `metaPtr` field at the cluster index, unpin.

Then the per-replica distance: `distance = distanceFunction.apply(vector,
clusterResult.centroid)` — **to that replica's own centroid**, so each stored copy's sort key
is self-consistent within its cluster.

### 3.4 Insert — tuple creation and `insertIntoDataPages` (~line 224)

**Tuple creation** — `VTreeDataTupleCreator`
(`.../vector/impls/VTreeDataTupleCreator.java`), quantized (production) layout, field order
pinned by `VTreeDataTupleConstants`:

| Field | Content | DML-path source |
|---|---|---|
| 0 `distance` | raw float64 | just computed |
| 1 `centroid_id` | raw int32 | the replica cluster's cid |
| 2 `quantized_distance` | raw float64 in a var-len field | **duplicate of field 0** — *not* a quantized-space distance; see the field-2 semantics entry in the [bug archive](../60-quality/bug-archive.md) (bulk load writes the real quantized distance here; nothing reads it) |
| 3 `quantized_embedding` | `[varint len][bytes]` | `quantizeVector()` with the resource's `float[6]` quantization constants |
| 4… | PKs, then INCLUDEs | copied ADM-verbatim from the operator tuple |

Byte-exact encoding in [page-formats §6](page-formats.md). The antimatter bit is **not** the
creator's business — it is added at page-write time by whichever frame the op-context armed
(§3.5).

**`insertIntoDataPages(metadataPageId, vector, distance, centroidId, tuple, ctx)`** — the
write-side descent:

1. **Directory chain walk.** Pin + write-latch each directory page in the `next_page` chain
   (guarded against cycles at 100 hops). On each page,
   `findDataPageInMetadataPage(frame, distance, isLastInChain)` scans the
   `max_distance`-ascending entries for the first with `distance ≤ max_distance`; on the
   *last* page of the chain the last data page acts as **catch-all** (matter, antimatter, and
   physical-delete lookups all share this rule — the routing must be identical or deletes
   miss their twin). Non-last page with no match → follow `next_page`.
2. **`tryInsertIntoDataPage`** (~line 329): pin the data page,
   `hasSpaceInsert(dataTuple)` — with contiguous or compactable space,
   `VTreeDataFrame#findInsertPosition(distance)` (a **right-boundary** binary search: equal
   distances stay FIFO, which is what makes "rightmost match = most recent" true for
   deletes) → `insert(dataTuple, insertIndex)` → bump page LSN →
   `updateMetadataMaxDistanceIfNeeded()` **in-place** rewrites the directory entry's
   `max_distance` if this tuple extended the page's range (`VTreeMetadataFrame#updateMaxDistance`).
3. **Page overflow** — `splitDataPageMaintainOrder` (~line 408): `freePageManager.takePage()`
   a new data page, BTree-style `split(newFrame, tuple, index)` (mirror buffer, shift right
   half, compact both, re-derive insert position), relink the chain
   (`old.next → new`, `new.next → old's former next`), then
   `updateMetadataAfterDataSplit` (~line 450): re-read both halves' true max distances from
   their last (sorted) tuples, `forceUpdateMetadataMaxDistance` on the original entry (may
   *decrease*), and `updateMetadataWithNewDataPage` (~line 722) for the new page — a
   **sorted insert** via `VTreeMetadataFrame#findInsertPosition(maxDistance)`. That sorted
   insert *is* the fix behind the antimatter-reconciliation bug: appending unsorted directory
   entries produced unsorted data-page chains, breaking the k-way merge's sorted-input
   precondition ([3754a §frames](../80-patches/3754a-storage-layer-p1.md)). A full directory
   page splits too (`handleMetadataPageOverflow` → `VTreeMetadataFrame#split`, chain
   extended).
4. **No data page at all** (fresh cluster in a memory component — directory pages start
   empty) → `handleDataPageOverflow` allocates the first data page and its directory entry.

### 3.5 Delete — `deleteVector` (~line 510)

Extraction: the vector as above, plus
`VTreeTupleUtils.extractPrimaryKeyFromTuple(tuple)` — PK captured as **raw bytes**, no type
assumption. Then, **for each of the same `findReplicaClusters(vector)`** (the in-code comment
is explicit: a cross-pollinated record was replicated into every accepted cluster, so a
delete must reconcile in every one of them or the surviving replicas resurface in ANN
results):

1. `prepareClusterAccess` + per-cluster distance, exactly as insert.
2. **`tryPhysicalDelete(metadataPageId, distance, primaryKey, tuple, ctx)`** (~line 545):
   walk the directory chain with the same `findDataPageInMetadataPage` routing; in the one
   data page whose range covers `distance`, run
   `VTreeDataFrame#findTupleByDistanceAndPrimaryKey(distance, primaryKey, pkFieldIndex)` —
   binary search to the right boundary of the distance run, then scan **backwards**
   (most-recent-first, by the FIFO invariant) comparing PK bytes with `Arrays.equals`.
   Found → `ctx.getDataFrame().delete(tuple, tupleIndex)` removes it **in place** — the
   common case when insert and delete land in the same unflushed memory component.
3. **Not found → write antimatter**: the identical `insertIntoDataPages(...)` call, but the
   op-context is in delete mode, so `ctx.getDataFrame()` is the **antimatter**
   `VTreeDataFrame` whose `LSMVTreeDataTupleWriter`
   (`hyracks-storage-am-lsm-vtree/.../lsm/vector/tuples/LSMVTreeDataTupleWriter.java`) sets
   the high bit (`0x80`, `ANTIMATTER_BIT_OFFSET = 7`) of null-flag byte 0 after the base
   write. The antimatter tuple is **byte-identical to the matter tuple except that one bit**
   — same distance, same cid, same PK — and is sorted into the data page like any insert
   ([page-formats §3.2](page-formats.md)).

The javadoc's three scenarios: (1) not found → antimatter; (2) found as matter → physical
delete; (3) found after an antimatter was superseded by a re-insert → physical delete of the
newest match. Note `PHYSICALDELETE` runs this same method, so even a "physical" delete
degrades to antimatter when the target lives in an older component.

## 4. What happens to those tuples later

### 4.1 Query-time cancellation — adjacency reconciliation

`LSMVTreeTopKSearchCursor` (`.../lsm/vector/impls/LSMVTreeTopKSearchCursor.java`), the sole
query cursor, merges per-component streams cluster-by-cluster through a priority queue keyed
by **⟨distance, PK⟩**, memory (newest) components ordered ahead of disk. Because insert and
delete used the *same* routing over the *immutable* static structure and the *same* injected
distance function, the antimatter twin has the **identical key** — so matter and antimatter
for one logical record become **adjacent** in the merged order. `getNextValidTuple()` holds
an antimatter tuple, compares it with the next queue head: key match → both vanish; mismatch
→ the antimatter is discarded alone (an orphan: its matter was already physically deleted or
never in these components). Sorted data pages + sorted directories are the precondition that
each component's stream really is key-ordered.

### 4.2 Flush — identity copy, antimatter preserved

`LSMVTree#doFlush` + `VTreeFlushLoader`: every VBC page is copied to the new disk component
with **identity page mapping** — matter and antimatter tuples travel byte-for-byte, still
sorted; `centroidDirPageMap` values remain valid as disk directory page ids. The static
structure is appended at the tail with pointers offset by `staticBasePageId`, each leaf's
`metaPtr` rewritten from `centroidDirPageMap`, and the component root recorded as
`staticBasePageId + staticTree.getRootPageId()` — the corrected root arithmetic (see the
flush-root FIXED entry in the [bug archive](../60-quality/bug-archive.md); it used to record
the first copied static page, a *leaf*). A flush is O(pages) with zero routing work: all
clustering effort was paid at insert time.

### 4.3 Merge — physical removal

`LSMVTree#doMerge` is "search everything, bulk-load the survivors": a full-scan
`LSMVTreeSearchCursor` (`fullScanMode=true`, `SequentialClusterSelectionStrategy`,
epsilon 0) runs the same k-way merge + adjacency reconciliation over the merging components
and feeds the surviving matter tuples into a regular `VTreeBulkLoader` on the merge target.
Matter/antimatter pairs are physically dropped here; **unmatched antimatter is retained
unless the merge includes the oldest disk component** (`createMergeOperation`) — it may still
have a matter twin in an older, non-participating component.

## 5. Worked example — insert then delete, M = 2

Setup: quantized index, euclidean, `cross_pollination_m = 2`. Record pk `"X"`, vector `v`.

**Insert.** `findReplicaClusters(v)` level-walks the shared static structure: candidates
within epsilon are `{cA: d=0.42, cB: d=0.57, cC: d=0.58}`; RNG accepts cA, accepts cB,
vetoes cC (too close to cB relative to its distance from `v`). Two matter tuples are created
and inserted into the memory component's VBC:

- cluster cA: `⟨0.42, cidA, 0.42₍dup₎, q(v), "X"⟩` → directory page
  `centroidDirPageMap[cidA − first]`, data page for range covering 0.42, sorted position by
  binary search;
- cluster cB: `⟨0.57, cidB, 0.57₍dup₎, q(v), "X"⟩` — same mechanics, cB's own distance.

**Flush.** Both tuples are identity-copied into disk component `1_1_vct`; the static copy is
appended; roots and directory pointers patched. The recycled memory component re-runs
`setStaticStructure` and starts empty.

**Delete pk X.** The delete tuple carries the same `v` (upsert/delete pipelines project the
*previous* record's vector — §1's prev-keys). `findReplicaClusters(v)` reproduces exactly
`{cA, cB}` (same config, same immutable tree). In the *new* memory component,
`tryPhysicalDelete` finds nothing in either cluster (both directory chains are empty of
"X") → two **antimatter** tuples are written: `⟨0.42, cidA, …, "X"⟩•` and
`⟨0.57, cidB, …, "X"⟩•` (• = bit 0x80 set).

**Query before merge.** Top-k search over `[memory, 1_1_vct]`: in cluster cA the merged
stream yields memory's antimatter `⟨0.42, "X"⟩•` then disk's matter `⟨0.42, "X"⟩` — adjacent,
identical key → both cancelled. Same in cB at key `⟨0.57, "X"⟩`. pk X is invisible; no other
record is affected.

**Merge.** After the antimatter component flushes, a merge that includes the oldest component
runs the full-scan cursor: both pairs cancel during the merge's k-way pass, the bulk loader
never sees them, and the merged component contains no trace of pk X.

## 6. Invariants and pitfalls

- **Routing must be config-identical across bulk load, insert, and delete.** Same
  `CrossPollinationConfig` (m/rngFactor/epsilon), same distance function, same static
  structure. History: DML originally wrote/deleted only the single closest cluster while
  bulk load replicated into M — deletes "leaked" and cancelled nothing in the other M−1
  replicas (the cross-pollination DML multi-cluster fix). Any future change to routing must
  change all three call sites in lock step, and only between full rebuilds of the index.
- **Sorted pages are a correctness precondition, not an optimization.** Data pages sorted by
  distance (FIFO among equals) and directories sorted by `max_distance` are what make (a) the
  k-way merge streams ordered, hence (b) matter/antimatter adjacency, hence (c) cancellation.
  The historical unsorted-directory-append bug broke exactly this chain
  ([3754a](../80-patches/3754a-storage-layer-p1.md)).
- **Identical key requires identical arithmetic.** The stored distance is recomputed at
  delete time; determinism holds because the same injected `IVTreeDistanceFunction` runs on
  the same full-precision inputs. Do not "optimize" either side to a quantized or otherwise
  approximated distance independently.
- **Field 2 (`quantized_distance`) lies on the DML path** — it duplicates the full-precision
  distance instead of the quantized-space one, unlike bulk load. Harmless today (zero
  readers), a trap for future pruning — OPEN entry in the
  [bug archive](../60-quality/bug-archive.md).
- **Open bug touching this path: delete-frame corruption.** An
  `ArrayIndexOutOfBoundsException` in `tryPhysicalDelete` /
  `VTreeDataFrame#findTupleByDistanceAndPrimaryKey`, exposed only with a large
  `storage.buffercache.size`; suspected interaction with the in-place insert path (tracked in
  the memory bug notes and [3754b caveats](../80-patches/3754b-storage-layer-p2.md); also
  related: the post-COMPACT recall anomaly at fraction = 0.4).
- **UPSERT never reaches the tree.** It is decomposed into forceDelete(prev) + forceInsert(new)
  by `LSMSecondaryUpsertOperatorNodePushable`; `VTreeAccessor#upsert` throws. If a new
  pipeline ever routes a genuine UPSERT op to `LSMVTree#modify`, it will hit that throw.
- **PHYSICALDELETE is best-effort physical.** It shares `deleteVector`, so when the target
  tuple lives in an already-flushed component it silently degrades to an antimatter write.

## Related

- [3754a — storage layer p1](../80-patches/3754a-storage-layer-p1.md): single-tree
  insert/delete engine, navigation utils, frame mechanics
- [3754b — storage layer p2](../80-patches/3754b-storage-layer-p2.md): LSM wrapper,
  antimatter encoding, cursors, flush/merge
- [20-creation-pipeline/overview.md](../20-creation-pipeline/overview.md): the bulk-load twin
  (Job 3 routing that this path must stay lock-step with)
- [page-formats.md](page-formats.md) / [index-instance-anatomy.md](index-instance-anatomy.md):
  bytes and component model
- [bug-archive.md](../60-quality/bug-archive.md): flush-root fix, field-2 semantics, latent
  risks
