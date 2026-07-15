# Search cursors — from the operator pushable to the LSM top-K merge

> **Status:** current
> **Verified against:** `56881406e7` (2026-07-05)
> **Scope:** the runtime read path inside `hyracks-storage-am-lsm-vtree`: how one input tuple
> becomes a stream of `[pk…(, dist)]` candidates — predicate wiring, the top-K cursor's blocked
> search, the spillable buffer, and cluster advancement.

Middle file of the query-path chain: **[optimizer.md](optimizer.md) → search-cursors.md →
[navigation.md](navigation.md)**. Cursor overview + design theses:
[3754b §the-two-cursors](../80-patches/3754b-storage-layer-p2.md); antimatter write-side:
[dml.md](../30-storage-engine/dml.md). This doc goes below both.

All paths below are under
`hyracks-fullstack/hyracks/hyracks-storage-am-lsm-vtree/src/main/java/org/apache/hyracks/storage/am/lsm/vector/`
unless noted. Quantized data-tuple layout used throughout
(`VTreeDataTupleConstants`, `hyracks-storage-am-vtree/.../vector/utils/`):

```
field 0: distance_to_centroid (float64)     ── the sort/merge key
field 1: centroid_id (int32)
field 2: quantized_distance (float64)       ── write-side artifact, unread here
field 3: quantized_embedding ([len][bytes]) ── dqx is computed from this
field 4…: PKs, then INCLUDE fields          ── pkStartField = 4 (quantized)
```

## 1. `VectorSearchOperatorNodePushable` (`dataflow/`)

Extends the generic `IndexSearchOperatorNodePushable`
(`hyracks-storage-am-common/.../dataflow/`), which owns the frame loop: `nextFrame(buffer)` →
for each input tuple `i`: `resetSearchPredicate(i)` → `indexAccessor.search(cursor, searchPred)`
→ `writeSearchResults(i, cursor)` → cursor close. One ANN search per input tuple (the input is
the one-row search-key ASSIGN frame from [optimizer.md §2.1](optimizer.md)).

The constructor deliberately zeroes out the generic machinery — no min/max filter fields, no
operator-level `tupleFilterFactory`, `outputLimit = -1`, no proceed-result callback — because
**filtering and K-limiting happen inside the cursor**, where they can count toward K correctly.
The projector is a `PKOnlyTupleProjectorFactory(numSecondaryKeys, numPrimaryKeys)`.

### 1.1 Predicate construction and per-tuple reset

`createSearchPredicate()` returns a bare `VTreeSearchPredicate`
(`hyracks-storage-am-vtree/.../vector/impls/`) — a *marker*: no deserialization happens in the
operator. `resetSearchPredicate(tupleIndex)` points a `PermutingFrameTupleReference`
(permutation = `queryFields` from the POperator) at the current input tuple and fills the
predicate:

| predicate field | source | extraction detail |
|---|---|---|
| `queryTuple` / `queryFieldIndex=0` | permuted tuple ref | raw bytes; decoded later by the injected vector accessor inside `VTree.search` |
| `k` | field 1 | `IntegerPointable.getInteger(data, start + 1)` — the `+1` skips the ADM type tag |
| `distanceMetric` | field 2 | `UTF8StringPointable` at `start + 1`; any failure → `"euclidean"` |
| `minProbeFraction` | field 3 | `DoublePointable` at `+1`; only applied when > 0 (predicate clamps to (0,1], ≤0 → default 0.1) |
| `kMultiplier` | field 4 | `IntegerPointable` at `+1`, `max(1, …)`; then **overridden by the session `compiler.vector.kmultiplier` when that is > 1** |
| `tupleFilter` | `tupleFilterFactory.createTupleFilter(ctx)` (created in `open()`) | pushed-filter compiled at codegen |
| `epsilon` | constructor `indexEpsilon` (index WITH, default 0.3) | not query-settable |
| `pkStartField` | constructor `numSecondaryKeys` (4 quantized / 2 not) | |

### 1.2 IAP keys — `addAdditionalIndexAccessorParams`

The generic pushable creates the LSM accessor with `IIndexAccessParameters`; this subclass adds:

| key | value | consumer |
|---|---|---|
| `IVTreeBinaryAccessorFactory.IAP_KEY` | `AOrderedListVectorBinaryAccessorFactory` | `VTree.search` decodes the query vector without asterix types leaking down |
| `IVTreeDistanceFunctionFactory.IAP_KEY` | `VectorDistanceFunctionFactory` | `VTree.search` builds the metric's `IVTreeDistanceFunction` (falls back to the index-persisted factory if absent) |
| `IVTreeQuantizerFactory.IAP_KEY` | `OptimizedScalarQuantizerFactory` (nullable) | `VTree.search` builds a per-query quantizer from the persisted `float[6]` params |
| `HyracksConstants.HYRACKS_TASK_CONTEXT` | task ctx | `SpillableTopKBuffer` (frame allocation + managed spill files) |
| `LSMVTreeTopKSearchCursor.IAP_KEY` (`"USE_TOPK_SEARCH"`) | `TRUE` | `LSMVTreeIndexAccessor` cursor selection |

### 1.3 Output tuple layout — `getFieldCount` / `writeTupleToOutput`

`getFieldCount = numPrimaryKeys + (indexOnly ? 1 : 0)`. Per emitted tuple:

1. `PKOnlyTupleProjector.project(tuple, dos, tb)` copies storage-tuple fields
   `[numSecondaryKeys, numSecondaryKeys + numPrimaryKeys)` — the PK bytes, ADM-verbatim.
2. **Index-only:** append one field, `[byte 12 = ADOUBLE type tag][8-byte IEEE-754 double]`,
   read from the *active cursor* via `IVectorSearchCursor.getCurrentDistance()` — the tag value
   is hardcoded because Hyracks cannot depend on asterix-om; the algebra side declared `$$dist`
   as `BuiltinType.ADOUBLE` to keep the wire convention in sync. `writeSearchResults` stashes the
   cursor in `activeCursor` around the super call so this override can reach it. A `NaN`
   distance (i.e., the streaming cursor, which doesn't track per-tuple dqx) → hard `IOException`
   — the optimizer should never have picked index-only for that configuration.

## 2. Cursor selection — `LSMVTreeIndexAccessor` (`impls/`)

`createSearchCursor(exclusive)`: if the op-context's IAP has `USE_TOPK_SEARCH == TRUE` →
`lsmVTree.createTopKSearchCursor(ctx)` (a `LSMVTreeTopKSearchCursor`). Otherwise the parent
`LSMTreeIndexAccessor` path uses the registered `cursorFactory = LSMVTreeSearchCursor::new`.

**The streaming `LSMVTreeSearchCursor` remains reachable in exactly two ways** (per the
quantized-only release decision it is *merge-only* product-wise):

1. `LSMVTree#doMerge` constructs it directly:
   `new LSMVTreeSearchCursor(opCtx, returnDeletedTuples, /*fullScan*/ true, stats)` — sequential
   cluster iteration, antimatter visible, drained into the merge target's bulk loader.
2. Any accessor whose IAP lacks the flag — in practice test fixtures that verify inserts/deletes
   through full iteration. Production queries always set the flag (§1.2).

`accessor.search(...)` goes through `LSMHarness.search` → `LSMVTree#search`, which just resets
the pre-created `LSMVTreeCursorInitialState` (frame factories, `MultiComparator` from
`cmpFactories`, harness, callbacks) with the predicate + `ctx.getComponentHolder()` — the
operational components, **mutable memory component first, then disk components newest→oldest** —
and calls `cursor.open(initialState, pred)`.

## 3. `LSMVTreeTopKSearchCursor` (`impls/`, ~750 lines)

Blocked design: **everything happens in `open()`**; `hasNext()/next()/getTuple()` only drain a
pre-computed, dqx-sorted result buffer. Implements `IVectorSearchCursor` so the pushable can read
per-tuple `D(q,x)`.

### 3.1 `open()` — step by step

1. Reset stats; read `cmp` (the `MultiComparator`) and `operationalComponents` from the initial
   state; `numComponents = |components|`.
2. From the predicate: `K`, `candidateLimit = K × max(1, kMultiplier)`, `epsilon`,
   `pkStartField`, `tupleFilter` (+ a `ReferenceFrameTupleReference` wrapper if set).
   `nprobe` starts at 1 — the strategy computes the real value later.
3. From the IAP: vector-accessor factory → `vectorAccessor`; task context →
   `topKBuffer = new SpillableTopKBuffer(candidateLimit, ctx)`.
4. `clusterStrategy = new NprobeClusterSelectionStrategy(minProbeFraction, epsilon)`.
5. Per component: `vTreeAccessors[i] = component.getIndex().createAccessor(iap)`,
   `rangeCursors[i] = accessor.createSearchCursor(false)` (a per-component `VTreeSearchCursor`
   in query mode).
6. `IndexCursorUtils.open(vTreeAccessors, rangeCursors, searchPred)` — each component's
   `VTreeAccessor.search` extracts the query vector (via the injected accessor factory), builds
   the distance function and per-query quantizer, and opens its `VTreeSearchCursor`, which runs a
   **DFS to its own closest cluster** and positions on that cluster's first data page
   ([navigation.md §2](navigation.md)).
7. From `rangeCursors[0]` (`firstSearchCursor`): `queryVector`, `distanceFunction`,
   `quantizedQueryVector` (= `quantizer.quantize(queryVector)`, a lossy double[] round-trip),
   `quantizer`. Null query vector → hard error.
8. `clusterStrategy.initialize(firstComponentVTree, queryVector, distanceFunction,
   candidateLimit)` — runs the **level-wise global-sort navigation** over the first component's
   navigation tree (`vTree.getNavigationBufferCache()/FileId/RootPageId` — the shared
   `.staticstructure` when component 0 is a memory component) and computes `nprobe` (§4).
   Note the strategy's "K" is `candidateLimit`, not the user's K.
9. `clusterStrategy.setFirstCursorForDFS(firstSearchCursor)`; the strategy's shared
   `visitedCentroidIds` set is handed to every component cursor
   (`setSharedVisitedSet`) so DFS never revisits a level-wise cluster and vice versa.
10. **Consistency re-open:** step 6's per-cursor DFS may have landed on a different cluster than
    the level-wise list's head (level-wise[0] is the global closest; DFS is greedy per
    component). If `dfsCluster.centroidId != firstCluster.centroidId`, every cursor is
    `resetClustersProbed()` + `openClusterByResult(firstCluster)` — all components always scan
    the *same* cluster in lock step.
11. `initPriorityQueue()` — one `PriorityQueueElement` per component; prime the queue with each
    cursor's first tuple (`hasNext`/`next`/`getTuple`); components with an empty first cluster
    are marked `clusterExhausted`. `clustersExplored = 1`; if *all* components were empty,
    immediately `advanceAllComponentsToNextCluster()`.
12. `performBlockedSearch()` (§3.3).
13. `drainIterator = topKBuffer.drain()` — from here on the cursor is a dumb iterator.

### 3.2 The merge key

`NaivePriorityQueueComparator` (and the identical `compare()` used for antimatter matching)
orders elements by:

1. **field 0** — `distance_to_centroid`, compared with `cmp.getComparators()[0]`;
2. **PK fields** — comparators `[pkStartField + i]` applied to tuple fields
   `pkStartField + i` (the intervening quantized fields are *skipped*, they are not part of the
   key);
3. **componentId ascending** — memory (index 0) before disk, newer disk before older.

⟨distance, PK⟩ equality is exactly what an antimatter twin reproduces (same routing, same
injected distance function ⇒ bit-identical stored distance; see
[dml.md §4.1](../30-storage-engine/dml.md)), so a delete and its target become **adjacent** in
the merged stream, with the newer (antimatter) element first by the component tie-break.

### 3.3 `performBlockedSearch()` — the outer loop

```
loop:
  while PQ non-empty (or a held antimatter pending):
      t = getNextValidTuple()                 // §3.4 — reconciled matter tuple
      if t passes tupleFilter:                // §3.5
          dqx = computeApproximateDistance(t) // §3.6
          topKBuffer.insert(t, dqx)           // §5
  if stopAdvancing: break
  if clusterStrategy.shouldStopAdvancing(minClustersProbed, topKBuffer.numEntries): break
  if !clusterStrategy.hasMoreClusters(): break
  if current cluster contributed 0 valid tuples:
      every cursor .decrementClustersProbed() // empty clusters don't count toward nprobe
  advanceAllComponentsToNextCluster()          // §3.7
```

`minClustersProbed` = the minimum `getClustersProbed()` across component cursors — the budget is
only "spent" when every component has actually scanned that many clusters.

### 3.4 `getNextValidTuple()` — antimatter three-way logic

State: `outputElement` (a held antimatter) + `needPushElementIntoQueue`.

- **No held element, head is matter** → poll, `TupleUtils.copyTuple` (the frame underneath is
  about to be advanced), refill that component's queue slot
  (`pushIntoQueueAndAdvanceClusterIfNeeded`: cursor `hasNext`? push next : mark
  `clusterExhausted[i]`), return the copy.
- **No held element, head is antimatter** (`ILSMTreeTupleReference.isAntimatter()`, the 0x80
  flag bit) → poll and *hold* it; loop.
- **Held antimatter vs. new head:**
  - `compare(...) == 0` (same ⟨distance, PK⟩) → **cancellation**: poll the head too, refill both
    components' slots, drop both tuples, count `antimatterCancellations`.
  - different key → the antimatter is an **orphan** (its matter twin was physically deleted
    in-place, or lives only in an older non-participating component): refill its slot, discard it
    alone, and re-examine the head.
- Queue empty but an element is still held → refill from its component and loop; truly empty →
  return null (cluster done in all components).

### 3.5 Filter application point

`passesTupleFilter` runs **after** reconciliation and **before** the buffer insert — a tuple
deleted by antimatter never reaches the filter, and a filtered-out tuple never occupies a top-K
slot nor counts toward `validTuplesFromCurrentCluster`. The filter sees the *full storage tuple*
(distance, centroid, quantized fields, PKs, INCLUDEs) through the offset schema compiled at
codegen ([optimizer.md §4.1](optimizer.md)).

### 3.6 `computeApproximateDistance` — dqx

Field 3 (`Q_QUANTIZED_EMBEDDING_FIELD`) holds `[varlen-prefix][quantized bytes]`
(`ByteArrayPointable` encoding). The prefix is stripped, `quantizer.dequantize(bytes)` produces a
lossy `double[]`, and

```
dqx = distanceFunction.apply(quantizedQueryVector, dequantized)
```

Both operands live in the same "quantize-then-dequantize" space (the query vector went through
`quantizer.quantize()`, which per `IVTreeQuantizer` returns the lossy double[] round-trip), so
the ranking error is symmetric. `dqx` — not the stored field-0 distance-to-centroid — is the
candidate ranking key. This cursor assumes the quantized layout unconditionally (it is the
quantized-only release's sole query cursor).

### 3.7 Cluster advancement

`advanceAllComponentsToNextCluster()` loops:

1. `clusterStrategy.getNextCluster()` → null ⇒ mark everything exhausted, `stopAdvancing`.
2. Otherwise open that cluster on **every** component
   (`VTreeSearchCursor.openClusterByResult(cluster)` — each component re-resolves the
   directory page *locally*, see [navigation.md §2.4](navigation.md)) and prime its first tuple
   into the PQ; `clustersExplored++`.
3. If *all* components turned out empty for this cluster, loop to the next one (still counting
   `clustersExplored`, but the per-cursor `clustersProbed` for the empty cluster is later
   rolled back by the §3.3 decrement).

## 4. `NprobeClusterSelectionStrategy` + the `IClusterSelectionStrategy` contract (`impls/`)

The contract (`IClusterSelectionStrategy`): `initialize(vTree, queryVector, distFunc, k)` once;
then a pull loop of `getNextCluster()` / `hasMoreClusters()`; `shouldStopAdvancing(minProbed,
resultsCollected)` as the budget check; `getVisitedCentroidIds()` shared for cross-component
dedup; `getFirstCluster()` for the lock-step re-open; `setFirstCursorForDFS` for the fallback
phase; `setQuantizer` (default no-op) to enrich results with quantized D(q,C);
`reset()`. Implementations: `NprobeClusterSelectionStrategy` (queries) and
`SequentialClusterSelectionStrategy` (merge full-scan, used by the streaming cursor).

`NprobeClusterSelectionStrategy` internals:

- **`initialize`** (only when `queryVector != null && epsilon > 0 && vTree != null`): calls
  `VTreeNavigationUtils.findCloseCentroidsLevelWiseGlobalSort(navBC, navFileId, navRoot, …,
  epsilon, quantizedQueryVector, quantizer)` — the epsilon-windowed, globally sorted candidate
  list ([navigation.md §4](navigation.md)). Then
  **`nprobe = max(1, floor(list.size() × minProbeFraction))`** — note the base is the
  *epsilon-filtered candidate count*, not the total leaf-cluster count (the local variable is
  named `totalLeafClusters`, misleadingly). `min_probe_fraction = 1.0` therefore means "probe
  every cluster in the epsilon window", not "probe every cluster". The head of the list is
  pre-marked visited and `globalClusterIndex` starts at 1 (the cursor consumed index 0 via
  `getFirstCluster()`). Any exception → `globalLevelWiseClusters = null` (DFS-only operation).
- **`getNextCluster`** — phase 1: next level-wise entry (marks visited); when the list is
  exhausted, phase 2: `firstCursor.findNextClusterDFS()` — the first component's iterative-DFS
  navigation state yields ever-farther leaf clusters, skipping the shared visited set. So DFS is
  the *overflow valve* when nprobe demands more clusters than the epsilon window contains.
- **`shouldStopAdvancing(minClustersExplored, resultsCollected)`** =
  `minClustersExplored >= nprobe && resultsCollected >= K` — where `K` here is
  `candidateLimit = user-K × kMultiplier` (what the cursor passed to `initialize`). Both
  conditions must hold: enough clusters *and* enough candidates.
- **`setQuantizer`**: implemented, but **never called** by `LSMVTreeTopKSearchCursor` — the
  cursor extracts `quantizedQueryVector`/`quantizer` from the first component cursor *after*
  strategy construction yet never forwards them, so the strategy always passes `null, null` and
  every `ClusterSearchResult.quantizedDistance` on the query path is `NaN`. Nothing currently
  reads that field, so this is dead plumbing (kept for triangle-inequality pruning plans).

## 5. `SpillableTopKBuffer` + `SpillableTopKDrainIterator` (`impls/`)

### 5.1 Buffer anatomy

Lazy-initialized on first insert (needs the tuple's field count): a `MaxHeap` of
`VectorSearchHeapEntry` (`{TuplePointer, dqx}`, max-ordered by dqx) over a
`VariableDeletableTupleMemoryManager` backed by a `VariableFramePool` with budget
**`DEFAULT_FRAMES_LIMIT = 4` frames × frameSize** (≈512 KiB at 128 KiB frames). Tuple bytes live
in managed frames; the heap holds pointers. Spill state: a list of `GeneratedRunFileReader`s +
`maxSpilledDqx` (the worst dqx ever spilled, `-∞` initially).

### 5.2 `insert(tuple, dqx)`

1. **Early reject** — only when `getNumEntries() ≥ candidateLimit` (in-memory + spilled) *and*
   the heap is non-empty: reject iff `dqx > max(inMemoryPeekMax, maxSpilledDqx)` (strict `>`, so
   K-boundary ties are admitted). The floor must include `maxSpilledDqx` because spilled tuples
   are immutable — after a spill the in-memory side holds the *best* half, so an incoming dqx
   worse than every in-memory entry can still beat a spilled one and must be kept.
2. Serialize the tuple through a single-tuple frame (`FrameTupleAppender`; oversized tuple →
   WARN + drop).
3. **replaceMax vs. grow**: if the heap is at `candidateLimit` *and* `dqx ≤ in-memory peekMax` →
   delete the worst in-memory tuple from the buffer manager and `replaceMax`. Otherwise (heap
   has room, *or* dqx is between in-memory-worst and spilled-worst) → plain `insert`, i.e. the
   heap **grows past candidateLimit** in the in-between case rather than evicting a better
   in-memory candidate.
4. Either path: if `bufferManager.insertTuple` fails (budget), `spillWorstHalf()` and retry once.

### 5.3 `spillWorstHalf()` — the spill trigger's payload

Sort the heap's backing array ascending by dqx; keep the best `n/2` in memory, write the worst
`n − n/2` to a fresh managed run file (`VectorTopKSpill…`) as **dqx-prefixed tuples**
(`[8-byte double dqx][original fields]`, hand-built frame-tuple encoding); update
`maxSpilledDqx`; delete the spilled tuples from the buffer manager; rebuild the heap from the
kept half. Each spill produces one sorted run; runs accumulate.

### 5.4 `drain()` and `DrainIterator` — drain order + candidateLimit math

Heap entries are array-sorted ascending by dqx, then:

- **No spills — fast path:** `inMemoryOnly(sorted, accessor)` with
  `outputLimit = sorted.length` — a zero-copy walk of the frame buffer, ascending dqx.
- **Spilled — merge path:** `withMerge(sorted, accessor, runs, spillRecDesc, ctx,
  candidateLimit)` — a k-way merge over one `InMemoryMergeSource` + one `RunFileMergeSource` per
  run, min-PQ keyed on each source's current dqx, **`outputLimit = candidateLimit`** — this is
  where the K×kMultiplier cap is actually enforced when the collection over-grew across spills.
  `RunFileMergeSource` reads frames back, exposes dqx from field 0, and adapts fields 1..N as a
  `RunFileTupleReference` that hides the prefix.

`hasNext()` **prefetches** in merge mode (advance the just-consumed source, re-offer it, poll the
next) and sets `prefetched`; `next()` consumes the prefetch (re-running the staging if a caller
skipped `hasNext`). `getCurrentDqx()` mirrors `getTuple()` validity — it's what
`LSMVTreeTopKSearchCursor.getCurrentDistance()` returns and ultimately what the index-only plan
emits as `$$dist`. On the fast path it reads `sortedInMemory[inMemoryIndex − 1].dqx`.

The cursor's `hasNext/next/getTuple/getCurrentDistance` are thin delegates onto this iterator;
`close()` closes component cursors, the drain iterator, and the buffer (which deletes spill
files via delete-on-close readers).

## 6. Memory vs. disk components in this layer

- The component list order (memory mutable first) is set by the harness; the top-K cursor treats
  all components uniformly — the differences live one layer down: a memory component's
  `VTreeSearchCursor` navigates the shared `.staticstructure` and resolves directory pages via
  `centroidDirPageMap`, a disk component navigates its own embedded static copy and lazily
  builds a local centroid→directory map ([navigation.md §2.4](navigation.md)).
- The **strategy's level-wise list is computed once, on component 0's navigation tree**. Cluster
  identity across components is by `centroidId` (all components share the same trained
  centroids), which is why `openClusterByResult` re-resolves page ids per component instead of
  trusting the result's `directoryPageId`.
- There is no mid-search memory-component switch handling: the harness enters/exits components
  around the whole operation, and all work happens inside `open()`.

## 7. Onward

Every `openClusterByResult`, DFS step, and level-wise list in this file is implemented by
`VTreeSearchCursor` + `VTreeNavigationUtils` inside a single component — continue in
**[navigation.md](navigation.md)**.

## Known gaps / discrepancies (as of `56881406e7`)

- `IClusterSelectionStrategy.setQuantizer` is dead on the query path (never called), so
  `ClusterSearchResult.quantizedDistance` is always NaN there; no reader exists.
- `nprobe` is derived from the epsilon-window size, not the total leaf count — the
  `minProbeFraction` name and the `totalLeafClusters` local both suggest otherwise.
- `candidateLimit` is enforced strictly only on the spilled drain path; the no-spill fast path
  outputs whatever the heap holds (bounded by candidateLimit growth logic, but the in-between
  grow case of §5.2 can leave slightly more than candidateLimit entries in memory).
- The cursor hardcodes the quantized tuple layout (field 3 embedding); opening it against a
  non-quantized component would misread — acceptable under the quantized-only release decision
  but unguarded.
