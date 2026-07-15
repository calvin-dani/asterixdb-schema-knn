# Navigation — single-component search at the bottom of the stack

> **Status:** current
> **Verified against:** `56881406e7` (2026-07-05)
> **Scope:** how one VTree component turns a query vector into cluster candidates and data-page
> scans: `VTreeSearchCursor`, `VTreeNavigationUtils` (DFS + level-wise), and the shared-static-
> structure wiring for memory components.

Last file of the query-path chain: **[optimizer.md](optimizer.md) →
[search-cursors.md](search-cursors.md) → navigation.md**. Page-byte layouts:
[page-formats.md](../30-storage-engine/page-formats.md); component model:
[index-instance-anatomy.md](../30-storage-engine/index-instance-anatomy.md).

All paths below are under
`hyracks-fullstack/hyracks/hyracks-storage-am-vtree/src/main/java/org/apache/hyracks/storage/am/vector/`.

## 1. Two buffer caches, one cursor — the `staticBufferCache` branch

`VTree.VTreeAccessor#configureCursor` (`impls/VTree.java`) wires every `VTreeSearchCursor` with
*two* page-access channels:

| | navigation (static structure) | data (directory + data pages) |
|---|---|---|
| **memory component** (`tree.staticBufferCache != null`) | `staticBufferCache` / `staticFileId` / `staticRootPage` — the shared `.staticstructure` disk component, cached by `VTree#setStaticStructure` at component wiring time | the component's own VBC (`tree.bufferCache` / `tree.getFileId()`), plus `centroidDirPageMap` + `firstLeafCentroidIdMem` for O(1) directory lookup |
| **disk component** | the component's own file — every disk component embeds its own static-structure copy at the tail (flush/bulk-load appended it) | same file |

The same dichotomy backs `VTree#getNavigationBufferCache()/getNavigationFileId()/
getNavigationRootPageId()` — which is what `NprobeClusterSelectionStrategy` uses for the
level-wise pass — and `VTree#findClosestClusterFromRoot` /
`findCloseCentroidsLevelWiseGlobalSortFromRoot` (the DML routing entry points), both of which
additionally **remap** each result's `directoryPageId` through `centroidDirPageMap` for memory
components. Consequence: *all* navigation for a live memory component runs against the one
trained tree; only the leaf-cluster payload pages differ per component.

`VTree.VTreeAccessor#search` builds the `VTreeCursorInitialState`: query vector decoded from the
predicate's tuple via the IAP-injected `IVTreeBinaryAccessorFactory`; distance function from the
IAP factory (falling back to the index-persisted factory, and to the index's own metric string
when the predicate carries none — the merge path); root = `staticRootPage` for memory components
else `rootPage`; and the per-query quantizer + `quantizedQueryVector = quantizer.quantize(qv)`
when the tree has `float[6]` quantization params (IAP factory path, or a pre-built
`IVTreeQuantizer.IAP_KEY` test fallback). A missing distance function is a hard
`IllegalStateException` — the null-metric merge NPE fix made this explicit.

## 2. `VTreeSearchCursor` (`impls/`)

### 2.1 `open()` — query mode

1. Copy query vector / root / distance function / quantized state from the initial state.
2. Build a `VTreeNavigationState` (buffer cache, fileId, root, frame factories, query vector,
   and — when the LSM layer already called `setSharedVisitedSet` — the shared visited set;
   otherwise a private one, later replaced when the LSM layer injects the shared set).
3. `VTreeNavigationUtils.initializeClusterIterator(state, distanceFunction)` — a full DFS
   descent (§4.2) returning the component-locally closest cluster (or null → empty tree, cursor
   born exhausted).
4. `targetMetadataPageId = getMetadataPageIdFromCluster(result)` (§2.4).
5. `currentDataPageId = getFirstDataPageFromMetadata()` — pin the directory page (data buffer
   cache!), read entry 0's data-page pointer, unpin. Empty directory → −1.
6. `openDataPage(id)` — pin + read-latch the data page, set up the data frame + frame tuple,
   `tupleCount`, `currentTupleIndex = 0`. `clustersProbed = 1`.

The cursor does **not** iterate clusters on its own in query mode — `hasNext()` returns false
when the current cluster's page chain is exhausted, and the LSM layer decides whether to call
`openClusterByResult` with the next strategy-chosen cluster
([search-cursors.md §3.7](search-cursors.md)).

### 2.2 Data-page chain iteration

`hasNext()`: tuples left in the current page → true; else `moveToNextDataPage()` follows the
`next_page` chain, **skipping empty pages** (in-place physical deletes can empty a page whose
successors still hold data) until a non-empty page or `next == −1`. `next()` positions
`frameTuple` by `resetByTupleIndex(dataFrame, currentTupleIndex++)` — zero-copy into the pinned
page. Exactly one data page is pinned/latched at a time (`openDataPage` closes the previous).

### 2.3 `openClusterByResult` — the LSM layer's per-component entry

Always **re-resolves the directory page locally** (`getMetadataPageIdFromCluster`, §3) — the
`ClusterSearchResult` usually comes from component 0's navigation tree, whose `directoryPageId`
(static-structure predicted id, or another component's page id) means nothing in this component.
Then `openClusterByDirectoryPage(localId)`: pin the directory page, take entry 0's data-page
pointer, open it. Bumps `clustersProbed` (the nprobe budget counter; the LSM layer decrements it
again for clusters that contributed zero valid tuples). Returns whether the cluster has data.

**The −1 sentinel, post-fix behavior:** `openClusterByDirectoryPage(−1)` treats the cluster as
empty. In full-scan (merge) mode that is legitimate and silent; in **query mode it logs a WARN**
(`"-1 directory page sentinel in query mode (fileId=…, rootPageId=…)"`) because a −1 there is
the signature of an unreachable cluster — historically a wrong persisted component root (the
flush leaf-as-root bug, see the [bug archive](../60-quality/bug-archive.md)) rather than a truly
empty cluster. A second WARN fires in `getMetadataPageIdFromCluster` when the local map has no
entry *and* the caller-provided result carries −1.

### 2.4 `getMetadataPageIdFromCluster` + `buildLocalCentroidDirPageMap`

Three-tier resolution:

1. **Memory component:** `centroidDirPageMap[centroidId − firstLeafCentroidIdForMap]` — the
   VBC directory page pre-created per leaf centroid at component initialization. O(1).
2. **Disk component:** lazily build `localCentroidDirPageMap` — a **BFS from the component's
   root**: a page queue + visited set; each page is classified via `IVTreeLeafFrame#isLeaf`;
   leaf pages contribute `(getCentroidId(i) → getMetadataPagePointer(i))` for every tuple and
   enqueue their overflow successor when `getOverflowFlagBit()` (via `getNextLeaf()`); interior
   pages enqueue every `getChildPageId(i)` plus their own overflow successor (`getNextPage()`).
   The map is built once per cursor and answers all subsequent clusters. This exists because
   the static structure's leaf tuples carry *predicted* sequential directory ids, while
   `VTreeBulkLoader.end()` writes the component's *actual* ids into its embedded copy.
3. **Fallback:** the result's own `directoryPageId` (with the §2.3 WARN when it is −1).

### 2.5 Full-scan mode (`navigateToFirstCluster`) — the merge path

Set by `VTreeAccessor.createSearchCursor(exclusive, fullScanMode=true)` (only the merge and
tests do this). `open()` then:

1. Descend from the root **always taking child 0** to the leftmost leaf (`isLeafPage` reads the
   frame's level byte; level 0 = leaf).
2. Walk the *entire* leaf level via `getNextLeaf()`, collecting every tuple's
   `getMetadataPagePointer(i)` into `allDirectoryPageIds` (order = centroid-id order).
3. Open cluster 0. `advanceToNextCluster()` (called by the streaming LSM cursor) then walks the
   list sequentially — 0 → 1 → 2 …, distance-free, `ClusterSearchResult` stuffed with
   placeholders (no centroid, distance 0.0, cluster index as centroid id). Empty clusters
   surface as −1 directory ids, silently.

## 3. `VTreeNavigationUtils` (`utils/`) — the traversal algorithms

Stateless static helpers; every method pins/unpins internally, callers hold no latches.
`MAX_TREE_DEPTH = 10` guards the greedy descent against cyclic child pointers.

### 3.1 The collect primitives — overflow chains at both levels

`collectAllChildCentroids` (interior) and `collectAllLeafCentroids` (leaf) share the loop shape:
the *first* page is already pinned by the caller; overflow pages
(`getOverflowFlagBit()` → `getNextPage()`/`getNextLeaf()`) are pinned, latched, and released
internally, one at a time. Every tuple's full-precision embedding is decoded from
`VTreeStaticTupleConstants.EMBEDDING_FIELD` (`DoubleArraySerializerDeserializer`); tuples whose
dimension doesn't match the query, or that throw during decode, are **skipped with a TRACE log**
(malformed tuples never abort a traversal). Results are sorted ascending by
`distanceFunction.apply(queryVector, centroid)`.

**Quantized vs. full-precision centroids:** navigation *ordering* always uses the
full-precision embedding. Only `collectAllLeafCentroids` — and only when **both**
`quantizer` and `quantizedQueryVector` are non-null — additionally computes
`quantizedDistance = distanceFunction.apply(quantizedQuery, dequantize(getQuantizedCentroidBytes(i)))`
per leaf centroid, stored as side metadata in the result (NaN otherwise). Callers that pass the
quantized pair today: `VTree#findClosestClusterFromRoot` (when its own caller provides them —
the DML path passes nulls) and `findCloseCentroidsLevelWiseGlobalSort`'s quantized overload —
which on the query path receives nulls because the top-K cursor never calls
`strategy.setQuantizer` ([search-cursors.md §4](search-cursors.md)). Net: **quantized centroid
distances are currently computed nowhere in production**; the plumbing awaits
triangle-inequality pruning.

### 3.2 `findClosestCentroid` — greedy descent (DFS without backtracking)

Root → leaf: at each interior page, collect **all** children (incl. overflow) sorted by
distance, descend into the closest; at the leaf page, collect all centroids and return the
closest as a `ClusterSearchResult`. Used by the write path (`findReplicaClusters` M = 1) and
test accessors — not by the query cursors, which need iteration.

### 3.3 Iterative DFS — `initializeClusterIterator` / `findNextClosestCluster`

State lives in `VTreeNavigationState`: a stack of `VTreeNavigationFrame`s (page id + its
distance-sorted centroid/child list + a consumption index) plus the visited-centroid set
(shared across LSM components when the top-K cursor injects it).

- **`initializeClusterIterator`**: greedy descent as in §3.2, but *every* visited page's sorted
  list is pushed onto the stack; the closest leaf centroid is returned and marked visited.
- **`findNextClosestCluster`**: classic backtracking —
  - top of stack is a leaf frame → yield its next *unvisited* centroid (marking visited);
    exhausted → pop;
  - top is an interior frame → `descendToLeaf(nextChild)` (pushing frames along the way, again
    picking closest-first and skipping visited leaf centroids); no children left → pop;
  - empty stack → null.

  Order note: this yields clusters in *locally* non-decreasing distance per subtree, not a
  global distance order — the global order for the first nprobe clusters comes from the
  level-wise pass; DFS is the overflow valve beyond the epsilon window.

### 3.4 `findCloseCentroidsLevelWiseGlobalSort` — the level-wise pass

Three phases over a breadth-first, level-synchronized traversal (`VTreeLevelNode{pageId,
level}` queue):

1. **Descent with per-level epsilon windows.** At each *interior* node: collect + sort all
   children, `localThreshold = epsilonThreshold(closest, ε)`, enqueue children while
   `distance ≤ threshold` (sorted ⇒ break at the first miss). At each *leaf* page (deduped via
   `visitedLeafPages`): collect **all** centroids of the page + its overflow chain — no
   filtering at the leaf during collection.
2. **Global sort** of every collected leaf centroid by full-precision distance.
3. **Global filter** (only when ε > 0): keep the prefix with
   `distance ≤ epsilonThreshold(globalClosest, ε)`.

An empty result throws `ILLEGAL_STATE` ("No closest clusters found"). The returned list is what
the nprobe strategy walks, head first.

**The epsilon formula** (`epsilonThreshold`, used at both interior levels and the global filter):

```java
threshold = closestDistance + Math.abs(closestDistance) * epsilon;
```

i.e. **multiplicative in both signs**: `(1+ε)·d` for positive distances (euclidean, cosine),
`(1−ε)·d` for negative ones (negated dot product, where smaller = more similar). History: the
original *additive* `d + ε` was a near no-op both for negative dot-product distances (a 0.1%
window at d ≈ −150) and for large-magnitude positive distances (euclidean_squared in high
dimensions), collapsing the search to ~1 cluster regardless of settings — the low-recall bug.
The current code has replaced the earlier hybrid (additive-for-positive) with the unified
relative form; the method javadoc documents exactly this.

## 4. `ClusterSearchResult` (`impls/`) — the inter-layer currency

| field | meaning | caveats |
|---|---|---|
| `leafPageId` | static-structure page holding the centroid | −1 in full-scan mode |
| `clusterIndex` | tuple index within that leaf page | reused as a plain counter in full-scan |
| `centroid` | full-precision centroid (cloned) | null in full-scan |
| `distance` | full-precision D(query, centroid) — the level-wise/DFS sort key | 0.0 in full-scan |
| `centroidId` | the cluster's identity **across components** | the only field safe to use cross-component |
| `directoryPageId` | direct pointer to the cluster's directory page | only valid in the component whose tree produced it (or after `centroidDirPageMap` remap); −1 = unknown/empty |
| `quantizedDistance` | D(q̃, C̃) side metadata (`hasQuantizedDistance()`) | NaN everywhere in production today (§3.1) |

## 5. Parameter semantics

| parameter | origin | default | consumed where | semantics |
|---|---|---|---|---|
| `epsilon` | index WITH `epsilon` → `MetadataProvider` → predicate | 0.3 | `epsilonThreshold` at every interior level + the global leaf filter | relative window `d ± abs(d)·ε` around the closest candidate; defines the level-wise candidate set (and therefore the nprobe base) |
| `min_probe_fraction` | `ann_distance` arg 3 | 0.1 (≤0 → default; >1 → clamp 1.0) | `NprobeClusterSelectionStrategy.initialize` | `nprobe = max(1, ⌊epsilonWindowSize × fraction⌋)` — fraction **of the epsilon window**, not of all leaves |
| `nprobe` | derived | ≥1 | `shouldStopAdvancing` | minimum clusters every component must have scanned (empty clusters excluded) before early stop |
| `k_multiplier` | `ann_distance` arg 4, session `compiler.vector.kmultiplier` overrides when >1 | 1 | `candidateLimit = K × kMultiplier` | widens both the candidate buffer and the stop condition (`resultsCollected ≥ candidateLimit`) |
| `k` | `LIMIT k` | — | stop condition + drain cap | user-visible top-k |
| `cross_pollination_m` / `rng_factor` | index WITH | 1 / 1.0 | **write side only** (`findReplicaClusters` routing, bulk load) | how many replica clusters a record lands in; the query side only compensates via the optimizer's DISTINCT ([optimizer.md §2.4](optimizer.md)) |

## 6. Worked end-to-end example

Toy index from [3754c](../80-patches/3754c-storage-layer-p3.md): euclidean, SQ8-quantized,
`epsilon = 0.3`, `cross_pollination_m = 1`; static structure = root (cid 0) over three leaf
clusters cA (cid 1), cB (cid 2), cC (cid 3); components = one memory component M (recent insert
pk 9 into cA; antimatter for pk 1 in cA) and one disk component D1 (pks 1–6 bulk-loaded).

```sql
SELECT VALUE m.idx FROM Movie m
ORDER BY ann_distance(m.embedding, [0.1,0.2,0.9,0.8], "euclidean", 0.5, 2)
LIMIT 2;
```

**Layer 1 — optimizer.** LIMIT→ORDER matched; metric `euclidean` == index similarity;
projection `m.idx` traces to the PK → **index-only**. Plan: ASSIGN
`[$$qv, $$k=2, "euclidean", $$mpf=0.5, $$kmult=2]` → `UNNEST_MAP ix_emb → [$$pk, $$dist]`
(indexOnly=true) → ORDER `$$dist` → LIMIT 2. No DISTINCT (M = 1).

**Layer 2 — pushable + top-K cursor.** For the single input tuple,
`resetSearchPredicate` fills the predicate (k=2, mpf=0.5, kmult=2, ε=0.3, pkStart=4);
`LSMVTreeIndexAccessor` sees `USE_TOPK_SEARCH` → `LSMVTreeTopKSearchCursor.open()`:

- Two `VTreeSearchCursor`s open. M's cursor navigates the **shared static structure**
  (staticBufferCache branch) and lands on cA via DFS, resolving cA's VBC directory page through
  `centroidDirPageMap`; D1's cursor navigates its **embedded static copy**, DFS also lands on
  cA, and lazily BFS-builds its `localCentroidDirPageMap`.
- Strategy: level-wise on M's navigation tree collects all three leaf centroids
  (root's epsilon window admits both subtrees), sorts globally → distances
  {cA: 0.02, cB: 1.10, cC: 1.55}; global filter keeps `d ≤ 0.02 + 0.02·0.3 = 0.026` → **window
  = [cA]** ⇒ `nprobe = max(1, ⌊1 × 0.5⌋) = 1`; `candidateLimit = 2 × 2 = 4`.
- DFS[0] == level-wise[0] == cA → no re-open. PQ primed: M yields antimatter
  `⟨0.022, "1"⟩•` and matter `⟨0.030, "9"⟩`; D1 yields `⟨0.022, "1"⟩, ⟨0.076, "2"⟩, …`.
- `getNextValidTuple`: head = M's antimatter ⟨0.022,"1"⟩• (component tie-break puts it before
  D1's matter twin) → held; next head = D1's ⟨0.022,"1"⟩ → **key match, both cancelled**. Then
  D1's ⟨0.076,"2"⟩ and M's ⟨0.030,"9"⟩ emerge as matter; each gets
  `dqx = D(q̃, dequantize(field3))` and enters the `SpillableTopKBuffer` (no spill: 2 tuples ≪
  4 frames).
- Stop check: `minClustersProbed = 1 ≥ nprobe = 1` but `entries = 2 < candidateLimit = 4` →
  keep going; level-wise exhausted → **DFS fallback** on M's cursor yields cB
  (visited-set skips cA); both components `openClusterByResult(cB)` — D1 re-resolves cB's
  directory page from its local map — contributing pks 3, 4 with larger dqx. Now
  `2 ≥ 1 && 4 ≥ 4` → stop. Drain iterator sorts by dqx ascending.

**Layer 3 — output.** The pushable projects each candidate's PK bytes (field 4) and appends
`[0x0C][dqx]`; the frame carries `("9", 0.031), ("2", 0.075), ("3", …), ("4", …)` upward. ORDER
BY `$$dist` sorts (already sorted per partition), LIMIT 2 keeps pk 9 and pk 2. pk 1 — deleted —
never left the storage layer.

## Known gaps / discrepancies (as of `56881406e7`)

- `ClusterSearchResult.quantizedDistance` is dead in production: the only query-path producer
  (`findCloseCentroidsLevelWiseGlobalSort` via the strategy) always receives null quantizer args
  (see [search-cursors.md](search-cursors.md)), and there are no readers.
- Full-scan `ClusterSearchResult`s reuse `clusterIndex` as `centroidId`; correct for the
  merge's sequential iteration but meaningless as an identity — do not key on it.
- `isLeafPage` (full-scan descent) and `IVTreeLeafFrame#isLeaf` (everywhere else) are two
  different leaf tests for the same property (level byte vs. frame flag).
- The DFS phase yields locally sorted, not globally sorted, cluster order — beyond the epsilon
  window, "next cluster" is best-effort nearest, which bounds recall for very large
  `min_probe_fraction` values.
