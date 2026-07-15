# Bug archive

> **Status:** current
> **Verified against:** `56881406e7` (2026-07-05)
> **Scope:** significant VTree bugs — symptom, root cause, discovery method, status. One
> section per bug, newest first. Open bugs stay here with status OPEN until fixed, then get
> their fix commit recorded.

## M=1 insert/delete routed greedy while bulk-load routed level-wise — disk-component delete leak — FIXED (2026-07-15, amended into 3754 p1 `31d0729825`)

- **Symptom:** index-only ANN `ann_verify` (integration Part 3/4) returns **deleted** PKs — but only for records deleted from a **disk (bulk-loaded)** component; memory-component deletes reconcile fine. The primary BTree is correct (PK scan reports all deletes applied); only the vector-index ANN path leaks. Strongly clustering-dependent: a fixed-seed sweep (`compiler.vector.trainseed`, M=1) leaked on **7/10 seeds, up to 659 deleted PKs**. Seed 42 was coincidentally clean — which is why prior unseeded "green" runs masked it.
- **Read side is sound:** `LSMVTreeTopKSearchCursor` reconciles **per cluster, across components**, grouping a run of equal **field-0 (distance-to-centroid)** (`compareField0`) then cancelling matter↔antimatter by PK (`reconcileGroupByPrimaryKey`, newest component wins). A matter is cancelled only if its antimatter shares **the same centroid AND a bit-identical field-0**.
- **Root cause (write side):** `VTree.findReplicaClusters` shortcut `M == 1` to **greedy** `findClosestClusterFromRoot`, while **bulk-load** places matters via **level-wise global-sort** (`findCloseCentroidsLevelWiseGlobalSort`). For boundary records the two pick a different leaf centroid, so a DELETE wrote its antimatter into a different cluster (and a different field-0) than the bulk-loaded matter → the per-cluster reconciliation never saw them together → leak. Inserted records don't leak: their matter *and* antimatter both route greedy, so they agree with each other.
- **Proof (direct probe, seed 21):** of 1000 bulk-loaded deletes, **659 had greedy ≠ level-wise-top-1 → exactly 659 leaked PKs** (1:1); greedy consistently chose a *farther* centroid (gd > ld).
- **Fix:** delete the `M == 1` greedy shortcut (~3 lines) so **all** M route through the same `findCloseCentroidsLevelWiseGlobalSortFromRoot` + `RngAcceptanceFilter` path bulk-load uses (`RngAcceptanceFilter` with m=1 returns the single globally-closest centroid). Zero new executable lines — the level-wise path already existed for M>1. Epsilon parity confirmed: `CrossPollinationConfig.epsilon()` is built from the same `levelwiseEpsilon` (WITH-clause `epsilon`) bulk-load uses (`VTreeResourceFactoryProvider`). One routing function for bulk-load / insert / delete.
- **Verified:** seed sweep 7/10-leak → **0/10**; storage JUnit **39/39**; runtimets vector **4/4**; M=3 and determinism (2× seed 42) green; p1 compiles standalone.
- **Recurrence note:** *distinct* from the 2026-06-28 [[bug-vtree-antimatter-distance-keyed-reconciliation]] (directory-append sort corruption — verified still fixed). **Same symptom** (disk-delete leak, distance-keyed reconciliation), **different cause** (routing asymmetry, not read-order). Unseeded k-means hid it behind lucky seeds; the greedy shortcut hid behind a javadoc that reasoned about insert-vs-delete parity but never bulk-load-vs-delete. **TODO:** add a seeded delete-leak regression guard (e.g. seed 21, delete bulk-loaded records, assert 0 ANN leak) — the existing 39 storage tests all passed *before* the fix too, so they don't guard this.

## Static-structure copy was O(K·d) heap + unbounded page confiscation — FIXED (2026-07-08, design-debt, review-driven)

- **Not a correctness bug — design debt** flagged by Ian Maxon on Gerrit change 21099 ps2:
  both static-structure copy paths were unbounded in memory. `VTreeBulkLoader` snapshotted
  **every** static page into a `List<byte[]>` at construction (O(numStaticPages · pageSize)
  heap held for the whole load) and then, in `end()`, confiscated **all** destination pages
  simultaneously into a `TreeMap` so `resolveLeafNeighborPointers` could sweep them after
  patching. `VTreeFlushLoader#copyStaticStructure` had the same
  snapshot-then-confiscate-everything shape. For a production-sized static structure this
  scales with the number of leaf centroids times vector dimensionality.
- **Fix (behavior-preserving refactor, amended into 3754 p1):** the source static component
  is immutable and open for the whole load, so no snapshot is needed. Destination page ids
  are deterministic (sequential from `staticBasePageId`, source page-id order), which makes
  a two-pass scheme possible in `VTreeBulkLoader#end()`: **pass 1** walks the source leaf
  pages one pin at a time building an O(K) `cid → (finalPageId, slot)` map; **pass 2**
  copies one source/destination page pair at a time — pin source, copy into a single
  confiscated destination page, release source, patch pointers (interior child offsets,
  leaf metaPtr/nextLeaf, neighbor entries resolved from the pass-1 map), write, move on.
  `VTreeFlushLoader#copyStaticStructure` uses the same bounded per-page loop (it needs no
  map — it only offsets already-resolved neighbor entries) and keeps the fixed root
  arithmetic (`staticBasePageId + staticTree.getRootPageId()`). Memory is now O(1) pages
  held at any moment plus the O(K) id map.
- **Output invariance:** identical bytes on disk — same page contents, same patches (the
  pass-1 map is complete before any provisional neighbor entry is resolved, exactly like
  the old whole-structure sweep), same ascending FIFO write order.
- **Verified:** lsm-vtree module 35/35 (`LSMVTreeLeafNeighborTest` and
  `LSMVTreeFlushSearchTest` are the guards for neighbor resolution and flush root),
  runtimets vector group 6/6, full build green.

## Partial merge re-encodes preserved antimatter as matter — delete resurrection — FIXED (2026-07-07, amended into 3754 p2 `250230d228`; test in p3 `10703e9c19`)

- **Symptom (predicted by the lifecycle doc pass, then red-test-confirmed):** a merge whose
  component set EXCLUDES the oldest disk component runs with `returnDeletedTuples=true`
  (antimatter must be preserved — an older matter twin may exist outside the merge). The
  merge cursor duly hands antimatter tuples to `VTreeBulkLoader`, but the loader's data
  frame came from the disk tree's single `dataFrameFactory`, wired to the **insert
  (matter)** `LSMVTreeDataTupleWriter` — `writeTuple` sets the antimatter bit only from its
  own flag, so the preserved antimatter was re-written as **matter**. After the merge, the
  older component's matter twin has no cancelling antimatter → the **delete resurrects**
  (worse: the merged component now holds a *positive* duplicate of the deleted record).
- **Reachable:** not under `NoMergePolicy` JUnit tests or COMPACT (full merge → drop mode);
  reachable under the production `size-bounded-concurrent` policy, which routinely merges
  runs that exclude the oldest component.
- **Fix:** LSMBTree's copy-tuple-writer pattern. New `LSMVTreeCopyTupleWriter(Factory)`
  (lsm-vtree `tuples` package) preserves the SOURCE tuple's antimatter bit
  (`tuple instanceof ILSMTreeTupleReference && isAntimatter()` → set bit after the base
  write). `LSMVTreeDiskComponent#createVTreeBulkLoader` installs it via
  `createCopyDataFrameFactory()` and the new `dataFrameFactoryOverride` parameter of
  `VTree#createComponentBulkLoader`/`VTreeBulkLoader` **iff
  `operation.getIOOperationType() == MERGE`** (safe for drop-mode merges too — no
  antimatter reaches the loader there); initial bulk load (LOAD) keeps the matter writer.
- **Red-green:** new `LSMVTreeMergeAntimatterTest#partialMergePreservesAntimatter` — bulk
  load matter for `pk_c_10_0`, flush filler inserts, DML-delete + flush (antimatter on
  disk), `scheduleMerge` over all disk components except the oldest, search. Pre-fix:
  "deleted PK 'pk_c_10_0' resurrected by partial merge". Post-fix: green; full lsm-vtree
  module 35/35.

## Merge cancellation key includes quantized fields — COMPACT leaks deletes on quantized indexes — FIXED (2026-07-07, amended into 3754 p2 `250230d228`; test in p3 `10703e9c19`)

- **Symptom (predicted by the lifecycle doc pass, then red-test-confirmed):**
  `LSMVTree#doMerge` built its merge predicate as bare `new VTreeSearchPredicate()`, whose
  default `pkStartField = 2` matches only the non-quantized layout. On quantized indexes
  (PKs at field 4) the merge cursor's cancellation/ordering key therefore covered fields
  2/3 (`quantized_distance`, `quantized_embedding`) before the PKs. Field 2 has different
  write semantics per producer (bulk load: quantized-space distance; DML insert/delete:
  full-precision duplicate of field 0), so a bulk-loaded matter tuple and its DML
  antimatter twin compared **unequal on field 2** → cancellation missed during a full merge
  (COMPACT, `returnDeletedTuples=false`): the orphan antimatter was silently dropped and
  the **deleted PK survived the compact** and reappeared in queries.
- **Fix (two parts):** (1) `doMerge` now sets
  `mergePred.setPkStartField(VTreeDataTupleConstants.getPkStartField(isQuantized()))`;
  `LSMVTree#isQuantized()` = `quantizationParams != null ||
  dataTupleCreatorFactory.isQuantized()` (new default method on
  `IVTreeDataTupleCreatorFactory` — the factory flag covers test fixtures that select the
  quantized layout with null params). (2) Key-audit: `LSMVTreeSearchCursor#compare` and its
  priority-queue comparator now cap the key at the end of the PKs —
  `Math.min(cmp.length − pkStartField, numPrimaryKeyFields)` (from
  `LSMVTree#getNumPrimaryKeyFields`) — so the reconciliation key is **exactly
  ⟨distance, PK…⟩**; trailing INCLUDE fields (which may legitimately differ between
  matter/antimatter twins) are excluded too.
- **Red-green:** new `LSMVTreeCompactQuantizedTest` — quantized bulk load with field 2
  written in quantized space (`distance × 12.75`), DML-delete `pk_opt_5`/`pk_opt_6`, flush,
  `scheduleFullMerge`, top-K search. Pre-fix: post-compact results =
  `[pk_opt_5, pk_opt_6, …]` (deleted PKs returned first); the pre-compact query on the
  same data passes both ways (query-path predicates carry the correct pkStartField).
  Post-fix: green; full lsm-vtree module 35/35.
- **Interaction with the antimatter-copy fix above:** fixing the key routes more antimatter
  into *preservation* on partial merges — both fixes must land together (they do, in
  3754 p2).
- **Post-COMPACT recall anomaly (`fraction=0.4`, see open entry below/memory):** NOT
  directly reproduced by this bug — this failure mode makes deleted PKs *survive*, not
  disappear. It may contribute indirectly (resurrected deletes consume the K/candidate
  budget and push out true neighbors); the anomaly should be re-tested on the fixed stack.

## Filter pushdown hard-codes a single primary key — composite-PK misfilter — FIXED (2026-07-07, amended into 3771)

- **Claim verified against code** (surfaced by the query-path doc pass,
  [optimizer.md §3](../40-query-path/optimizer.md)): `PushFilterIntoVectorSearchRule` computes
  each INCLUDE field's physical index as `numSecondaryKeys + 1 + positionInIncludeList` —
  the `+ 1` is literally commented `// +1 for single PK`
  (`asterixdb/asterix-algebra/.../optimizer/rules/PushFilterIntoVectorSearchRule.java:182`).
- **Authoritative layout** puts INCLUDE fields *after all* primary keys:
  `outputRecFields[numOutputSecondaryFields + numPrimaryKeys + i]` in
  `SecondaryVectorOperationsHelper` (asterix-metadata, ~line 452), with `numPrimaryKeys =
  dataset.getPrimaryKeys().size()`. So for a dataset with N > 1 PK columns the
  `VECTOR_FILTER_VAR_MAPPING` annotation points each filter variable at physical field
  `numSecondaryKeys + 1 + pos` — which is PK column 1 (then PK 2, …) instead of the INCLUDE
  field, an off-by-(N−1) for every mapped variable.
- **The wrong index is consumed for real:** the mapping resolves through
  `VectorIndexFilterSchema.findVariable` (direct, un-offset lookup) into the compiled
  `ITupleFilterFactory`, and `LSMVTreeTopKSearchCursor#passesTupleFilter` evaluates that
  filter against the **full storage tuple** (`[dist, cid, (qd, qe), pk…, includes…]`) before
  the top-K insert. With 2 PKs the predicate reads PK[1]'s bytes as the INCLUDE field's type —
  garbage comparison → wrong rows admitted/rejected inside the K-budget (silent wrong
  results, or a deserializer error, depending on type-tag luck). The delegate-schema mode
  (`opSchema position + numSecondaryKeys`) used for PK variables is correct for any PK count;
  only the annotation path is broken.
- **Reachable:** nothing blocks composite-PK VTREE creation (the VTREE branch of
  `QueryTranslator` validates only the vector field and INCLUDEs;
  `SecondaryVectorOperationsHelper` and `MetadataProvider#getVectorSearchRuntime` handle
  `numPrimaryKeys` generically), and the pushdown fires in both index-only and
  lookup-and-rerank shapes. Untested: every in-tree vector test dataset has a single PK
  (`idx`), so nothing exercises the bug.
- **Fix (2026-07-07, amended into the 3771 commit):** the rule now computes
  `numSecondaryKeys + dataset.getPrimaryKeys().size() + positionInIncludeList`
  (`VectorSearchInfo.numPrimaryKeys` populated in `buildSearchInfo` from the `Dataset`
  already in hand). Regression coverage: runtimets
  `vector/create-index-vtree-composite-pk` — composite PK `(id1, id2)` + `INCLUDE(year)`,
  a `.plans` regex asserting the `condition (...)` landed on the `idx_cpk_emb`
  unnest-map, and an executed `WHERE year > 2000` top-3 whose golden is hand-derived
  (pre-fix the pushed filter read PK `id2` and returned zero rows — verified red-green).

## Top-K budget consumed by cross-pollination replicas — DEFERRED BY DESIGN (2026-07-07)

- **Symptom:** ANN queries deterministically missing PKs that exist (integration Part 3:
  ~1.4–2.4k of 8000 pre-delete, from memory AND disk components); point lookups fine.
- **Root cause:** `SpillableTopKBuffer` counts its `candidateLimit = K × kMultiplier` budget
  in RAW tuples (insert early-reject + `SpillableTopKDrainIterator` `outputLimit`). With
  `cross_pollination_m = 3`, replicas of the same record (identical dqx) consume the budget;
  the optimizer's DISTINCT dedups only *after* it is spent. Measured: 12,295 raw entries for
  8,000 records → ~6,560 unique PKs returned. Latent bug **exposed by the (correct)
  cross-pollination DML replication fix** — not a regression; no branch with current config
  ever passed (`storage-wrap-up-integrate` is byte-identical to `integrate-newbase`).
- **Status decision (2026-07-07): deliberately NOT fixed in the canonical stack.** Replica
  inflation is considered part of what the `k_multiplier` knob compensates for (its rerank
  headroom doubles as replica headroom), so this is not a pure bug in our context. The
  operational contract: **deployments/tests using `cross_pollination_m = M` should size
  `k_multiplier` with replica headroom (worst case ×M)** — the integration config needs this
  for Part 3 to pass. A working dedup-at-insert fix exists (`b1f9834374` on the scratch
  branch `fix-ann-completeness`: key on bytes from `pkStartField`; safe since replicas share
  dqx; validated suite 0-FAIL) and can be adopted later as a deliberate design change in
  3754 p2 — it would decouple the two knobs cleanly. Note the same scratch commit also
  carries the flush-root forward-port for integrate-newbase (that part IS wanted there).
- **Exonerated during investigation:** `updateMetadataWithNewDataPage` sorted insert present
  on both branches; chain instrumentation logged zero across-page distance inversions.
- **After fix:** full integration suite 12 PASS / 4 WARN (pre-existing unseeded recall) /
  **0 FAIL**. Diagnostics commit `6f6e468766` (VTNPROBE/VTTOPK/VTCHAIN/VTFLUSH) droppable.

## Flush persists a LEAF page as the component root — FIXED (2026-07-04)

- `VTreeFlushLoader#copyStaticStructure` returned `staticBasePageId` (first copied static page
  = **leaf** page 0 under bottom-up layout); `LSMVTree#doFlush` persisted it as root via
  `flushLoader.end(...)` — missing the `+ staticStructureRootPage` offset that
  `VTreeBulkLoader#end` (line 551) applies correctly.
- **Consequence:** top-k queries against a flushed-but-unmerged component saw only leaf
  cluster 0 (navigation from a leaf stops; other clusters fall back to the `-1` metaPtr
  sentinel → silently empty). If the flushed component is component 0, the *global* candidate
  list is truncated for all components. Wrong root is persisted → survives restart.
- **Why undetected:** merge full-scan follows `nextLeaf` unconditionally, so data survives
  merges; `LSMVTreeMergeTest` was the only flushing test and only searches *after* merging.
  Does NOT explain the post-COMPACT recall bug (predicts pre-compact misses instead).
  The part4 integration test (`test_part4_flush_compact.py`) *did* query the vulnerable
  window (ANN after restart-flush, before compact) but with exclusion-only assertions
  (deleted PKs absent), so a truncated result set passed.
- **Fix (2026-07-04):** `copyStaticStructure` now returns
  `staticBasePageId + staticTree.getRootPageId()` (mirrors `VTreeBulkLoader#end`); `doFlush`
  needed no change (it already plumbs the return value into `flushLoader.end`). The
  `-1`-sentinel fallback is now loud: `VTreeSearchCursor#getMetadataPageIdFromCluster` and
  `#openClusterByDirectoryPage` log a WARN (query mode only — merge full-scan legitimately
  sees `-1` for empty clusters) instead of silently yielding an empty cluster.
- **Regression tests:** new unit test `LSMVTreeFlushSearchTest`
  (flush-then-search-without-merge; asserts inserted PKs from leaf clusters on static leaf
  pages other than page 0 are returned) — verified to FAIL before the fix (c13 cluster empty)
  and PASS after. Part4's `_verify_ann_query` carries a positive completeness assertion
  (all surviving `verify_expected_ranges` PKs must appear in the ANN results) on both the
  restart and compact paths, closing the exclusion-only gap.

## quantized_distance (field 2) has three semantics and zero readers — OPEN, LOW (confirmed 2026-07-04; readers claim corrected 2026-07-07)

- Bulk load writes quantized-space distance; DML (`VTreeDataTupleCreator#writeQuantizedFields`)
  duplicates the full-precision field 0; navigation fallback writes 0.0.
- **Readers claim, corrected:** the original "no code reads field 2" was wrong — until
  2026-07-07 the MERGE reconciliation comparator read field 2 as part of its cancellation
  key on quantized indexes (default `pkStartField = 2`), and the divergent write semantics
  made COMPACT leak deletes (see the FIXED merge-cancellation-key entry above). After that
  fix the merge key skips field 2 again, so field 2 is back to having **no readers**
  (`Q_QUANTIZED_DISTANCE_FIELD` has no consumers; top-k dqx recomputes from field 3).
- Still a correctness trap for any future pruning that trusts the stored value, plus
  8 wasted bytes/tuple (declared VarLen despite fixed 8B).
- **Fix sketch:** either populate true quantized distance on the DML path (smaller diff,
  preserves the pruning use) or drop field 2 entirely (`pkStartField` 4→3 everywhere).

## K trained leaf centroids dropped by hierarchical k-means — FIXED (2026-07-21, found 2026-07-03)

- **Symptom:** a vector index built with `num_clusters = K` (or the `sqrt(cardinality/
  partitions)` default) ends up with ~√K leaf clusters whenever K ≥ 4. The finest, most
  expensive level of the trained hierarchy never reaches the static structure.
- **Root cause:** producer/consumer key mismatch in
  `HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor#performMemoryEfficientHierarchicalKMeans`:
  leaves stored at `levelCentroids[0]` (~line 1582) → first hierarchy iteration
  `initializeParentLevel(0)` overwrites key 0 → `buildLevelFromAssignments(parent=0,
  child=-1)` re-stores leaves at key **-1** → `outputHierarchicalStructure` iterates
  `0..maxLevel`, never key -1. K ≤ 3 escapes (hierarchy building skipped).
- **Impact:** `num_clusters` is honored by k-means but not by the emitted tree; searches probe
  much coarser clusters. Past benchmarks that used the default K likely measured a ~√K-leaf
  tree (e.g. glove-50k / 4 partitions: ~111 requested → ~10 effective per partition). Verify
  with `num_leaf_centroids` in old `staticstructure_*` dumps before trusting old numbers.
- **Found by:** the layer-2 unit test
  `HierarchicalKMeansPlusPlusCentroidsOperatorDescriptorTest` (structural-invariant
  assertions); empirically confirmed: K=36 over 80 vectors emits 8 tuples (6 mid + 2 roots).
  End-to-end tests didn't catch it because the runtimets goldens are deliberately
  clustering-independent.
- **Regression provenance:** not present in 3760 from the start. Gerrit 21159 PS1–PS19 emitted the
  leaves via a root-to-leaf BFS queue that descended to key -1; PS20 (2026-06-30) refactored emission
  to a fixed `0..maxLevel` loop (to add BFS-from-root ids + bottom-up order) that never visits key -1,
  orphaning the leaves the build side still files there.
- **Status:** FIXED (2026-07-21). Build-side fix in
  `performMemoryEfficientHierarchicalKMeans`: start the parent loop at `currentLevel = 1` so the K leaf
  centroids stay at key 0 (inside the emitted `0..maxLevel` range) and parent levels occupy `1..maxLevel`.
  Keeps PS20's id/order machinery; no storage-format change. Guarded by a new unit test
  `testLeafLevelEmittedWithFullClusterCount` (asserts the deepest level carries all K centroids; fails on the
  pre-fix code). Unit + golden green. The finer tree it produces exposed a latent **query** bug — the ANN
  search orphaned one cluster per partition (next entry), losing ~0.4% of records; that is now FIXED too.
  **Ship the two together** — the leaf-drop fix alone would regress recall.

## Finer VTree tree drops ~0.4% of records from ANN results (orphaned greedy-seed cluster) — FIXED (2026-07-21)

- **Symptom:** with the leaf-drop fix active (correct fine tree, e.g. ~35 leaves for 8k records), an ANN
  query with `LIMIT 10000` (full recall) returns ~0.4% fewer records than exist. Integration Part 3/4
  `ann_pre_delete` / `ann_verify` / `ann_restart` FAIL (e.g. 7971/8000, 6474/6500). The direct PK-scan finds
  all records (no data loss); deleted-PK leak check passes. Reproducible and deterministic.
- **A/B confirmed the leaf-drop fix is the trigger** (same clean build): `currentLevel=0` (coarse ~5 leaves)
  → PASS; `currentLevel=1` (fine ~35 leaves) → FAIL with the identical missing set. So it is exposed, not
  caused, by the fix — the coarse buggy tree masked it.
- **Root cause (per-tuple BUILD/INSERT/SCAN trace, isolated to a single dataset):** *not* a within-cluster
  data-page gap — an earlier guess, **disproven**: the missing records are never even *enumerated* (zero
  `openClusterByResult` for their cluster), so the storage/data-page path is innocent. The real cause is an
  **orphaned first cluster in the query enumeration.** Every missing record sits in exactly ONE leaf cluster
  per partition that the search never opens (all 29 losses in one run were partition-1 `cid=6`; the earlier
  "disk 23 / mem 6" split was that single cluster holding both bulk-loaded and inserted rows). Mechanism:
  each per-component `VTreeSearchCursor.open` greedily descends to its *locally* closest leaf centroid and, in
  `initializeClusterIterator`, both **consumes it from the DFS iterator** and **marks it visited**. But the
  strategy makes the *globally* closest `level-wise[0]` the first cluster (greedy tree descent is only
  approximate — a parent centroid can be nearer while the true nearest leaf sits under a sibling). When the
  two differ, `LSMVTreeTopKSearchCursor.open` re-opens all cursors to `level-wise[0]` yet leaves the greedy
  seed consumed-and-visited, so the DFS overflow permanently skips it. With `cross_pollination_m=1` (no
  replica) every record routed there silently vanishes. Data-dependent per partition: fires only when
  greedy-seed ≠ level-wise[0], and loses data only when that cluster is populated — which is why one
  partition lost records and the others none. The finer tree (more, smaller clusters ⇒ more subtree
  boundaries) makes the greedy/level-wise disagreement — and thus the orphan — far more likely; cross-
  pollination (M>1) previously masked it via replicas in other scanned clusters.
- **Fix (query-side only; index format untouched):** on the re-open, hand the displaced greedy seed to the
  strategy (`IClusterSelectionStrategy.deferSeedCluster`); `NprobeClusterSelectionStrategy` probes it exactly
  once, between the level-wise phase and the DFS overflow, opening it on every component. Guarded so a seed
  already inside the ε-window (already scanned by the level-wise phase) is NOT re-probed — otherwise the
  cluster is double-scanned: duplicate candidates, double antimatter reconciliation, and duplicate result
  rows under M=1 (where the optimizer adds no DISTINCT). Files: `IClusterSelectionStrategy`,
  `NprobeClusterSelectionStrategy`, `LSMVTreeTopKSearchCursor` (~64 lines added; no index/build change).
- **Verified:** with this fix + the leaf-drop fix, integration Part 3 returns 8000/8000 (was 7971),
  post-delete 6500/6500 with no leaks; the orphaned cluster is enumerated and scanned in every partition.
  Correct for both M=1 (recovers the records) and M>1 (extra tuples are replicas the optimizer's DISTINCT
  already dedups).
- **Status:** FIXED (2026-07-21). Ships with the leaf-drop fix.
- **Related minor finds (same session):** `resetRunFileReader` opens a new reader per pass
  without closing the previous (handle leak until job end); the quantization aggregate's
  mixed local+global state silently prefers the local branch (not reachable from production
  plans).

## Index-only ANN + WHERE on an INCLUDE field returns zero rows — OPEN (found 2026-07-21)

- **Symptom:** a PK-only-projection ANN query with a `WHERE` on an INCLUDE field returns an empty
  result silently (no error). Example:
  `SELECT VALUE m.idx FROM Movie m WHERE m.year > 2000 ORDER BY ann_distance(m.embedding, q, "l2") LIMIT k`.
  Projecting a non-PK field (`SELECT m.idx, m.title …`) returns correct rows.
- **Root cause: two mechanisms that don't compose.**
  1. *Index-only* (`VectorIndexAccessMethod`, chosen by `IntroduceTopKAccessMethodRule.isProjectionPkOnly`):
     when the projection above the LIMIT is PK-only, the record variable is declared dead, the primary
     lookup is skipped, and `neutralizeDanglingExpressions` rewrites every remaining expression that
     references the dead record var to `MISSING`. It rewrites PK field-accesses to the index PK vars
     (`rewriteRecordFieldAccessToPk`) but has **no equivalent for INCLUDE fields**.
  2. *INCLUDE inline filtering* (`PushFilterIntoVectorSearchRule`, a later `physicalRewritesTopLevel`
     rule) serves a `WHERE` on an INCLUDE field by pushing the predicate into the vector search, and it
     assumes the `field-access($$rec, includeField)` is still present when it runs.
  Index-only runs first and rewrites that field-access to `MISSING`, so the WHERE's
  `$$y := field-access($$rec, "year")` becomes `$$y := MISSING` and `SELECT gt($$y, 2000)` collapses to
  `select(MISSING)`, filtering every row. When the pushdown finally runs the predicate no longer mentions
  `year`, so there is nothing to push. Lookup-and-rerank is immune because the record var stays live (the
  primary lookup materializes it), so neutralize never fires.
- **Missing guard:** `isProjectionPkOnly` inspects only the live-out variables *above* the LIMIT (the
  projection). The `WHERE` is a `SELECT` *below* the LIMIT, invisible to the guard, so index-only is
  entered even though the query needs a non-PK record field.
- **Workaround:** project any non-indexed field to force the lookup-and-rerank shape.
- **Fix A (correctness, LANDED 2026-07-21):** `isProjectionPkOnly` also requires every below-LIMIT
  `SELECT`-condition variable to be PK-safe (`collectSelectConditionVars` + `isVarPkSafe`); otherwise the
  plan falls back to lookup-and-rerank. This kills the wrong-results bug. Importantly, in the
  lookup-and-rerank plan the existing `PushFilterIntoVectorSearchRule` (a physical rewrite) still pushes the
  INCLUDE predicate INTO the vector cursor (verified by EXPLAIN: the `VECTOR_SEARCH` unnest-map carries
  `condition (gt($$32, 2005))`), so the filter is still evaluated before the top-K cut — correct recall.
  The only thing the fallback gives up vs. index-only is skipping the primary BTree lookup for the PK
  projection. Guarded by runtimets `vector/create-index-vtree-include-filter` (returns 6,7,8; empty before A).
- **Fix B (index-only + in-cursor filter) — INVESTIGATED, BLOCKED, deferred.** The intended B embeds the
  INCLUDE filter into the index-only vector UNNEST-MAP directly (reusing the pushdown's embed logic, extracted
  to a shared helper). It does NOT work from the logical access-method phase: the embedded INCLUDE filter
  variable is an unnest output referenced only by the operator's `selectCondition` annotation, and logical
  type-environment recomputation cannot infer its type → hard `Could not infer type for variable '$$N'`
  compilation error (empirically confirmed). `PushFilterIntoVectorSearchRule` runs in
  `physicalRewritesTopLevel` precisely to avoid this. So real B (skip the primary lookup for a filtered PK
  projection) needs a DEDICATED PHYSICAL-PHASE rule that, after the index-only plan is set, embeds the filter
  — not a reuse of the logical embed. Deferred: modest perf-only benefit (the filter is already in-cursor via
  A's fallback), real architectural cost. B's exploratory changes were reverted; only A is retained.
- **Related risk:** `ClusterSearchResult.quantizedDistance` is always `NaN` on the query path (see the
  latent-risks section). A NaN distance from the non-top-K streaming cursor is a hard IOException, so any
  future B must keep index-only off the streaming cursor.
- **Status:** FIXED for correctness (A, 2026-07-21; pending amend into 3771 / Gerrit 21287). B (index-only
  perf for filtered ANN) deferred pending a physical-phase design.

## Materialized training sample leaked on every index build — FIXED (2026-07-03)

- **Symptom:** every `CREATE INDEX ... TYPE VTREE` left run files behind (46 from one
  30-record test); caught by the runtimets leaked-run-file detector on the first execution of
  `vector/create-index-vtree`.
- **Root cause:** the k-means operator reads its materialized sample via
  `MaterializerTaskState.createReader()`, bypassing `writeOut()`'s last-consumer accounting
  that normally deletes the backing file.
- **Fix:** new `MaterializerTaskState.deleteFile()` (hyracks-dataflow-std) called in the
  FindCandidates activity's finally block after the last reader closes. Verified: leak
  detector silent on re-run.

## Latent risks and smells (documented, not scheduled)

Byte-level and dead-code findings surfaced by the storage-engine doc pass
([page-formats.md](../30-storage-engine/page-formats.md),
[index-instance-anatomy.md](../30-storage-engine/index-instance-anatomy.md)), the 3760
review, and the query-path doc pass ([optimizer.md](../40-query-path/optimizer.md),
[search-cursors.md](../40-query-path/search-cursors.md)) — each re-verified against code on
2026-07-05. None is scheduled work; they are listed so the next person touching the area
doesn't rediscover them.

- **Schema/tuple arity divergence under filter pushdown — verified benign (2026-07-05).**
  `PushFilterIntoVectorSearchRule` appends the filter variables to the vector unnest-map's
  output variable list (and in index-only mode `$dist` already precedes them), so the codegen
  `RecordDescriptor` declares `numPK (+1) + numFilterVars` fields, while
  `VectorSearchOperatorNodePushable` only ever adds `numPK (+1)` fields per tuple —
  `RemoveUnusedAssignAndAggregateRule` has no UNNEST_MAP case in `removeFromAssigns`, so the
  extra variables survive to jobgen. **Why it doesn't corrupt frames:** the generic
  `IndexSearchOperatorNodePushable` sizes its `ArrayTupleBuilder` from the *output*
  `recordDesc` (`new ArrayTupleBuilder(recordDesc.getFieldCount())`, line 245) and appends
  `tb.getFieldEndOffsets()` — the full fixed-size slot array — so every emitted tuple
  physically carries the declared number of field slots; the PK/dist fields align exactly,
  and the trailing filter-var slots keep their never-written value 0 (phantom fields with
  *negative* computed length). Safe only because nothing reads them: the SELECT was deleted,
  the variables are referenced nowhere above, and sorts/exchanges copy tuples wholesale by
  tuple-level offsets. Exercised end-to-end by integration part2 `movie_esq_filter`
  (`WHERE m.year > 2000` on `Movie` INCLUDE(title, year)) — the only in-tree query that
  triggers the rule; runtimets has none. **Latent trap:** any consumer that walks fields by
  index up to the declared arity (e.g. a `retainInput` copy loop, a field-wise
  re-serializer, or a future rule that references the filter vars above the unnest) would
  read a negative field length.
- **`compiler.vector.prunedsearch` is a no-op knob.** Declared
  (`CompilerProperties.COMPILER_VECTOR_PRUNEDSEARCH_KEY`, whitelisted in
  `SqlppCompilationProvider`) but has no reader anywhere — setting it changes nothing
  (`asterixdb/asterix-common/.../config/CompilerProperties.java:286`).
- **`queryVarList` slot [5] (`search_approach`) is documented but dead.** The
  `VectorJobGenParams` class comment describes a sixth slot (0 = ann_distance,
  4 = vector_distance); `createIndexSearchPlan` only ever appends five variables — never
  written, never read (reserved for the dual-navigation experiment)
  (`asterixdb/asterix-algebra/.../optimizer/rules/am/VectorJobGenParams.java`).
- **`IClusterSelectionStrategy.setQuantizer` is never called on the query path.**
  `LSMVTreeTopKSearchCursor` extracts the quantizer/quantized query vector *after*
  constructing `NprobeClusterSelectionStrategy` and never forwards them, so navigation always
  passes `null, null` and `ClusterSearchResult.quantizedDistance` is always `NaN` in
  production; no reader exists — dead plumbing kept for triangle-inequality pruning plans
  (cross-ref the `quantized_distance` field-2 entry above: same "stored/derived quantized
  distance has no consumer" theme)
  (`hyracks-storage-am-lsm-vtree/.../lsm/vector/impls/LSMVTreeTopKSearchCursor.java`).
- **`nprobe` is computed over the epsilon-filtered candidate list, not the total leaf
  count.** `nprobe = max(1, floor(list.size() × minProbeFraction))` where `list` is the
  epsilon-windowed global candidate list — the local is misleadingly named
  `totalLeafClusters`, and `min_probe_fraction = 1.0` means "probe every cluster in the
  epsilon window", not "probe every leaf". The semantics are arguably intended (a fraction
  of the already-plausible window); the naming is not
  (`hyracks-storage-am-lsm-vtree/.../lsm/vector/impls/NprobeClusterSelectionStrategy.java:97-99`).
- **Wrong quantized field-order javadocs in the filter-pushdown pair.**
  `PushFilterIntoVectorSearchRule` (~line 180) and `VectorIndexFilterSchema` (class comment)
  both list the quantized secondaries as `[distance, qDist, qEmbed, centroidId]`; the
  authoritative `VTreeDataTupleConstants` order is `[distance, centroidId, qDist, qEmbed]`.
  Harmless today — only the *count* (4) enters the offset math — but the comments mislead
  (same transposition as the `LSMVTreeUtils` entry above).
- **Epsilon is now uniformly multiplicative — doc supersession, not a bug.**
  `VTreeNavigationUtils#epsilonThreshold` (line 552) computes
  `d + |d|·ε` for *all* distances, i.e. `(1+ε)·d` for positive and `(1−ε)·d` for negative
  (negated dot product). This supersedes the older fix note in `CLAUDE.local.md` that
  described an additive `d + ε` window for non-negative metrics with the multiplicative form
  only for dot product.

- **Recovery validation of `.staticstructure` is existence-only, but a failed check deletes
  every data component.** `LSMVTreeFileManager#validateStaticStructureFile` only calls
  `ioManager.exists()`; on failure `cleanupAndGetValidFiles` deletes **all** `vct` files via
  `cleanupOrphanedVTreeFile` — a truncated/corrupt-but-present static file passes, a missing
  one silently empties the index
  (`hyracks-storage-am-lsm-vtree/.../lsm/vector/impls/LSMVTreeFileManager.java`).
- **8 dead header bytes in every VTree page.** `VTreeNSMFrame` reserves `cluster_id`
  (initialized to −1, read only by debug `printHeader`) and `centroid_id` (never written at
  all) at offsets 22/26 of every interior/leaf/directory/data page; only their end offset is
  used to place subtype fields (`hyracks-storage-am-vtree/.../vector/frames/VTreeNSMFrame.java`;
  [page-formats §1](../30-storage-engine/page-formats.md)).
- **The page `level` byte doesn't reflect actual depth.** Every interior page gets constant
  level 1 regardless of height (`VTreeStaticStructureBuilder#openPage`); leaf, directory, and
  data pages are all level 0 — page kind is established by pointer provenance, and only the
  `level > 0` / `isLeaf()` distinction is meaningful
  ([page-formats §1.1](../30-storage-engine/page-formats.md)).
- **`LSMVTreeUtils` layout comment contradicts the real quantized field order.** The block
  comment claims `[distance, qDist, qEmbed, centroidId, pk…]`; the authoritative order —
  `VTreeDataTupleConstants`, `VTreeDataTupleCreator`, the resource provider, and the Job-3
  sort keys `{1, 0}` — is `[distance, centroidId, qDist, qEmbed, pk…]`
  (`hyracks-storage-am-lsm-vtree/.../lsm/vector/utils/LSMVTreeUtils.java`, ~line 139).
- **Dead merge-file plumbing.** `LSMVTreeFileManager#getRelMergeFileReference` mints a
  per-merge `<seq>_.staticstructure` reference into the bloom-filter slot of
  `LSMComponentFileReferences`, but `LSMVTree#doMerge` only uses
  `getInsertIndexFileReference()` — the file is never created.
- **`resetRunFileReader` leaks readers — FIXED (2026-07-06).** Six call sites reassigned the
  reader without closing the predecessor; turned out NOT minor — tripped the runtimets
  tearDown fd check with 65 leaked `.waf` handles once tests ran at the reworked 3760'. Fix
  (in 3760' commit `650c912535` on vtree-spann-integrate): single-owner `currentSampleReader`
  field, `resetRunFileReader` closes the superseded reader, idempotent close in the outer
  finally. SPANN operator audited clean for the same pattern. (Still unfixed on the
  superseded vtree-tests-and-fixes branch.)
- **Quantization aggregate silently prefers local state.** In
  `QuantizationConstantsAggregateDescriptor`'s `finish()`, when both `localValues` and
  `finalValues` are populated the local branch returns early and global values are ignored —
  not reachable from production plans today, a trap for future plan shapes
  (`asterixdb/asterix-runtime/.../aggregates/std/QuantizationConstantsAggregateDescriptor.java`).

For pre-2026-07 bugs (dot-product sign inversion, epsilon window for negative distances,
dequantization inverse mapping, unsorted directory-entry insert breaking antimatter
reconciliation, DFS-vs-level-wise conflict), see the fix notes in `CLAUDE.local.md` and the
memory files; they will be migrated here in a later pass.
