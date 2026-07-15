---
name: bug-vtree-antimatter-distance-keyed-reconciliation
description: "VTree secondary leaks deleted PKs because adjacency-based antimatter reconciliation in LSMVTreeTopKSearchCursor fails in the cluster-by-cluster nprobe merge; matter and antimatter have identical keys but aren't placed adjacent. Masked by primary lookup, exposed by index-only ANN"
metadata: 
  node_type: memory
  type: project
  originSessionId: 8431569a-5592-4753-823d-da9762cb5b99
---

Root-caused 2026-06-28 with extensive logging while integrating index-only ANN (gated off as a result).

**Symptom:** with `INDEX_ONLY_ENABLED=true`, PK-only ANN verify (Part 3/4 `ann_verify`) returns ~904 deleted PKs. Legacy (index-only off) returns 0 — the primary BTree lookup masks it; index-only skips that lookup and trusts the secondary's PK set, exposing the leak.

**What was RULED OUT (by measurement, not assumption):**
- NOT a navigation divergence: greedy (`findClosestCentroid`, incremental delete) vs level-wise (`findCloseCentroidsLevelWiseGlobalSort`, bulk-load) agree on centroid AND distance 100% (1318/1318).
- NOT different stored centroid: matter centroid == antimatter centroid for all 1323 deletes (0 mismatch), bulk-loaded and inserted alike.
- NOT different stored distance: both paths store field-0 = navigation distance; matched 0/1323. (Distance is deterministic for identical embedding+centroid — so it MUST be byte-identical; an earlier "distances differ" guess was wrong.)
- NOT a different cluster at scan time: reconciliation trace showed leaked matters emitted in the SAME cluster as their antimatter (2191 same, 0 different).

**Actual root cause:** the antimatter reconciliation in `LSMVTreeTopKSearchCursor.getNextValidTuple` is **adjacency-based** — it holds an antimatter and cancels only if the *immediately next* priority-queue element is the same (field-0 distance, PK). In the cluster-by-cluster nprobe k-way merge across components, a matter and its same-key antimatter are NOT reliably adjacent, so cancellation fails. Measured in the post-delete query: **1303 antimatters held, only 415 CANCEL, 711 DISCARD** → ~888 leak (matches the ~904 observed). The discard log showed a held antimatter at dist 0.7523 whose next queue element was an unrelated matter at 0.7794 — the matching 0.7523 matter was not adjacent. (The exact micro-ordering reason in the merge was narrowed but not fully isolated; the cross-query log mixing of pre/post-delete confounded the component-order analysis. It does not change the fix.)

**ROOT CAUSE (write side, confirmed 2026-06-28 with corrected instrumentation):** the data-page `next_page` chain within a cluster is NOT maintained in distance order. The per-component per-cluster read order (cursor follows entry-0 then `next_page`) jumps backward by up to 0.26 mid-cluster (e.g. page spans 0.79..1.03, its `next_page` restarts at 0.77). 13/28 cluster-read groups non-monotonic; per-file max backward jump: files 7=0.26, 25=0.20, 24=0.17, 6=0.06, others 0.00. Insert/split components are the egregious offenders; some bulk-load components are clean. An out-of-order per-component stream violates the k-way merge's sorted-input precondition, so a record's matter and same-distance antimatter (different components) are never surfaced adjacently → adjacency reconciliation discards the antimatter → PK leaks. Recall is unaffected because `topKBuffer` re-sorts final output by approx distance; reconciliation is the only in-scan-order-dependent consumer.

**MY EARLIER "page chains ARE sorted → not write side" WAS WRONG** — flawed instrumentation: logged only `dataPageId` without qualifying by component file, so same-numbered pages across the memory/disk files (each LSM component is a separate file with its own page numbers) were conflated into a garbage reconstruction. Correct instrumentation = `VTCLOPEN`/`VTPREAD` logs in `VTreeSearchCursor` keyed by `System.identityHashCode(this)` (cursor=component) + `dataFileId`. Validated read order per (cursor,file) between cluster opens.

**Ruled out (still valid):** navigation divergence (greedy==levelwise), stored distance (bit-identical), stored centroid (0 mismatch), duplicate cluster list (none), per-component cluster-open desync (advanceAll lockstep).

**FIXED 2026-06-28 — exact one-line write-side cause + fix (validated code+logs):** `VTree.updateMetadataWithNewDataPage` (VTree.java ~line 729) **appended** the new data-page directory entry at `getTupleCount()` instead of inserting it in sorted-by-max_distance position. After a non-last data-page split the new page's max_distance falls between existing entries, so the append corrupted the directory's max_distance-ascending invariant (which `VTreeMetadataFrame` javadoc mandates and `findDataPageInMetadataPage` relies on for "first entry with maxDistance>=d" routing). Measured: 48/96 appends broke order; 3463 insert/delete routing lookups ran over unsorted directories → tuples mis-routed to wrong data pages → overlapping data-page distance ranges → non-monotonic `next_page` read chain (max backward jump 0.26) → violated the k-way merge's sorted-stream precondition → adjacency antimatter reconciliation leaked deletes.

**FIX:** replace the append with `int pos = ((VTreeMetadataFrame) ctx.getMetadataFrame()).findInsertPosition(maxDistance); ctx.getMetadataFrame().insert(metadataTuple, pos);` — uses the frame's existing binary-search sorted-insert (same call the metadata split path already uses). Search/merge/reconciliation code untouched (honors user constraint). `splitDataPageMaintainOrder` + `VTreeDataFrame.split` were already correct (left=lower, right=higher, spliced original→new→origNext); bulk-load was near-sorted; the directory append was the sole corruptor.

**VALIDATION (post-fix Part 3):** METAUNSORTED_LOOKUP 3463→0; non-monotonic cluster read groups 13→0 (max jump 0.26→0.0); `ann_verify` with `INDEX_ONLY_ENABLED=true` leaks 0 deleted PKs (was ~904); unit LSMVTreeDelete{,Quantized,Include}Test pass; full integration 11 PASS/3 WARN/0 FAIL (the 3 WARN are pre-existing Part-2 ANN recall ~64-70% vs 70% threshold, unrelated to deletes — routing fix doesn't change cluster-scan recall; baseline-confirm if doubted). Index-only re-enabled (`INDEX_ONLY_ENABLED=true`) since the leak that gated it is gone. Debug instrumentation removed. Related: [[dfs-levelwise-conflict-bug]], [[bug-vtree-delete-frame-corruption]], [[bug-vtree-post-compact-recall]].

**Index-only status:** fully ported, compiles, gated behind `IntroduceTopKAccessMethodRule.INDEX_ONLY_ENABLED=false`. The separate plan-rewrite typing bug (above-LIMIT query-vector LET dangling) is fixed (rewrite now covers `aboveLimitOps`). Re-enable once this reconciliation gap is fixed or under a no-deletes guarantee. See [[project-vtree-quantized-only]].
