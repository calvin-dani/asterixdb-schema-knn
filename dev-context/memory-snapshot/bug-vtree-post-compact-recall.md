---
name: bug-vtree-post-compact-recall
description: "VTreeFlushLoader.copyStaticStructure scrambled disk-component leaf-frame metadata-page-pointers when static-structure leaf pages had physical page IDs in a different order than the nextLeaf chain. FIXED 2026-06-03 by deriving the centroidDirPageMap index from the slot's stored centroid_id instead of a running counter."
metadata: 
  node_type: memory
  type: project
  originSessionId: 2655393a-a843-47af-ad80-f946bd38bbce
---

# Post-compact ANN recall regression — FIXED

## The bug

`VTreeFlushLoader.copyStaticStructure` iterated the static-structure pages by physical page id
and used a **running counter** `centroidIndex` to index into the source memory tree's
`centroidDirPageMap`. The counter incremented per leaf-frame slot visited. The implicit
assumption was that *physical page-id order matches the nextLeaf chain order*, so slot K in
visit-order is the K-th centroid in `centroidDirPageMap`.

That assumption was violated whenever the static-structure builder allocated leaf pages with
non-monotonic page ids vs the nextLeaf chain. When violated, the disk leaf-frame's slot K's
metadata-page-pointer was set to `centroidDirPageMap[K-th-slot-in-page-id-order]` — but that's
the dir page for the centroid that occupies the K-th *physical-page-order* slot, not the
centroid that the cursor will later read at slot K when it follows the nextLeaf chain. Result:
pointers got scrambled, records appeared to be in the "wrong" cluster, the merge cursor
emitted non-monotonic `centroid_id_field` values, the bulk loader REVISITed clusters, pages
got orphaned, recall cratered (up to ~36% missing in the worst observed run).

## Symptom (pre-fix)

Integration Part 4 `ann_compact_Movie` failed intermittently with 0–36% of expected PKs missing.
Primary-key scan confirmed all records were physically present; ANN couldn't reach them.

## The fix (applied 2026-06-03)

Derive `centroidDirPageMap` index from each slot's stored `centroid_id` instead of a running
counter:

```java
// Fixed: VTreeFlushLoader.copyStaticStructure leaf-page branch
int firstLeafCid = sourceMemoryTree.getFirstLeafCentroidIdMem();
for (int t = 0; t < lfFrame.getTupleCount(); t++) {
    int cid = lfFrame.getCentroidId(t);
    int idx = cid - firstLeafCid;
    if (idx >= 0 && idx < numLeafCentroid) {
        lfFrame.setMetadataPagePointer(t, centroidDirPageMap[idx]);
    }
}
```

Now slot K's metadata-page-pointer is *always* the dir page for the centroid actually stored
at that slot, regardless of the physical-page-id traversal order.

File: `hyracks-fullstack/hyracks/hyracks-storage-am-vtree/src/main/java/org/apache/hyracks/storage/am/vector/impls/VTreeFlushLoader.java`,
lines 134-180.

## Verification

5 back-to-back integration runs (parts 3+4, cache=512 MB, fraction=0.4):

| Run | ann_compact_Movie | REVISITs | PqPush mismatches |
|---|---|---|---|
| 1 | PASS | 0 | 0 / 8516 |
| 2 | PASS | 0 | 0 / 8532 |
| 3 | PASS | 0 | 0 / 8510 |
| 4 | PASS | 0 | 0 / 8524 |
| 5 | PASS | 0 | 0 / 8512 |

`[bug2] FullScanInit` post-fix shows a perfectly monotonic layout: slot K → cid K+5 →
dirPage K+2 across every partition.

## How we got here (investigation log)

The investigation explicitly falsified several wrong hypotheses before landing on the right one:

1. ❌ **Epsilon filter dropping centroids** — tested by setting epsilon=2.0 to keep all 35
   centroids; failures still occurred.
2. ❌ **Per-component leaf-page slot order divergence** — `FullScanInit` proved every
   component had slot K → cid K+5 in the same order.
3. ❌ **Cursor not lock-stepping** — verified by code reading and PqPush patterns.
4. ❌ **`centroidDirPageMap` misalignment in memory** — `MemCompInit` showed a clean
   `i ↦ i+2` mapping; `InsertRoute` matched it 100% (3000/3000 events).
5. ✅ **Disk leaf-frame's metadata-page-pointers scrambled at flush time** — `FullScanInit`
   on flushed components showed slots 0–8 pointing correctly, then slots 9–21 holding the
   dir pages that should belong to slots 22–34 and vice versa — direct evidence of the
   page-id-vs-chain order divergence in `copyStaticStructure`.

The bug was structural and lived in flush, but only became visible after the also-amplifying
[[bug-vtree-delete-frame-corruption]] (Bug 1) was fixed. Pre-Bug-1-fix the slot-corruption
sometimes overwrote `cid_field` to arbitrary values, masking the structured form of the
flush misrouting under random-looking noise.

## Diagnostic logs added during the investigation

All in working tree, uncommitted. Can be removed now that the fix is verified, or kept as
guard rails:

- `[bug2] FullScanInit` — `VTreeSearchCursor`: per-component slot/cid/dirPage layout.
- `[bug2] MemCompInit` — `VTree`: centroidDirPageMap + firstLeafCentroidIdMem at memory init.
- `[bug2] InsertRoute` — `VTree.insertVector`: per-insert routing decision + expected dir page.
- `[bug2] PqPush[site]` — `LSMVTreeSearchCursor`: per-tuple (component, slot, cid_field, pkHash).
- `[bug2] AdvanceEmpty` — `LSMVTreeSearchCursor`: empty advance per component.
- `[bug2] BulkLoader transition` — `VTreeBulkLoader`: REVISIT detector per cluster transition.
- `[bug2] Search complete` — `LSMVTreeTopKSearchCursor`: per-query processed/topK count.
- `[bug2] doMerge totals` — `LSMVTree`: per-centroid_id tuple count delivered by merge cursor.
- `[bug2] NprobeStrategy init` / `keptCids` — `NprobeClusterSelectionStrategy`: kept-cluster list.
- `[bug2] findCloseCentroidsLevelWiseGlobalSort` — `VTreeNavigationUtils`: epsilon-cut details.

## Status

**FIXED and verified.** Both Bug 1 (delete-path slot corruption) and Bug 2 (flush
metadata-page-pointer scrambling) are now resolved with minimal local fixes.

Related: [[bug-vtree-delete-frame-corruption]] (FIXED — was masking this bug pre-fix),
[[storage-cleanup-followups]].
