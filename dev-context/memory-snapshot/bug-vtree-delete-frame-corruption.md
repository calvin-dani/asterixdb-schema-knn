---
name: bug-vtree-delete-frame-corruption
description: "VTree.tryInsertIntoDataPage skipped compact() on SUFFICIENT_SPACE — tuple bytes wrote past FREE_SPACE_OFFSET into the slot region, corrupting slot offsets. Surfaced as AIOOBE in VarLenIntDecoder during later physical delete. FIXED 2026-06-02."
metadata: 
  node_type: memory
  type: project
  originSessionId: 2655393a-a843-47af-ad80-f946bd38bbce
---

# VTree delete-path frame-corruption bug — FIXED

## Symptom

Integration Part 3 `insert_delete_Movie` 500 error during the second delete batch
(PKs 6000-6499 of records just inserted into the memory component):

```
HYR0002: Error in processing tuple 0 in a frame
Caused by: java.lang.ArrayIndexOutOfBoundsException: Index -1018459970 out of bounds for length 32768
    at VarLenIntEncoderDecoder$VarLenIntDecoder.decode      (VarLenIntEncoderDecoder.java:133)
    at TypeAwareTupleReference.resetByTupleOffset           (TypeAwareTupleReference.java:61)
    at LSMVTreeDataTupleReference.resetByTupleIndex         (LSMVTreeDataTupleReference.java:44)
    at VTreeDataFrame.getDistanceToCentroid                 (VTreeDataFrame.java:74)
    at VTreeDataFrame.findInsertPosition                    (VTreeDataFrame.java:109)
    at VTreeDataFrame.findTupleByDistanceAndPrimaryKey      (VTreeDataFrame.java:138)
    at VTree.tryPhysicalDelete                              (VTree.java:544)
```

## Diagnostic evidence (captured by [bug1] WARN logs)

```
[bug1] getDistanceToCentroid AIOOBE: tupleIndex=38 tupleCount=77 slotOffset=1768979400
       freeSpace=32716 pageCapacity=32768
[bug1] AIOOBE during findTupleByDistanceAndPrimaryKey: fileId=27 metadataPageId=29
       dataPageId=65 distance=0.921 pkHash=1163603923 liveTupleCount=77
```

Decoded:
- Page capacity 32768; slot region for 77 tuples needs `77 * 4 = 308` bytes → starts at 32460.
- Slot for tupleIndex=38 lives at offset 32612, contains garbage 1768979400 → AIOOBE.
- `freeSpace=32716` — contiguous-free-space high-water-mark is **256 bytes inside** the slot
  region. Tuple data was appended into space occupied by slots, trampling live slot bytes
  including tupleIndex=38's.

## Root cause

`VTree.tryInsertIntoDataPage` at line 334-345 merged `SUFFICIENT_SPACE` into the
`SUFFICIENT_CONTIGUOUS_SPACE` case and dropped the `compact()` call.

`SUFFICIENT_SPACE` means *enough reclaimable space, but not contiguous* — the canonical
BTree pattern (`BTree.java:309-315`) calls `compact()` first. Without it,
`TreeIndexNSMFrame.insert` writes at the current `FREE_SPACE_OFFSET`, which had been pushed
past the slot-region boundary by prior inserts whose deletes only updated
`TOTAL_FREE_SPACE_OFFSET`. The new tuple's bytes overwrote live slot data; a subsequent
search/delete tried to decode the corrupted slot as a tuple offset → AIOOBE.

## Fix (applied 2026-06-02)

Split the merged case at `VTree.java:334-345`. New code:

```java
case SUFFICIENT_CONTIGUOUS_SPACE: {
    int insertIndex = ((VTreeDataFrame) ctx.getDataFrame()).findInsertPosition(distance);
    ctx.getDataFrame().insert(dataTuple, insertIndex);
    ctx.getModificationCallback().found(null, originalTuple);
    ctx.getDataFrame().setPageLsn(ctx.getDataFrame().getPageLsn() + 1);
    updateMetadataMaxDistanceIfNeeded(ctx.getMetadataPageId(), dataPageId, distance, ctx);
    return true;
}
case SUFFICIENT_SPACE: {
    // Compact first: resets FREE_SPACE_OFFSET to a safe high-water mark so the
    // subsequent insert writes into legitimate free space, not into the slot region.
    ctx.getDataFrame().compact();
    int insertIndex = ((VTreeDataFrame) ctx.getDataFrame()).findInsertPosition(distance);
    ctx.getDataFrame().insert(dataTuple, insertIndex);
    ctx.getModificationCallback().found(null, originalTuple);
    ctx.getDataFrame().setPageLsn(ctx.getDataFrame().getPageLsn() + 1);
    updateMetadataMaxDistanceIfNeeded(ctx.getMetadataPageId(), dataPageId, distance, ctx);
    return true;
}
```

## Verification

After fix (integration Parts 3+4 at fraction=0.4):
- cache=128MB: 7/7 PASS, 0 AIOOBEs.
- cache=512MB: 7/7 PASS, 0 AIOOBEs.

Pre-fix runs reproduced the AIOOBE deterministically with `dataPageId=65, tupleIndex=38,
freeSpace=32716` — same page-state byte-for-byte across multiple runs.

## File

`hyracks-fullstack/hyracks/hyracks-storage-am-vtree/src/main/java/org/apache/hyracks/storage/am/vector/impls/VTree.java`
(line 334-360 after fix).

The `[bug1]` diag WARN/DEBUG logs are still in working tree (uncommitted) in
`VTreeDataFrame.java` and `VTree.java`. Useful belt-and-suspenders; can be removed when the
fix lands.

Related: [[bug-vtree-post-compact-recall]] (separate flaky issue, was downstream-amplified by
this bug pre-fix), [[storage-cleanup-followups]].
