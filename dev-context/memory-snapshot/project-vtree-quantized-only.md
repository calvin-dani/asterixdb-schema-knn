---
name: project-vtree-quantized-only
description: VTree feature ships quantized-only; non-quantized path is deprecated and exists only for unit tests
metadata: 
  node_type: memory
  type: project
  originSessionId: e8da6b5b-eb59-469f-9d56-05ade6986d72
---

The ready-to-release VTree feature is **SQ8-quantized only**. The non-quantized variant has been deprecated.

Two concrete consequences any cleanup pass must respect:

1. **Tuple format** — the non-quantized layout `[distance, centroidId, PKs..., includes...]` no longer exists in production code paths. The only place it still appears is unit-test scaffolding. Anywhere the production code has a non-quantized branch, that branch is dead and the comment/conditional can collapse to the quantized form `[distance, qDist, qEmbed, centroidId, pk, include_fields...]`. This is why the PS14 cleanup of `VectorIndexFilterSchema` removed non-quantized comment fragments — same rule applies elsewhere.

2. **Cursor roles** — there are two LSM cursors, and they serve different lifecycles:
   - `LSMVTreeTopKSearchCursor` — **the production ANN search cursor**. Quantized top-K window via `SpillableTopKBuffer`. Routed to by `LSMVTreeIndexAccessor.createSearchCursor()` *only when the IAP flag `LSMVTreeTopKSearchCursor.IAP_KEY` is set to `Boolean.TRUE`*. `VectorSearchOperatorNodePushable` always opts in.
   - `LSMVTreeSearchCursor` — **streaming search + merges + test verification**. Used by `LSMVTree.scheduleMerge()` / `merge()` (line 588 with `returnDeletedTuples=true`) AND it's the default returned by `createSearchCursor()` when the TopK flag is absent. Test fixtures that verify inserted/deleted/merged records (e.g. `verifyInsertedRecords`, `verifyRecordsWithSearch`) iterate this cursor via the usual `accessor.createSearchCursor(false)` path and rely on its lifecycle.

   **Don't collapse the dispatch to always-TopK** — that breaks the test verification paths because TopK's `open()` expects a full predicate-driven setup that the test verification code doesn't supply, leading to `NullPointerException: this.rangeCursors`. Tried this on `cleanup/reflection-removal`; 13/17 LSM-vtree tests broke. The fix was to invert the default: streaming cursor is the default, TopK is opt-in via `LSMVTreeTopKSearchCursor.IAP_KEY = "USE_TOPK_SEARCH"`. See `LSMVTreeIndexAccessor.createSearchCursor()` for the dispatch.

Touched files when this came up: `LSMVTreeIndexAccessor.java`, `LSMVTreeTopKSearchCursor.java` (carries the IAP_KEY), `LSMVTree.java` (lines 96, 493, 588, 594-595, 603-605), `VectorSearchOperatorNodePushable.java`, `VectorTreeTestUtils.java`.
