---
name: ""
metadata: 
  node_type: memory
  originSessionId: e8da6b5b-eb59-469f-9d56-05ade6986d72
---

When proposing to drop "dead" fields/methods on a class that implements a framework interface (e.g., `ICursorInitialState`), a grep showing "0 callers in our module" is **not sufficient evidence the API is dead.** The framework itself (LSM harness, abstract base classes, dispatch machinery) may read the interface methods via interface dispatch from code your grep doesn't cover. Unit tests don't always exercise those paths either — the LSM-vtree unit tests (17/17) passed with my changes, but the integration tests (parts 3 + 4) failed with three ANN-search correctness regressions: records that had been inserted couldn't be found via ANN.

**Case in point** — `cleanup/reflection-removal` commit `13b2009e16` ("[cleanup] LSMVTreeCursorInitialState — drop dead fields/methods") removed:
- frame factory fields + getters
- predicate field + `getSearchPredicate()`
- searchCallback field (kept `getSearchOperationCallback()` but made it return null)
- `isDiskComponentScan` field + setter/getter
- `getCursors()` (returned null)

All confirmed 0 callers via grep. **Integration tests failed** — ann_pre_delete_Movie: 118/8000 PKs missing (disk:34 mem:84). Confirmed via bisect: removing this commit alone restores 7/7 PASS. The actual broken path was not identified — most likely candidate is `getSearchOperationCallback()` now returning null when framework code expects a non-null callback, but never verified empirically. Lesson: don't speculate which sub-removal is to blame after the fact; just don't remove fields on framework-interface classes without integration-test evidence.

**Rules of thumb:**
- For classes that implement a framework interface (especially storage-am interfaces like `ICursorInitialState`, `ILSMIndexOperationContext`, `IIndexAccessParameters`), do NOT trust grep alone for dead-code claims. Reflective / framework dispatch is common.
- If an integration test suite exists, **run it before committing dead-code removals on these classes**. Unit tests are insufficient.
- If you've already committed and integration tests fail, **revert the whole commit** before trying to bisect within it. Field-removal interactions are hard to reason about post hoc.
- Comments-only / javadoc cleanups on the same class are fine to keep — they don't affect runtime behavior.

Touched files: `LSMVTreeCursorInitialState.java`, `LSMVTreeOpContext.java`, `LSMVTree.java` (the `reset()` caller).
