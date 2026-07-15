---
name: Dual Navigation Structure Experiment
description: Flat vs hierarchical VTree A/B experiment — two root pages in one .staticstructure file, query-time selection via ann_distance 6th arg
type: project
originSessionId: 3843bbe6-b5e9-4845-90fc-88ee32910c02
---
Implemented dual navigation structure (flat IVF + hierarchical VTree) in a single index for A/B experiments.

**Why:** Measure tree topology benefits — CPU (distance comps), IO (page pins), build time, recall.

**How to apply:**
- Both structures share identical leaf centroids and data pages
- Flat leaf pages appended to `.staticstructure` after hierarchical pages, stored as `flat_root_page_id` in metadata
- Query-time selection: `ann_distance(emb, qvec, "euclidean_squared", 0.4, 2, 1)` — 6th arg `1` = flat, `0`/omitted = hierarchical
- Bulk load uses large epsilon to guarantee global closest cluster assignment
- Instrumentation: ThreadLocal counters in VTreeNavigationUtils, WARN-level logging in NprobeClusterSelectionStrategy

**Plan file:** `/Users/hongyu/.claude/plans/sequential-sniffing-crystal.md`

**Key files modified:**
1. `VTreeStaticStructureBuilder.java` — `buildFlatStructure()` in `end()`
2. `VTree.java` — `staticFlatRootPage` field, `getFlatNavigationRootPageId()`
3. `VectorIndexAccessMethod.java` — 6th arg, arity guard → 6
4. `VectorSearchPOperator.java` — shifted searchApproach to index 6
5. `VectorSearchOperatorNodePushable.java` — reads useFlat from field 5
6. `VTreeSearchPredicate.java` — `useFlatNavigation` boolean
7. `NprobeClusterSelectionStrategy.java` — `initializeWithRootOverride()` overload
8. `LSMVTreeTopKSearchCursor.java` — selects flat vs hierarchical root
