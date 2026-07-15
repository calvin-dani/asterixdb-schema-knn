# VTree — Project Documentation

VTree: an LSM-based hierarchical IVF vector index for AsterixDB (ANN / top-k search over
embeddings, SQL++ `CREATE INDEX ... TYPE VTREE`). This directory is the single home for all
VTree documentation. It supersedes the flat legacy `doc/` pile — legacy files get imported
into `90-archive/` and only promoted into a numbered section after re-verification.

## How this folder is organized

Sections are numbered by lifecycle: orient → use → build → store → query → decide → verify →
experiment. Every document lives in exactly one section; cross-cutting content is split, not
duplicated (link instead).

| Section | What belongs here |
|---|---|
| `00-orientation/` | Entry points: handoff for a fresh session, glossary, patch-stack map |
| `10-user-guide/` | The SQL++ surface: DDL, WITH parameters, querying, operational notes |
| `20-creation-pipeline/` | The three index-build jobs and the runtime operators (patch 3760) |
| `30-storage-engine/` | On-disk layout, frames/tuples, bulk load, LSM lifecycle, DML (patches 3754) |
| `40-query-path/` | Optimizer rule, search cursors, tree navigation (patch 3771 + storage cursors) |
| `50-design-decisions/` | ADRs — one decision per file, numbered, never rewritten (superseded instead) |
| `60-quality/` | Testing guide, bug archive, performance/benchmark results |
| `70-experiments/` | Research spikes: flat-vs-tree, graph leaf-neighbors, cross-pollination, dual navigation |
| `80-patches/` | One walkthrough per Gerrit patch: what layer it adds, module breakdown, data-stream trace |
| `90-archive/` | Imported legacy docs, unmodified; header marks import date and trust level |
| `_assets/` | Diagrams and plots referenced by docs (name-prefixed by owning doc) |

## Conventions

1. **One topic per file**, kebab-case names. A file answers one question; if it answers two,
   split it.
2. **Every doc starts with this header block:**

   ```markdown
   > **Status:** current | draft | stale | superseded-by: [link]
   > **Verified against:** <commit short-hash> (<date>)
   > **Scope:** one sentence — what question this doc answers.
   ```

   "Verified against" is the anti-rot mechanism: after a rebase or rename sweep
   (e.g. VCTree→VTree), a doc whose hash predates the sweep is suspect until re-checked.
3. **Code references:** repo-relative path + class/method name (line numbers optional — they
   rot fastest). Example: `SecondaryVectorOperationsHelper#buildLoadingJobSpec`
   (`asterixdb/asterix-metadata/.../SecondaryVectorOperationsHelper.java`).
4. **ADRs** (`50-design-decisions/`): `adr-NNN-short-slug.md`, with Status
   (proposed/adopted/superseded), Context, Decision, Consequences. Never edit an adopted ADR's
   decision — write a new one that supersedes it.
5. **This README is the only index.** Adding, renaming, or retiring a file means updating the
   inventory below in the same change.
6. **Archive rule:** nothing in `90-archive/` may be linked as authority from a numbered
   section. To use archived content, verify it against current code, rewrite it into the right
   section, and mark the archived original `superseded-by`.

## Inventory & fill-in plan

Checked = written and verified. This doubles as the roadmap; we fill it one item at a time.

### 00-orientation/
- [ ] `handoff.md` — orient a fresh session/developer: what VTree is, where things live, current state
- [ ] `glossary.md` — terms (static structure, directory page, cross-pollination, …) + rename history (VCTree→VTree, metadata→directory)
- [ ] `patch-stack.md` — Gerrit stack map: 3754 p1–3 (storage), 3760 (training/creation), 3771 (optimizer); what each contains, branch state

### 10-user-guide/
- [x] `ddl.md` — full creation surface: grammar constraints (one VECTOR field, WITH + EXCLUDE UNKNOWN KEY mandatory, INCLUDE position/VTREE-only), complete WITH-parameter table incl. the `manhattan_distance` ghost value, train_list_fraction clamp/full-scan rule, num_clusters caveats (unvalidated; ignored top-down+SelectHead), cross_pollination_m headroom contract, ANALYZE prerequisite + sample-seed, all creation-time SET knobs (trainseed, selecthead.\*, topdown.\* incl. two dead deprecated keys) + the SET-allowlist registration quirk, worked examples
- [x] `querying.md` — ORDER BY ann_distance + LIMIT pattern, 5-argument table (metric normalize-match or silent full-scan fallback, min_probe_fraction, k_multiplier), SET kmultiplier precedence over arg 5 / prunedsearch no-op, plan shapes (index-only vs lookup-and-rerank, M>1 DISTINCT, INCLUDE filter pushdown), honest caveats (OPEN index-only+WHERE empty-result bug + workaround, dot sign convention, recall knobs)
- [x] `operations.md` — transparent DML maintenance (M-replica routing, antimatter deletes, upsert = delete+insert), flush triggers + restart durability, COMPACT DATASET = full merge + physical delete reclamation, limitations list (quantized-only, metric set, no online retrain) + user-visible open bugs

### 20-creation-pipeline/
- [x] `overview.md` — end-to-end dataflow: three-job orchestration, per-edge tuple formats, the Hyracks storage side of each job (builder/loader page mechanics, component layout), cross-job handoffs, determinism levers
- [ ] `job1-quantization.md` — component extractor → local/global quantization aggregate → params on resource JSON
- [ ] `job2-training.md` — sampling, k-means‖ + Lloyd's, bottom-up hierarchy emission, static structure creator
- [ ] `job3-bulkload.md` — routing (level-wise + RNG filter), sort {1,0}, VTreeBulkLoader cluster streaming
- [ ] `operators-reference.md` — per-operator I/O tuple formats (the five 3760 operators)

### 30-storage-engine/
- [x] `index-instance-anatomy.md` — one LSMVTree instance on an NC: file set (`N_N_vct`, shared `.staticstructure`, `.metadata` JSON), component model incl. the isStaticStructure component lifecycle, per-component page-space maps, resource JSON key table, accessor/op-context wiring
- [x] `page-formats.md` — byte-level reference for every page kind: header offset tables, slot array, tuple encodings (quantized/non-quantized/neighbor-list/antimatter), sort invariants, chaining semantics, index metadata page + `VTreeMetadataKeys`
- [ ] `static-structure.md` — bottom-up builder, page-id/centroid-id conventions, pointer fix-up on copy
- [x] `lsm-lifecycle.md` — FLUSH and MERGE end to end: all four flush triggers (VBC-full → opTracker → FLUSH log → scheduleFlush, FlushDatasetUtil, shutdown/restart, recovery), VTreeFlushLoader identity copy + static append + fixed root arithmetic, memory-component recycle, `COMPACT DATASET` → scheduleFullMerge, merge-policy wiring (size-bounded-concurrent default vs NoMergePolicy tests), full-scan merge cursor → VTreeBulkLoader drain, file naming, worked matter/antimatter trace, invariants checklist incl. two NEW suspect findings (antimatter bit stripped by partial-merge rewrite; merge compare key reads field-2 `quantized_distance`)
- [x] `dml.md` — insert/delete/upsert end to end: optimizer maintenance branch (`[vector, includes…, pk…]`), LSM routing + op-context frame-polarity flip, in-tree replica routing (`findReplicaClusters` lock-step with bulk load), sorted insert + antimatter delete, adjacency cancellation at query/flush/merge, worked M=2 example
- [x] `distance-and-quantization.md` — end-to-end distance-function + quantizer reference doubling as a review guide: hyracks IVTree* interface families, asterix implementations (all five parallel metric maps + drift audit, OSQ encode/decode math), every injection path (DDL→resource JSON→createInstance, query-time IAP, job-time ctor params, defaults table), full use-site inventory (FP vs reconstructed-quantized operands), invariants checklist, 5-question patch review guide. Absorbs the scope planned for `resource-and-injection.md`.

### 40-query-path/
- [x] `optimizer.md` — SQL++ → physical plan: IntroduceTopKAccessMethodRule internals (findOrderOperator, filter-field extraction, metric matching, isProjectionPkOnly trace), VectorIndexAccessMethod's two plan shapes (index-only ORDER-BY rewrite + neutralizeDanglingExpressions, lookup-and-rerank, M>1 DISTINCT), VectorJobGenParams slot table, PushFilterIntoVectorSearchRule + filter schema offset math, VectorSearchPOperator/getVectorSearchRuntime, rule phases; full before/after listings for both shapes
- [x] `search-cursors.md` — VectorSearchOperatorNodePushable (predicate wiring, IAP keys, output layout incl. index-only dist emission), cursor selection, LSMVTreeTopKSearchCursor open() step-by-step (⟨distance,PK⟩ PQ merge, antimatter three-way logic, dqx computation, filter application point), SpillableTopKBuffer/DrainIterator internals (heap, spill-worst-half, candidateLimit math), NprobeClusterSelectionStrategy + cluster advancement, when the merge-only LSMVTreeSearchCursor is still reachable
- [x] `navigation.md` — single-component bottom layer: VTreeSearchCursor open/data-chain/full-scan modes, centroid→directory resolution (VBC map vs BFS-built local map, −1 sentinel WARN), VTreeNavigationUtils (greedy + iterative DFS, level-wise global sort, multiplicative epsilon `d + |d|·ε`, overflow chains, quantized-vs-full-precision centroid usage), ClusterSearchResult fields, epsilon/nprobe/kmult/rng parameter table, shared-static-structure branch; ends with a worked three-layer query trace
- [ ] `index-only-include-filter-fix-b.md` — deferred design for serving index-only ANN with an INCLUDE-field WHERE (skip the primary lookup): why the logical-phase embed fails to type, and the physical-phase-rule approach to build it. Correctness already handled by fix A (see 60-quality/bug-archive)

### 50-design-decisions/
- [ ] `adr-001-bottom-up-static-structure.md`
- [ ] `adr-002-quantized-only-release.md`
- [ ] `adr-003-distance-function-injection.md`
- [ ] `adr-004-cross-pollination-replication.md`
- [ ] `adr-005-adjacency-antimatter-reconciliation.md`
- [ ] (add as decisions surface)

### 60-quality/
- [ ] `testing.md` — unit tests, integration runner (`integration/tests/run_tests.py`), what each part covers
- [x] `review-3754-p1.md` — industry-strict code review of storage patch 1 (pin/latch exception-safety, test-mock branches in production, loader/DTO duplication, silent-catch recall ergonomics, api layering); verified top findings, refactor comments only
- [x] `bug-archive.md` — significant bugs with root cause + status; currently: flush leaf-as-root (fixed), quantized_distance field-2 semantics (OPEN), k-means leaf-level drop (fixed), training-sample run-file leak (fixed), plus a latent-risks/smells section; legacy bugs to migrate
- [ ] `performance.md` — profiling method + current numbers (consolidates time_profiling/, meeting plots)

### 70-experiments/
- [ ] `flat-vs-tree.md` — status + findings (consolidates flat-vs-tree/ scripts' conclusions)
- [ ] `graph-leaf-neighbors.md` — pointer to doc-graph-leaf-neighbors/ or absorbed content
- [ ] `cross-pollination-tuning.md` — sweep results (cross-pollination/ folder)
- [ ] `dual-navigation.md` — two-root experiment design

### 80-patches/
- [x] `3754a-storage-layer-p1.md` — VTree core module: four frames, single-tree insert/delete/search, static builder, bulk/flush loaders, IVTree* dependency inversion
- [x] `3754b-storage-layer-p2.md` — LSM wrapper module: components + shared .staticstructure, flush/merge, two cursors, antimatter, resource JSON persistence
- [x] `3754c-storage-layer-p3.md` — unit-test suite: harness, canonical geometric structures, full test matrix, semantics the tests pin down
- [x] `3760-training-vtree-index.md` — creation pipeline: quantization training, k-means, grouped bulk load
- [x] `3771-ann-optimizer-rule.md` — read path (IntroduceTopKAccessMethodRule, VectorIndexAccessMethod, VectorSearchPOperator, filter pushdown), ann_distance fallback, DML maintenance, DDL surface

### 90-archive/
- [ ] import pass over legacy `doc/` (38 files) — copy in, stamp headers, triage current/stale
