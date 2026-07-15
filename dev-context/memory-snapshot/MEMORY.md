# Project Memory

## Key Bug Fixes
- USER-GUIDE DOC PASS FINDINGS (2026-07-07, in doc-vtree/10-user-guide + ddl.md §5, not yet archived): (1) **Job-2 sample-scan path is UNSEEDED** — helper reads `sample_seed` from index WITH but DeclUtil rejects that key → currentTimeMillis fallback; trainseed only gives determinism in the full-scan regime (card<10k). (2) creation-time knobs (trainseed/selecthead.*/topdown.*) not in SqlppCompilationProvider SET allowlist — work for DDL-only requests, fail mixed requests. (3) kmultiplier precedence: SET>1 silently overrides ann_distance arg 5. (4) num_clusters has no DDL validator. (5) epsilon default drift 0.25 vs 0.3 (unreachable).
- BOTH MERGE BUGS FIXED (2026-07-07, red-green, amended into 3754 p2 `250230d228` + tests p3 `10703e9c19`): (A) partial-merge antimatter re-encoded as matter → LSMVTreeCopyTupleWriter installed iff MERGE op (LSMBTree pattern); (B) merge cancellation key covered fields 2/3 on quantized → doMerge sets pkStartField(isQuantized) + compare capped at numPrimaryKeyFields (INCLUDEs excluded too). Stack now: 9c5fd8c704 SPANN / 526d62d13b 3771 / 3cecbbacb5 3760 / 10703e9c19 p3 / 250230d228 p2 / 3c5605c82e p1. 35/35 module, 6/6 runtimets, 7/7 integration P3+4. Post-compact-recall anomaly NOT directly repro'd by B (B makes deletes survive, not PKs vanish) — retest anomaly on fixed stack. Backup: vtree-spann-integrate-pre-mergefix. NEEDS: re-push origin (history rewritten), regenerate admin bundle (old top3 bundle stale).
- (was) TWO NEW SUSPECTS (2026-07-07, lsm-lifecycle doc pass, chip filed, in doc §6.6/6.7): (1) **partial merges strip the antimatter bit** — doMerge drains preserved antimatter into VTreeBulkLoader whose frames use the INSERT tuple writer → deletes resurrect; unreachable under NoMergePolicy/COMPACT-full-merge, reachable under production size-bounded-concurrent policy (LSMBTree uses a copy writer). (2) **field-2 "zero readers" is FALSE** — doMerge's default VTreeSearchPredicate pkStartField=2 puts fields 2/3 in the merge cancellation key; bulk-load matter (quantized-space d) + DML antimatter (FP d) twins can miss cancellation during COMPACT → plausibly the post-compact recall bug. (3) delete-frame-corruption fix appears IN TREE (tryInsertIntoDataPage compacts before insert w/ comment) — the OPEN memory note is likely stale.
- FIXED (2026-07-07, amended into 3771): composite-PK filter pushdown — `numSecondaryKeys + dataset.getPrimaryKeys().size() + pos`; red-green verified (pre-fix: filter read PK id2, zero rows). Regression: runtimets create-index-vtree-composite-pk (plan-check proves pushdown fires). Stack now: e88d40b79a SPANN / 2c55c77c5b 3771(+fix) / 650c912535 3760 / 3754 unchanged; backup vtree-spann-integrate-pre-fix2. Runtimets 6/6, JUnit green. **NEW OPEN BUG found during test design: index-only ANN plan + WHERE on INCLUDE field degrades the predicate to `select (missing)` → silent empty result** (workaround: project a non-indexed field to force lookup-and-rerank); agent filed a task chip. Not yet in bug-archive.
- (was) CONFIRMED OPEN (2026-07-05): composite-PK filter pushdown misfilter — `PushFilterIntoVectorSearchRule:182` hardcodes `numSecondaryKeys + 1` (single PK); 2-PK datasets read PK[1] bytes as INCLUDE field in the storage-level tuple filter. Fix: `+ numPrimaryKeys`. Untested (all in-tree vector datasets single-PK). Details + 7 more query-path latent risks in doc-vtree/60-quality/bug-archive.md.
- FIXED (2026-07-04): flush persisted leaf page as component root (`VTreeFlushLoader#copyStaticStructure` returned base, not base+rootOffset) → flushed-unmerged components invisible beyond cluster 0 in top-k. Fix mirrors VTreeBulkLoader arithmetic; -1 dir sentinel now WARNs in query mode; regression = `LSMVTreeFlushSearchTest` (red-green verified). Still OPEN LOW: quantized_distance field 2 has 3 write semantics, 0 readers. Details: doc-vtree/60-quality/bug-archive.md.
- [K-means drops K leaf centroids (OPEN 2026-07-03)](bug-kmeans-leaf-level-dropped.md) — hierarchy build re-keys true leaves to -1, never emitted; index gets ~sqrt(K) leaves, num_clusters not honored (K<=3 escapes); past benchmarks suspect
- [CLUSTER BY scoping/traversal bugs](bug-clusterby-scoping-traversal.md) — two Module C rewrite-pass gaps (clustering expr unresolved; sc/rvc not in scope) found+fixed via first Module F plan-golden test
- [DFS vs Level-wise Cluster Conflict Bug](dfs-levelwise-conflict-bug.md) — Root cause of missing PKs in ANN queries
- [VTree Delete Frame Corruption (open)](bug-vtree-delete-frame-corruption.md) — AIOOBE in `tryPhysicalDelete` exposed only when `storage.buffercache.size` is large; suspect `VTreeDataFrame.findTupleByDistanceAndPrimaryKey` / in-place insert race
- [Post-Compact ANN Recall at fraction=0.4 (open)](bug-vtree-post-compact-recall.md) — After `COMPACT DATASET`, ANN at fraction=0.4 misses ~20% of PKs that fraction=0.3 found; pre-compact identical query at 0.4 is fine
- [VTree antimatter reconciliation leaks deletes (FIXED 2026-06-28)](bug-vtree-antimatter-distance-keyed-reconciliation.md) — distance+PK keyed (adjacency) reconciliation in LSMVTreeTopKSearchCursor is the ADOPTED, correct design (not a bug). Real cause was write-side: `VTree.updateMetadataWithNewDataPage` appended dir entries unsorted → unsorted data-page chain → broke the k-way merge's sorted-input precondition. Fix = sorted insert via `findInsertPosition`. Index-only ANN re-enabled.

## Gerrit Stack State (2026-07-06)
- 3760 (change 21159) **ps22 = ps21 + our vtree-tests-and-fixes 3760-scoped work absorbed** (trainseed, materializer leak fix, 3 runtimets cases, 3 JUnit classes, AiProvenance entry). ps21 = ps20 ported to double[]-only VD API (== our integrate-newbase commit tree). **Flush-root fix (3754-scoped: VTreeFlushLoader/VTreeSearchCursor/LSMVTreeFlushSearchTest) is NOT in any Gerrit ps yet** — needs 3754 placement. On restack: our 3760-scoped commits dedupe against ps22. tmp-3760-ps20/21/22 branches exist locally.
- **ps22 SCOPING BUG (2026-07-06): the runtimets vector cases + data + VectorQueries/testsuite registration are misplaced in 3760 ps22** — SQLPP.jj at ps22 has no VTREE grammar (that's 3771), so per-change CI of 21159 will fail on parse. Tell Calvin: move runtimets/data/XML pieces up to 3771; JUnit tests + trainseed + leak fix stay in 3760.

## ANN Completeness ROOT CAUSE — DEFERRED BY DESIGN (user decision 2026-07-07)
- **User decision: top-K replica-budget behavior is NOT a pure bug** — replica inflation is part of what k_multiplier compensates for. Dedup fix NOT applied to canonical stack; contract = size k_multiplier with replica headroom (worst ×M) when cross_pollination_m=M. **CONTRACT VALIDATED (2026-07-07): k_multiplier: 3 in integration config.yaml part3/4 ann_verify (commented in-file) → Parts 3+4 = 7/7 PASS on vtree-spann-integrate, product code untouched.** Dedup fix preserved on scratch branch fix-ann-completeness (b1f9834374) for later deliberate adoption in 3754 p2. NOTE: integration/ dir is untracked — config change lives on local disk only.

## (superseded detail below — root-cause investigation record, 2026-07-06)
- **Primary: cross-pollination replicas consume the top-K budget.** SpillableTopKBuffer counted RAW tuples (insert early-reject + drain outputLimit); with M=3, 8000 records → 12295 raw entries vs candidateLimit 10000 → ~1436 unique PKs missing. DISTINCT dedups only AFTER budget spent. Fix `b1f9834374`: dedup at insert keyed on bytes from pkStartField (replicas share dqx so drop is safe). Latent bug EXPOSED by the (correct) cross-pollination DML replication fix — not a code regression.
- **Secondary: flush-root fix was genuinely lost** — it postdates (07-04) the newbase migration (07-02); forward-ported in same commit. updateMetadataWithNewDataPage sorted-insert fix EXONERATED (present on both branches, zero chain inversions logged).
- **Honest finding: gerrit/storage-wrap-up-integrate is byte-identical to integrate-newbase HEAD** — the remembered green run predates current replication behavior/test config.
- Diagnostics commit 6f6e468766 (VTNPROBE/VTTOPK/VTCHAIN/VTFLUSH WARN logging) = droppable; keep while iterating. Still to forward-port to this line (or just use vtree-spann-integrate stack which has them): fd-leak fix, deleteFile, trainseed.
- Suite after fix: 12 PASS / 4 WARN (pre-existing recall, unseeded) / 0 FAIL.

## Integration Baseline Comparison (2026-07-06)
- Full integration suite FAILS 3 on BOTH integrate-newbase and vtree-spann-integrate → **pre-existing ANN completeness bug** (missing PKs from mem+disk: baseline 2420/821, ours 2099/691 — ours slightly better). Prime suspect: k-means leaf-drop (~√K leaves + fraction probing). Part4 qualitative win: baseline resurrects **351 deleted PKs after restart** (flush-root bug signature in production); ours = zero resurrected deletes — flush fix confirmed effective end-to-end. Recall WARNs = unseeded-training noise (integration doesn't SET trainseed). Results JSONs in session scratchpad.

## Reworked Stack (vtree-spann-integrate, 2026-07-06 — CURRENT)
- Clean 6-commit Gerrit-ready stack, all Change-Ids/authors preserved: 3c5605c82e p1(+flush-root fix+AiProvenance) / 32d3026ebb p2 / a2ddbacca9 p3(+FlushSearchTest) / 650c912535 3760(+trainseed+leak fixes+3 JUnit only) / 495c7f1f1e 3771(+ALL runtimets/data — moved from 3760) / 5b55d0eea5 SPANN squashed (Calvin author, +create-index-vtree-spann CI case, no selecthead fallback needed). Backup: vtree-spann-integrate-old.
- Validated: 3771' checkout → 33/33 storage, 17/17 JUnit, 3/3 runtimets; top → 4/4 runtimets ×2 (deterministic).
- **Another bug FIXED in 3760':** resetRunFileReader fd leak (6 call sites reassigned reader without closing predecessor — the previously-documented "minor" latent risk; tripped tearDown lsof check with 65 leaked .waf handles). Fix: single-owner currentSampleReader + close-on-reset. Exists on vtree-tests-and-fixes too (unfixed there). SPANN op audited clean for the pattern.

## SPANN Integration (branch vtree-spann-integrate, 2026-07-06)
- `713d15a24e` (creation_mode knob: "bottom-up" k-means default / "top-down" SPANN; renamed Calvin's structure_build) on `dbb49b3601` (cherry-pick of Gerrit 21405 ps3, SpannTopDownCentroidsOperatorDescriptor). Build green, 3/3 runtimets pass (bottom-up unaffected). Cherry-pick silently deleted cross_pollination_m/rng_factor WITH-params — restored.
- `789e40f5f8` ports both k-means fixes into the SPANN op: trainseed now seeds both phases (`trainSeed*31+partition`, +1_000_003 phase offset for BKT) and sample run file deleted in activity finally (dead line-1080 reader removed). 17/17 unit + 3/3 runtimets green.
- SPANN op review — remaining unfixed: clipCentroid clamps coords to ±1e3 silently; fragile BFS-id stored-order assumption; num_clusters ignored when selecthead.enabled (default); 0-heads → Job 2 hard-fails by design; stale class javadoc claims wrong emission order. Plus: clipCentroid clamps coords to ±1e3 silently; fragile BFS-id stored-order assumption; num_clusters ignored when selecthead.enabled (default); 0-heads → Job 2 hard-fails by design; stale class javadoc claims wrong emission order. Not yet fixed or archived.

## Integration Test
- [trainseed knob + create-index-vtree runtimets (2026-07-03)](project-vtree-trainseed-and-runtimets.md) — deterministic k-means via `compiler.vector.trainseed`; CI regression case in runtimets; k-means materializer run-file leak found+fixed
- Test runner: `integration/tests/run_tests.py --parts 3` (Part 3 = insert/delete correctness)
- Launch via `AsterixHyracksIntegrationUtil` (in-process), NOT via asterix-server snapshot
- Build chain: hyracks-storage-am-btree → hyracks-storage-am-lsm-btree → asterix-app

## Planned Features
- [Flat IVF vs VTree Comparison](project_flat_vs_vtree_comparison.md) — Add `flat=true` param to compare flat IVF vs hierarchical tree
- [Dual Navigation Experiment](project_dual_nav_experiment.md) — Two root pages in .staticstructure, query-time flat vs tree via ann_distance 6th arg

## Architecture
- [Bottom-up static structure build](project-bottom-up-static-structure.md) — leaves at low page ids, root at highest; append-only streaming; centroid IDs preserve BFS-from-root (Option I)
- [CLUSTER BY architecture](project-clusterby-architecture.md) — new SQL++ feature; chose Path B (assign-then-groupby): new blocking ClusterAssign op + stock GROUP BY cluster_id; docs in doc-clusterby/
- [CLUSTER BY optimizer-test coverage (PAUSED 2026-06-25)](project-clusterby-optimizer-test-coverage.md) — next task: add APE-proposal movie⋈review/nested/group-by goldens; how to gen plan-goldens; why no live/EXPLAIN test (Phase 2 job-gen throws)
- [Graph leaf-neighbors](project-graph-leaf-neighbors.md) — VTree leaf centroids carry neighbor pointer lists; two-pass storage build on experiment/bottom-up-integrated; step-1 empty-list plumbing done, field-count dispatch hazard noted
- [Cross-pollination DML multi-cluster fix](project-crosspollination-dml-multicluster.md) — insert/delete now replicate into the same M leaf clusters as bulk-load (fixes leaked deletes); CrossPollinationConfig threaded to storage VTree on the ship branch (integrate); index-only ANN Distinct gap also fixed
- [TODO: cross-pollination regression test](todo-crosspollination-regression-test.md) — Part-3-with-M=3 isn't a real regression (passes at M=1 too); need assertions for replication-occurred + index-only dupes==0
- [Gerrit stack handoff (cross-pollination)](project-gerrit-stack-handoff-crosspollination.md) — amended edits into patch1/2/3 + 3771; amendment bundle absorbed into Calvin's 3760 ps20; WAITING on Calvin to rebase 3760 onto new patch3, then restack (both simulated clean)
- [VTree new-base VD migration](project-vtree-newbase-vd-migration.md) — `integrate-newbase` worktree: rebased stack onto LSM-sampling ps12 + dummy VD; ported Training(ps20)+3771 off the reorganized double[]-only vector-distance API; done, not yet build-verified

## Product Constraints
- [VTree is quantized-only at release](project-vtree-quantized-only.md) — non-quantized tuple format & streaming search cursor deprecated; `LSMVTreeSearchCursor` is merge-only, `LSMVTreeTopKSearchCursor` is the sole search cursor
- [VTree distance injection](project-vtree-distance-injection.md) — storage VectorUtils DELETED; canonical distance factory injected + JSON-persisted (full dependency inversion); merge-path null-metric NPE fixed

## Code Reviews
- [VTree Patch 1 Review](vtree_patch1_review.md) — 48-file review: critical bugs (upsert, reflection, LSN), cleanup targets in api/frames/tuples/impls/utils
- [Storage Cleanup Follow-ups](storage-cleanup-followups.md) — Open items after 2026-06-02 cleanup pass: deferred refactors, flagged bugs, dead code awaiting asterix-runtime round

## Debugging Patterns
- WARN-level diagnostic logging added to VCTreeNavigationUtils, NprobeClusterSelectionStrategy, LSMVCTreeSearchCursor
- Use ODID partition identifiers to filter logs per partition (e.g., `ODID:6:0:2:0`)

## Workflow
- [User rebuilds manually](feedback-user-rebuilds-manually.md) — don't auto-run `mvn install` or cluster restart scripts
- [Revert from base, not HEAD](feedback-revert-from-base-not-head.md) — when dropping a stray edit from a Gerrit change, `git checkout master --` (HEAD already has the bad version)
- [Don't `mvn -am test` for a single module](feedback-mvn-test-scope.md) — `-am` runs tests on every transitive dep; use `mvn -pl <target> surefire:test` after a compile step
- [Surefire reads JARs from ~/.m2, not target/classes](feedback-surefire-needs-install.md) — main-source edits need `mvn install` of the changed module before `surefire:test`, else stale JAR runs
- [Keep class javadoc concise](feedback-concise-javadoc.md) — one-line class summaries; don't restate what's on related classes or what `@param` would say
- [Grep isn't enough for dead-code on framework-interface classes](feedback-grep-isnt-enough-for-dead-code.md) — `ICursorInitialState` & friends can have framework-level readers grep won't see; run integration tests, not just unit tests, before dropping fields
