---
name: vtree-branch-model
description: "The VTree (vector-tree / ANN index) project — Gerrit chain, GitHub remote, branches, worktrees, build"
metadata: 
  node_type: memory
  type: project
  originSessionId: 7215a24d-5931-4f2d-a7f7-01a58dabdad1
---

VTree = vector-tree index for ANN vector search in `analytics/asterixdb` (Apache AsterixDB). Focus shifted
here 2026-07-15 (from CLUSTER BY / [[clusterby-branch-model]]). Distinct feature/team from clusterby.

**Gerrit relation chain (open on master, unmerged), top → bottom:**
- 21405 — "DON'T REVIEW: spann vector index" (WIP above the target; NOT fetched)
- 21287 PS22 — ASTERIXDB-3771 ANN query optimizer rule for top-k ANN
- 21159 PS28 — ASTERIXDB-3760 Training VTree index
- 21101/21100/21099 — ASTERIXDB-3754 Storage layer of VTree - patch 3/2/1
- 20959 — ASTERIXDB-3702 LSM Sampling (the base the chain sits on)
Note: user refers to these by Jira number (3771/3760/3754); 3754 = three Gerrit patches. Same Apache
Gerrit as clusterby (asterix-gerrit.ics.uci.edu, SSH user Le0shy). Fetch the TIP (21287 PS22) → whole
chain comes as ancestry. Local branch `vtree` (6730c8f079) = that chain, in worktree ~/vtree-wt.

**REBASED onto new master — LSM sampling merged upstream (2026-07-20).** ASTERIXDB-3702 LSM Sampling is now
MERGED into Apache master (gerrit/master == couchbase/master == `3d6992d0e7`), and it EVOLVED beyond the local
snapshot (added "Address review comments" + "Encode LSM sampling leaf page ids as bitmap+select"). Rebased
`vtree-integ` onto `3d6992d0e7` via `git rebase --onto gerrit/master 79fb779262 vtree-integ` — DROPS the local
3702 LSM commit (now in base), replays only the VTree chain. Two fixups needed:
  (1) **Conflict in 3760** (ErrorCode.java + en.properties): error-code collision — master's evolved LSM added
      `INVALID_SAMPLE_METHOD(1256)`; VTree used 1255-1264. Resolved: VTree keeps 1255, master keeps 1256,
      VTree's 1256-1264 bumped +1 → 1257-1265 (contiguous, both files identical). Baked into the 3760 commit.
  (2) **API rename in 3754-p1** (VTree.java, VTreeBulkLoader.java): the evolved LSM sampling renamed
      `IComponentSampler` → `ISketchSampler` (method `serializeSamplingMetadata()`→`serialize()`, but VTree
      doesn't call it — pure type rename, 8 refs). Folded cleanly into 3754-p1 via cherry-pick rebuild.
  GOTCHA: an isolated `mvn -pl <module>` compile against STALE ~/.m2 gave FALSE errors (CLAUDE_FABLE_5,
  createBulkLoader signature, LocalOnlyWriteContext) — all are present in the rebased SOURCE; only a full
  reactor build (fresh deps in dep order) gives the true list (which was ONLY IComponentSampler). Always
  verify rebase breakage with a full/`-am` build, not `-pl`.
  RESULT: **full `make -j4 install` BUILD SUCCESS** (hyracks+asterixdb+cbas) — rebase fully validated end-to-end.
  **TESTS ALL GREEN post-rebase (2026-07-21):** (a) UNIT: hyracks-storage-am-lsm-vtree-test 26/26, sampling
  (ThetaSketch/LSMThetaSketch/LSMSample* in am-lsm-btree-test) 37/37, asterix vector ops+rewrites
  (KMeans++/VectorExtractor/QuantizationConstants/distance-rewrite) 11/11 — 74 total, 0 failures. (b) GOLDEN:
  SqlppExecutionTest scoped to the `vector` group (inject the group into runtimets/only_sqlpp.xml, run, then
  restore) = 6/6 test-cases pass (distance-functions + create-index-vtree{,-composite-pk,-glove,-movie,-spann}),
  all .adm goldens matched. (c) INTEGRATION (live cluster via ~/vtree-cluster.sh start → :9600, couchbase:couchbase):
  loaded 10k EXPERIMENT.Movie {idx,title,year,embedding[384]} via batched INSERT + ANALYZE, `CREATE INDEX
  movie_vtree ... TYPE VTREE ... EXCLUDE UNKNOWN KEY` OK, EXPLAIN shows VECTOR_SEARCH, ANN self@0 + sensible
  neighbors, 5-query bench ~35ms each, recall@10=10/10 vs exact (vector_distance) at 32 clusters.
  **TESTING GOTCHAS:** (1) the full `make install` does NOT refresh every ~/.m2 jar (hyracks-test-support +
  asterix-common stayed stale from a prior build) → `-pl <module> test` gave FALSE "cannot find symbol"
  (AbstractVectorTreeTestContext, QuantizationConstants). FIX: `mvn -o install -DskipTests` the hyracks-fullstack
  AND asterixdb reactors first to refresh main jars, THEN run `-pl` tests. (2) surefire-junit4 provider not in
  ~/.m2 → run tests WITHOUT `-o` (online) so it fetches. (3) movie_test.py HARNESS is STALE vs vtree-integ:
  uses `CREATE VECTOR INDEX` (newer sugar; branch has `CREATE INDEX ... TYPE VTREE`) and field `id` (dataset
  uses `idx`), and hardcodes :29002 (local svc is :9600, no auth header). So the harness can't run unmodified;
  drove the integration essence directly via curl/python instead.
  **THE REAL SUITE = `integration/tests/run_tests.py` + config.yaml (NOT scripts/movie_test.py).** It FITS
  vtree-integ: `TYPE VTREE` grammar (asterixdb_client.py), `idx` PK, targets EMBEDDED AsterixDB on :19002 via
  AsterixHyracksIntegrationUtil (cc-main.conf), self-manages lifecycle (asterixdb_lifecycle.py builds classpath
  via cached asterix-app/target/test-classpath.txt or `mvn dependency:build-classpath`; `--skip-lifecycle` to
  reuse a running one; `--parts 1 2` to scope). 4 parts: 1 index-creation · 2 ANN-recall · 3 insert/delete · 4
  flush/compact. Datasets in config.yaml: movie(384/esq), glove(100/cosine), gist(960/euc). We only have movie
  + glove locally (NOT gist) → use `config_no_gist.yaml` (movie+glove). RAN 2026-07-21: **14 total · 11 PASS ·
  3 WARN · 0 FAIL · 78.6s → PASSED**. Parts 1/3/4 all PASS (insert/delete: 1500 deleted, 0 leaked, all present;
  flush+compact incl. cluster restart: 0 leaked — the LSM-maintenance-under-mutation paths the sampling rebase
  most exposes = clean). Part 2 movie recall WARN (61-67% < 70% threshold at suite params k=50; glove_cosine
  PASS 75%) — recall-quality tuning, NOT correctness, likely param-driven/pre-existing (direct test at 32
  clusters/k=10 gave 10/10).
  **IMPORT TIDY DONE (2026-07-21):** the ISketchSampler import in VTree.java + VTreeBulkLoader.java was out of
  alpha order (impsort would flag `check`); moved to correct slot (after ISearchPredicate / after IIndexBulkLoader),
  folded into p1 via detached-amend + `git rebase --onto`. Validated with `impsort:1.9.0:check -Dimport-sort.skip=false`
  = BUILD SUCCESS, and vtree-module `install -DskipTests` = BUILD SUCCESS.
  **COMMITTER-EMAIL GOTCHA:** a plain `commit --amend`/`rebase` re-stamps the COMMITTER from local git config
  (hongyu.shi@couchbase.com) — Apache Gerrit rejects that. Authors are preserved but committer flips. FIX: force
  re-apply with the right identity: `GIT_COMMITTER_NAME=Hongyu GIT_COMMITTER_EMAIL=leoshy1005@gmail.com git rebase -f <base>`
  (the `-f` forces recreation even on fast-forward, so the env re-stamps committer). Verify with `--format=%ae/%ce`.
  New chain on `3d6992d0e7` (== gerrit/master; bottom→top): feade3ca33 3754-p1 (rename + import-tidy folded) ·
  a7aa3d59ea p2 · ecd4743898 p3 · b42ce9b065 3760 (errcode-resolved) · 769f8a5b60 3771 · 3623d3b442 spann ·
  1e555cc896 docs. All committer leoshy1005@gmail.com; authors leoshy1005 (VTree) / calvinthomas.dani (3760, spann).
  Change-Ids preserved → a push updates the SAME Gerrit changes (21099/21100/21101/21159/21287/21405) as new PSs;
  docs commit has no Change-Id (local-only, not pushed). Backups: `backup-vtree-integ-preimport` (pre-tidy),
  `backup-vtree-integ-couchbase-committer` (post-tidy pre-committer-fix). range-diff: only p1 differs vs pre-tidy
  (the import move); committer re-stamp = 0 content change.
  **PUSHED (2026-07-21):** storage patches 3754 p1/p2/p3 pushed by Hongyu (author+committer=leoshy1005, no forge
  needed) via `git push gerrit ecd4743898:refs/for/master` → new PSs on 21099/21100/21101, SUCCESS. Pushing the
  p3 TIP carries p1→p2→p3 as one relation chain (Gerrit makes one change per commit in master..tip) — no need to
  push each separately. 3760/3771/spann NOT pushed by Hongyu: 3760+spann are AUTHORED by calvinthomas.dani →
  need Forge Author (+Forge Committer since committer=leoshy≠pusher); Hongyu lacks Forge Author. So handed to a
  Gerrit admin as a git BUNDLE: `~/vtree-rebased-bundle/vtree-3760-3771-spann-rebased.bundle` (tip ref
  vtree-admin-push=3623d3b442 spann, prerequisite ecd4743898=p3 which now exists on Gerrit) + README-admin.md.
  Admin: `git fetch <bundle> vtree-admin-push:vtree-admin-push && git checkout vtree-admin-push && git push gerrit
  HEAD:refs/for/master` → PSs on 21159/21287/21405. GitHub push to vtree/vtree-development-migration is BLOCKED:
  local SSH key auths as hongyushi-creator (no write to calvin-dani fork); the intended "other account" cred is in
  the macOS keychain, reachable only from the user's interactive session (bg job gets "could not read Username").
  **CI VOTERS on Apache asterix Gerrit (2 lanes) + "BRANCH RESTRICTED" (2026-07-21):** (1) APACHE Jenkins
  `asterix-jenkins.ics.uci.edu` posts the real Verified/Integration-Tests — THIS is the Apache-merge gate (was green
  pre-rebase). (2) `cb-jenkins` = COUCHBASE voter: normally runs "Analytics Compatibility" (cbjenkins.page.link URLs,
  votes Contrib) to check asterix changes don't break cbas back-compat. After the rebase onto brand-new master
  3d6992d0e7 (merged LSM-3702, ~1 day old), cb-jenkins instead posted `Verified-1 … asterixdb-restricted-branch-check
  … BRANCH RESTRICTED` (server.jenkins.couchbase.com). CAUSE: the rebased base is NEWER than Couchbase's asterixdb
  mirror has ingested, so the compat build has no known base → gate parks a -1. NOT a code/rebase defect; self-clears
  once Couchbase's mirror syncs the new master. Only today's rebased PS hit it; every prior PS ran normal compat.
  server.jenkins.couchbase.com needs its OWN token (the ~/.jenkins_auth token is scoped to analytics.jenkins only;
  jenkins API user = hongyu.shi@couchbase.com).
  **LEAF-LEVEL DROP FIX implemented locally (2026-07-21, on vtree-integ, UNCOMMITTED):** the k-means hierarchy
  bug (num_clusters=K honored by k-means but the tree ships ~sqrt(K) leaves) is a build-side off-by-one in
  `HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor#performMemoryEfficientHierarchicalKMeans`: `currentLevel`
  started at 0, so `initializeParentLevel(0)` overwrote the leaf level at map key 0 and the leaves were re-keyed
  to -1, which the `for L=0..maxLevel` emission never writes. FIX = start `currentLevel = 1` (leaves stay at key 0,
  parents at 1..maxLevel; emission already correct). REGRESSION PROVENANCE: NOT in 3760 from the start — Gerrit
  21159 PS1-PS19 (Apr24-Jun11) emitted leaves fine via a BFS queue that descended to key -1
  (`childLevel=currentLevel-1` while `containsKey`); PS20 (2026-06-30) refactored emission to a fixed
  `for L=0..maxLevel` loop + idOffset BFS-from-root ids + bottom-up order (legit goals) but the loop never
  visits key -1, orphaning the leaves the build still files there. So my build-side fix is the right
  reconciliation (keeps PS20's id/order machinery, puts leaves back in range) vs reverting to the PS1-era BFS.
  Calvin's weighted-kmeans branch never regressed because it forked off the PS1-era BFS emission. ONE line + comment + method @AiProvenance(ASSISTED). Emission/
  storage format UNCHANGED. NOT the weighted-kmeans approach (that reworks emission to a BFS descending to -1 and
  drags in weighted/format/interface churn; Gerrit 21159 PS29 + migration/spann-integrate branches are all still
  buggy; only Calvin's `weighted-kmeans` fork branch fixes it, entangled). Added regression test
  `testLeafLevelEmittedWithFullClusterCount` (separatedGroups(9,6), frame 512 → asserts deepest level == K=9):
  PROVEN to fail on currentLevel=0 (collapses to maxTreeLevel=0) and pass on the fix. Unit 5/5 green,
  formatter:validate + impsort:check clean. Lands as amend into 3760 (b42ce9b065 / Gerrit 21159). NOT committed/
  pushed — awaiting user go.
  **RECALL RE-VALIDATED (2026-07-21, embedded AsterixHyracksIntegrationUtil suite, integration/tests/run_tests.py
  config_no_gist.yaml parts 1-2):** the fix is a net win, NOT a regression. Movie (was recall-starved by the bug)
  jumped WARN→PASS: movie_esq 58→75%, movie_esq_filter 56→73%, movie_esq_no_include 66→76%. glove_cosine dipped
  75/78%→57% at the DEFAULT min_probe_fraction=0.4, but a glove-only sweep (0.4/0.7/1.0) showed 64%/74%/76.4%:
  glove recovers to PASS at pf>=0.7, and even pf=1.0 (probe ALL clusters, brute force over the index) caps at
  ~76% = the SQ8 quantization ceiling for 100-dim cosine (independent of the fix; glove was ~78% pre-fix too).
  So glove's dip is a TUNING artifact — the correct finer tree (~158 leaves vs ~12) needs a higher probe fraction
  to cover the same space at fixed 0.4; not a correctness regression, no vectors unreachable. Follow-up (separate,
  non-blocking): default min_probe_fraction should scale with cluster count / nprobe. Harness gotcha: a temp
  config's `datasets_dir: ../datasets` + `project_root: ../..` are relative to the CONFIG FILE dir, so a config
  placed outside integration/tests/ breaks path resolution + cluster launch; keep sweep configs in tests/.
  **CRITICAL CORRECTION (2026-07-21): the leaf-drop fix EXPOSES a latent VTree storage bug — DO NOT ship it
  alone.** With the fix active (fine tree ~35 leaves/8k recs), integration Part 3/4 ANN full-recall queries
  (LIMIT 10000) lose ~0.4% of records (7971/8000, 6474/6500). A/B CONFIRMED on the SAME clean build:
  currentLevel=0 (coarse ~5 leaves) PASS, currentLevel=1 (fine) FAIL with the identical missing set. Root-caused
  via temporary WARN diagnostics in NprobeClusterSelectionStrategy + LSMVTreeTopKSearchCursor: (1) NOT cluster
  reachability — the query visits ALL leaves (visitedSet==trueTotal==35; DFS fallback complete). (2) records are
  NEVER SCANNED — totalTuplesProcessed == returned count (7971 not 8000), topKBufferEntries==processed (nothing
  dropped post-scan). So it's a WITHIN-CLUSTER data-page traversal gap: the cursor reads getDataPagePointer(0) +
  the dataFrame.getNextPage() next-chain (VTreeSearchCursor ~L570/723/782); records on data pages not in that
  chain are missed. Fine tree's per-cluster distribution exposes it; disk(bulk-load)+mem(insert) both affected.
  FIX DIRECTION: bulk-loader/flush must link ALL cluster data pages into the next-chain, OR the cursor must
  iterate all directory pointers getDataPagePointer(0..tupleCount-1). Storage work in VTreeBulkLoader/
  VTreeFlushLoader/VTreeSearchCursor + a full-recall storage regression test. (This SUPERSEDES the earlier
  parts-1-2 "net win, no regression" note, which only ran Part 2 recall, not the Part 3/4 full-recall checks.)
  TWO GOTCHAS that cost hours: (a) a "rejected" perl edit had actually LANDED (reverted currentLevel to 0), so
  early "clean-build passes" were secretly testing the coarse tree — ALWAYS re-grep the fix line is present
  before trusting a pass; (b) stale ~/.m2/classpath → ClassNotFoundException(VectorDistanceFunctionFactory);
  refresh asterix-runtime+asterix-app to ~/.m2 AND `rm asterix-app/target/test-classpath.txt` before integ runs.
  **INDEX-ONLY + WHERE-on-INCLUDE bug — guard A implemented (2026-07-21, uncommitted):** PK-only-projection
  ANN + WHERE on an INCLUDE field returned ZERO rows silently. Root cause = two mechanisms collide: index-only
  (`VectorIndexAccessMethod.neutralizeDanglingExpressions`) rewrites the dead record var's field-accesses to
  MISSING (only PK gets a real rewrite, INCLUDE doesn't), so the WHERE becomes select(missing); the deferred
  INCLUDE pushdown (`PushFilterIntoVectorSearchRule`, physical) then has nothing to push. `isProjectionPkOnly`
  only checked above-LIMIT vars, missing the below-LIMIT WHERE. FIX A (`IntroduceTopKAccessMethodRule`): also
  require below-LIMIT SELECT-condition vars to be PK-safe (new `collectSelectConditionVars` + isVarPkSafe reuse),
  else fall back to lookup-and-rerank. Regression test = runtimets `vector/create-index-vtree-include-filter`
  (INCLUDE year, WHERE year>2005, PK-only proj, min_probe 1.0 → golden 6,7,8); PROVEN: PASSES with guard,
  FAILS (actual <EOF>) without. Compiles + format gates clean. Lands in 3771 (Gerrit 21287). FIX B (queued,
  NOT done): teach index-only to project INCLUDE columns off the secondary UnnestMap + rewrite field-access to
  those vars so the pushdown fires into the cursor (keeps index-only speed for filtered ANN). To run one
  runtimets test: inject a test-group into runtimets/only_sqlpp.xml, `mvn -pl asterix-app -Dtest=SqlppExecutionTest
  test`, restore only_sqlpp.xml (back it up first).
  **FIX B ATTEMPTED + REVERTED (2026-07-21) — hit an architectural wall.** Tried to make index-only serve the
  INCLUDE filter (skip primary lookup) by extracting the pushdown embed into a shared static helper
  `PushFilterIntoVectorSearchRule.embedIncludeFilterIntoVectorUnnest` and calling it from the index-only branch,
  relaxing guard A to allow INCLUDE WHEREs. RESULT: embedding in the LOGICAL access-method phase FAILS — the
  embedded INCLUDE filter var is an unnest output referenced only by the operator's selectCondition annotation,
  and logical type-env recompute can't infer it → hard `Could not infer type for variable '$$N'` (proven by
  forcing indexOnly=true + EXPLAIN). PushFilterIntoVectorSearchRule runs in physicalRewritesTopLevel precisely
  to avoid this. KEY REFRAME: in A's lookup-and-rerank fallback the physical pushdown ALREADY pushes the filter
  into the cursor (EXPLAIN: VECTOR_SEARCH carries `condition (gt($$32,2005))`), so filter-before-topK recall is
  already correct; B's ONLY gain is skipping the primary BTREE lookup for the PK projection = modest perf. So B
  needs a DEDICATED PHYSICAL-PHASE rule (embed after the index-only plan is set), not a logical-phase reuse.
  Reverted all B (3 optimizer files back to committed via `git checkout`), re-applied clean guard A. FINAL state:
  guard A only (+41 lines in IntroduceTopKAccessMethodRule) + runtimets test; include-filter returns 6,7,8 AND
  baseline create-index-vtree (PK-only→index-only) both PASS. Lands in 3771 (Gerrit 21287).

**GitHub remote `vtree`** = https://github.com/calvin-dani/asterixdb-schema-knn (Calvin Dani's fork; added
2026-07-15, shared across all worktrees). Many branches (VLDB_Vector_Index, VLDB_Schema_Inference, spann-*,
vtree-*, etc.). KEY: its branches FORCE-UPDATE (rebase) — re-fetch + reset, don't merge.

**`vtree-development-migration` branch** (Calvin's) = the Gerrit VTree stack + SPANN operator + additive
"migration" content. After its 2026-07-15 rebase "onto current code stack", diff vs local `vtree` is CLEAN
(merge-base = 3754-p1 31d0729825; 194 files, +26k/−21). Composition: (1) doc-vtree/ (28, impl docs),
(2) dev-context/ (33, reviewer artifacts), (3) integration/ (~127, integration test suites) — ALL pure
additions, committed IN-TREE (unlike clusterby docs); plus the SPANN delta = 5 prod files (the +2729-line
`SpannTopDownCentroidsOperatorDescriptor.java` + wiring in SecondaryVectorOperationsHelper/VectorIndexDeclUtil/
CompilerProperties/VectorDistanceFunctionFactory) from the "spann vector index (top-down creation_mode)" commit.

**WORKTREES / where things are (2026-07-15):**
- `analytics/asterixdb` (MAIN / repo superproject location) = branch `vtree-migration` (local, tracks
  vtree/vtree-development-migration, tip 6ba01885e3). BUILT (asterixdb reactor). Put here because vtree work
  needs the FULL analytics/Columnar monorepo build (`make -j4` at analytics root) — the `.repo` manifest
  expects asterixdb at analytics/asterixdb; standalone ~/ worktrees are embedded-AsterixDB-only, not wired
  into the superproject. `clusterby` (P20) now checked out nowhere (safe; `git switch clusterby` to return).
- `~/vtree-wt` = branch `vtree` (Gerrit chain, 6730c8f079). BUILT.
- `~/clusterby-gerrit-wt` = `clusterby-gerrit` (unrelated; isolated m2head).
- (removed) `~/vtree-migration-wt` — was the standalone vtree-migration worktree; deleted when its branch
  moved to the main checkout.

**INTEGRATION INTO COUCHBASE ANALYTICS (scoped + baseline-verified 2026-07-15):** `analytics/` is a
`repo`/manifest workspace (`couchbase/manifest`, `enterprise-analytics/helios.xml`, `-g all`) pinning
asterixdb→`couchbase/master`, cbas→`cbas-core master`, analytics-dcp-client→master. VTree was developed
as PURE APACHE asterixdb (Calvin's fork) and has NEVER been integrated into cbas/analytics. cbas builds
against the IN-TREE asterix (relativePath `../asterixdb/asterixdb`, ver 0.9.10-SNAPSHOT).
- **Scoping (couchbase/master vs vtree-migration):** merge-base `861e55506d` (2026-07-08) — forks diverged
  only ~1 week. couchbase-only 10 commits (plan-cache 3183, dep bumps, gerrit merges), vtree-only 8 (the
  chain + LSM base + SPANN). couchbase/master ALREADY has 3676 (vector distance); LACKS 3702/3754/3760/3771.
  Replay conflict surface = 6 files: 3 test .regexadm (trivial) + SqlppCompilationProvider(2/2),
  CompilerProperties(10/1), QueryTranslator(couchbase +148/-17 plan-cache = the one real merge). So
  approach A (replay 8 VTree commits onto couchbase/master) is SMALL, not a cross-fork port.
- **The cbas "drift" was a red herring:** cbas's CLAUDE_UI / TestExecutor errors were purely because the
  user's cbas checkout was the STALE `clusterby/main` (Jun 16, 62 commits behind, 0 unique commits — just
  an old master pointer). BOTH couchbase/master AND vtree asterix already have CLAUDE_CODE_UI.
- **BASELINE VERIFIED GREEN (2026-07-15):** asterix `couchbase-master` (2dbf57060d) + cbas `cbas-master`
  (cbas-core master 7a87cd4d1) → hyracks+asterixdb install to ~/.m2 + cbas-server test-compile (fail-at-end)
  = ZERO errors. So couchbase/master asterix + cbas-core master are a clean matched baseline; CloudClusterIntegrationUtil
  (cbas-server test, Adobe S3Mock, boots 2-node Columnar + 1-node KV, loads travel/beer samples, blocks) present.
- **Current branch state:** MAIN asterix worktree = `couchbase-master` (was vtree-migration); cbas = `cbas-master`
  (was clusterby/main). ~/.m2 now holds couchbase/master asterix. Integration base is ready.
- **INTEGRATION DONE (approach A, 2026-07-15):** branch `vtree-integ` = couchbase/master (2dbf57060d) +
  the 8 VTree commits cherry-picked (`git cherry-pick 861e55506d..vtree-migration`), **ZERO conflicts** (the
  ~6 predicted overlap files auto-merged; new hashes 79fb779262..b0ea27831b). Checked out in MAIN asterix
  worktree. Integrated hyracks+asterixdb (couchbase/master+VTree) BUILD SUCCESS → installed to ~/.m2;
  cbas-server (cbas-master) test-compiles clean against it. So VTree is fully compatible with couchbase/master
  at build level. The integration/ harness came along (scripts/, tests/, docs); datasets/ are untracked
  (gitignored) and persist across switches.
- **RUN model:** `integration/scripts/movie_test.py` (NOT movie_index_test.py — doc is stale) drives the test:
  connects to `http://localhost:29002/query/service` (analytics query svc, via SSH tunnel); subcommands
  create/drop/query/bench (e.g. `create --name idx --num-clusters 100 --dimension 384 --similarity euclidean_squared`).
- **RUN BLOCKER (not a build issue):** CloudClusterIntegrationUtil boots a 2-node Columnar + **1-node KV**
  cluster and `loadSampleBucket("travel-sample"/"beer-sample")` — needs the **couchbase-server (KV/ns_server)
  runtime**, not just the Maven jars. So it's an IDE/dev-env launch (with couchbase-server available), not a
  clean headless CLI run. Docker is up (for the Adobe S3Mock it uses). Once the cluster is up (analytics on
  the tunneled 29002), movie_test.py can create/query the vector index.
- To run the vector-index test (MOVIE_INDEX_TEST.md): launch CloudClusterIntegrationUtil, then
  `python scripts/movie_index_test.py movie_embeddings_384d.json <numClusters> <k> <year>` against the live cluster.

**RAN IT END-TO-END (2026-07-15) — key finding: the block is CBAS-SIDE, precisely located.**
- Refreshed the analytics component into $WORKSPACE/install via `cd analytics && make -j4 install` (KV/ns_server
  binary untouched Jun17; cbas+asterix jars refreshed 17:05 — install DOES contain VTree: hyracks-storage-am-vtree,
  -lsm-vtree, asterix-runtime w/ SpannTopDownCentroids). `make` in analytics/ builds ONLY the analytics component
  (delegates to $WORKSPACE/build/analytics CMake→mvn); the full couchbase-server (KV/ns_server) is `make` at
  $WORKSPACE (already built, install present).
- LAUNCHED CloudClusterIntegrationUtil headlessly: `cd analytics/cbas && mvn -o -pl cbas-server exec:java
  -Dexec.mainClass=com.couchbase.analytics.test.CloudClusterIntegrationUtil -Dexec.classpathScope=test`
  (needed `mvn install` of the cbas reactor to ~/.m2 first so exec:java resolves siblings). Docker up (Adobe S3Mock).
  Cluster came up: 3 NCs ACTIVE, KV+samples, remote link. **Analytics query svc = http://localhost:9600/query/service,
  basic auth couchbase:couchbase.** (movie_test.py's 29002 is just a dev SSH tunnel.)
- Loaded 2000 movie records (idx,title,year,embedding[384]) via batched INSERT (columnar rejects `LOAD ... USING
  localfs` = "Internal error" 25000, and rejects multi-statement requests = 21003; use single fully-qualified stmts +
  INSERT). CREATE DATAVERSE/TYPE/DATASET(column) + ANALYZE all succeed. requests lib: `pip install --user requests`.
- **`CREATE INDEX ... TYPE VTREE` REJECTED: "Unsupported index type" (cbas err 13 / BAD_INDEX_TYPE), on BOTH row &
  column datasets (BTREE works).** ROOT CAUSE (definitive): cbas gate `cbas/cbas-connector/.../lang/
  CBStatementExecutor.java` `validateIndexType()` (~L2029) hard-whitelists only `IndexType.BTREE` and `ARRAY`, throws
  BAD_INDEX_TYPE otherwise. VTree is fully present in the running ASTERIX binaries (grammar has VTREE, DatasetConfig.
  IndexType.VTREE, storage/runtime jars) — but the CBAS layer never learned VTree. So the asterix-side integration is
  done; the CBAS-side integration is NOT: minimally `validateIndexType` must permit VTREE, plus whatever downstream
  columnar index-maintenance (DCP/column-storage) VTree needs. That's the real remaining "VTree into cbas" work.
- Cluster left RUNNING (blocking) at 9600 for further experiments; kill the exec:java bg job to stop it.

**END-TO-END SUCCESS (2026-07-15): VTree vector index created + queried on a COLUMNAR Couchbase collection
via CloudClusterIntegrationUtil — first time.** Opened the cbas gate with a 2-line patch to
`cbas/cbas-connector/.../lang/CBStatementExecutor.java`: (1) `validateIndexType` (~L2029) now allows
`IndexType.VTREE`; (2) `validateIndexFieldType` (~L2040) skips the PUBLIC_PRIMITIVE_TYPES_INDEXABLE check
when `indexType==VTREE` (vector field is an array, asterix super-validation is VTree-aware). Then
`make -j4 install` (refresh $WORKSPACE/install cbas jars) + RESTART cluster (kill exec:java + orphaned
beam.smp/memcached under $WORKSPACE/install + `docker rm -f` leftover couchbase KV container, then relaunch).
Result on live 9600: `CREATE INDEX movie_vtree ON EXPERIMENT.Movie(embedding VECTOR) TYPE VTREE WITH
{"dimension":384,"similarity":"euclidean_squared","train_list_fraction":0.1,"epsilon":0.3} EXCLUDE UNKNOWN
KEY` → SUCCESS (1.6s, 2000 recs). Metadata.Index shows movie_vtree IndexStructure=VTREE. ANN query
(`ann_distance(embedding, qvec, "euclidean_squared")` ORDER BY … LIMIT 5) returns correct neighbors
(self at dist 0, then sensible crime-themed movies), and EXPLAIN shows the plan USES the VTREE index. So
asterix-side columnar integration is proven functional through cbas once the gate is opened. NOTE: this
is a local spike (cbas-master patch, uncommitted, 2000-rec small scale). Proper cbas integration = land the
validateIndexType/FieldType change cleanly + soak-test SPANN/LSM maintenance under live DCP mutation.
CLUSTER-RESTART GOTCHA: hard-killing the launcher orphans the columnar ns_server (beam.smp 9000-9002) +
memcached (12000-12004) + the Docker couchbase KV container; clean ALL before relaunch or init's
"CB cluster shutdown not achieved (10s)" fails. Single clean launches (don't share a log / pkill-churn).

**ROW-vs-COLUMN maintenance analysis (verified in code 2026-07-15): the ASTERIX side is columnar-complete
for VTree; the ONLY wall is the cbas validateIndexType gate.** A secondary index is always its own
structure (not columnar) — normal. All 3 storage-boundary points are explicitly VTREE-handled in vtree-integ:
(1) BUILD — `SecondaryVectorOperationsHelper` uses a format-aware tuple projector (ROW full-record / COLUMN
project embedding+PK) via `IndexUtil.createPrimaryIndexScanTupleProjectorFactory`;
(2) MAINTENANCE — `IntroduceSecondaryIndexInsertDeleteRule` has `case VTREE` (insert ~L321 + delete/upsert
~L875), tuple-level & format-agnostic ("handled similarly to VALUE indexes"), + physical op in
SetAsterixPhysicalOperatorsRule case VTREE, + full LSM modify/flush/merge (hyracks-storage-am-lsm-vtree:
LSMVTreeOpContext/FlushOperation/IClusterSelectionStrategy);
(3) COLUMNAR UPSERT BEFORE-IMAGE — `DatasetUtil.getPrevRecordType` (asterix-metadata DatasetUtil.java:611)
has `case VTREE` → unions VectorIndexDetails.getIndexExpectedType() so the embedding IS projected for
delete-old-key (uses UpsertPreviousColumnTupleProjector). So "VTree row-store on columnar primary" is NOT a
blocker. Remaining genuine risk = VTree's own SPANN cross-pollination + LSM flush/merge correctness under
live mutation (soak-test), NOT plumbing. DCP = Couchbase Database Change Protocol (KV mutation stream →
analytics via analytics-dcp-client); relevant for LINKED collections (standalone uses INSERT/UPSERT DML).

**BUILD:** vtree & vtree-migration share identical hyracks (delta is asterix-side SPANN only). Once ~/.m2
has vtree hyracks, an asterixdb-only `mvn clean package -DskipTests -Dmaven.gitcommitid.skip=true` in the
inner `asterixdb/` reactor suffices (~3 min); no hyracks reinstall needed between vtree↔vtree-migration.
Uses standard ~/.m2 (NOT the clusterby m2head). ~/.m2 currently holds vtree/SPANN hyracks — so returning to
clusterby/P20 in the main worktree needs the hyracks-first rebuild ([[build-main-worktree]]). Full Columnar
build = `cd ~/Proj/workspace/analytics && make -j 4` (CMake-orchestrated; now picks up vtree-migration).
