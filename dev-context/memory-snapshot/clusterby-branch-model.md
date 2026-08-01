---
name: clusterby-branch-model
description: How the asterixdb CLUSTER BY work is split across git branches, its Gerrit state, and in-flight deps
metadata: 
  node_type: memory
  type: project
  originSessionId: 2147a65d-7f57-419c-ad59-1b3f153979ce
---

CLUSTER BY (ASTERIXDB-3783) lives in the `analytics/asterixdb` submodule (submodule branches, not the
parent repo's `clusterby/main`).

**BRANCH INVENTORY (updated 2026-07-15) — 3 branches. LATEST STATUS = `clusterby` (P20 dev) +
`clusterby-gerrit` (G3 formal series). `clusterby-m2` was NOT the "usual run branch" (mislabel, fixed).**
- `clusterby` c062ea3d17 — canonical P1-P20 dev history + re-slice source; the LATEST informal dev.
  Now checked out in the MAIN asterixdb worktree (~/Proj/workspace/analytics/asterixdb).
- `clusterby-gerrit` 1306687f37 — active formal series G1/G2/G3 (checked out in ~/clusterby-gerrit-wt,
  isolated m2head build).
- `clusterby-dev` 1ce02bb9eb — 3702-integrated two-branch model. Stale (v3 pivot dropped 3702 dep) but
  KEPT (only holder of the 3702 integration + build-integration replay); delete only if 3702-sampling
  path is truly abandoned.
DELETED 2026-07-15: `clusterby-m2` 8c0349527c (M2/≈P12 milestone, contained in clusterby; main worktree
  switched clusterby-m2 → clusterby, then -d). Also the throwaway `clusterby-g3-run` 1306687f37 (dup of
  clusterby-gerrit, made to run G3 from main worktree). DELETED 2026-07-13 (recoverable via reflog ~90d):
  clusterby-firstpatch-deprecated c553c89d72, clusterby-deprecated 62780da701, clusterby-repack 3db046b48f.

**Current stack (2026-07-08), branch `clusterby` — rebased onto upstream master:**
- base = upstream master `5b224b5a44` (includes 3676 vector distance, MERGED as `6fb7cf2342`)
- P1 `cbbb828b0b` "grammar, desugar rewrite, and EXPLAIN" = **Gerrit change 21384, PS6**
  (pushed 2026-07-08 with user approval; Change-Id I250cf1abe7cfe8dccac75b154856f6d2d771cf28).
  PS6 = PS5 + test files covering the CLUSTER BY syntax itself (kmeans-syntax-validate,
  kmeans-syntax-full-validate = the APE 33 example query, w/ plan goldens) and 3 v1-scope
  negatives (multiple-from-terms, join-in-from, setop-with-clusterby — the APE's own
  Design #1/#3 queries). Self-contained, no deps.
- P2 `a1e4916be4` "distributed k-means runtime" — local only, NOT pushed yet. Depends only on
  merged master (3676 now upstream), so it can be pushed as a simple child on 21384.
- P4 `5d770c30b9` "nearest_centroid_distance function" (I0e93d86f...) — increment A: d2(x,C)
  distance-to-set scalar (shared evaluator, emitDistance flag). Local only.
- P5 `4951c4ae99` "k-means|| oversampling rounds" (I6e32d45a...) — increment B at 2 ROUNDS (not 5).
  GOTCHA (cost me a 2.5h hang): the compiler inlines LET subplans PER REFERENCE — chained-LET rounds
  grow the plan EXPONENTIALLY (3 refs/round ^ 5 rounds = optimizer fixpoint ran for hours; jstack showed
  recursive rewriteOperatorRef). Fix: LET-bind the score once per round + rounds=2 (same profile as the
  Lloyd chain, ~1.5s/query). INIT_OVERSAMPLING_ROUNDS has a loud warning; raising it (and psi/Bernoulli)
  is GATED on engine-side common-subplan materialization — now the top follow-up. Local only.
- P6 `c9cf1d2f8a` "k-means|| weighting and recluster" (I4059d9a9...) — increment C: steps 6-7 replace
  the LIMIT-k bridge (weighting GROUP BY + top-k-by-weight group means). Desugar gotchas: aggregates in
  SELECT VALUE ORDER BY don't resolve; direct SCALAR_SQL_COUNT fid → "Illegal state: array_sql-count",
  emit parser-style name-based count() instead; LET+WHERE form loses topK pushdown (minor perf). Local only.
  Next: engine-side subplan sharing (gates rounds=5/psi/materialization); weighted k-means++ recluster fn;
  cluster_radius; distance-fn routing. Consider pushing P2-P6 stack to Gerrit.

**Subplan-sharing design + experiment A0 (2026-07-09, POSITIVE):** root cause of the exponential
compile is SqlppQueryRewriter.inlineWithExpressions() (language-level, option `inline_with` default
true). With `SET inline_with "false"`: rounds=5 compiles in 1.15s (vs hours), plan linear
(~19 lines/round), DATASOURCE_SCAN 13→2 (scan collapse free — __vecs becomes a shared value).
Full analysis + options A/B/C in doc-clusterby/design/distributed-kmeans-design/ENGINE_SUBPLAN_SHARING.md.
Option A validation: step 1 DONE (2026-07-09) — kmeans-syntax-noinline exec test (SET inline_with
false) PASSED with byte-identical results (scratch test dir kmeans-syntax-noinline in worktree,
uncommitted; remove before P7 commit unless kept as regression test). Step 2 PARTIAL (2026-07-09): scratch tests movies-scale-inline/-noinline (uncommitted, in worktree;
open MovieType {id,embedding} + localfs load of clusterbyTests/movie_filtered_indexed.jsonl). The
INLINE variant completed ONCE: 8 balanced clusters summing to exactly 99,936, dim=384 — v1 pipeline
validated at scale; that output adopted as shared golden for both variants. BLOCKED since: every
surefire fork dies with silent exit code 11 early in the scale test — NOT memory (10GB same), NOT
disk (798G free), NOT zombie JVMs/port 8000 (verified clean), no hs_err/no banner. Suspect the
sandboxed shell kills the process (946MB file mapping?). WORKAROUND to try: user runs the mvn
surefire command directly (! prefix, outside sandbox) or via cluster_run. The noinline-vs-inline
timing comparison is the only missing datum; correctness of noinline already proven at small scale.
Step 3 DONE — P7 `56398bd10d` "full k-means|| rounds via shared LETs" (I7ec41f6b...) local only:
LangRewritingContext.markNoInlineLetVar + InlineWithExpressionVisitor skip + desugar marks all
centroid LETs (NOT __vecs, kept inlined per user decision — skipped the blocked scale measurement).
Rounds=5 compiles ~4s/query, 14 scans (linear). New regression test kmeans-syntax-noinline.
All 12 cluster-by cases green. First patch touching shared lang machinery (lang-common + inline
visitor) — expect closer upstream review. movies-scale scratch tests deleted (946MB data dep);
Remaining backlog: psi/Bernoulli sampling, weighted k-means++ recluster fn, real cluster_radius,
distance-fn routing, algebra-level CSE (option B) as the general upstream successor.

**STRATEGIC PIVOT (user decision 2026-07-09):** compile-time/desugar layer is DONE for v1 (P1-P7,
freeze it). Shift to RUNTIME optimization: option C (HKPP dedicated-operator pattern, 3760 reference)
as the guide, and take ASTERIXDB-3702 LSM-sampling as a dependency EVEN THOUGH UNMERGED (user: "almost
close to end, we could rely on it") — latest = change 20959 PS17 (2026-07-08; was pinned PS12 before).
Consequence: the two-branch model RETURNS. R1 DONE (2026-07-09): build-integration.sh updated
(3676 dropped as merged; SAMPLING_REF=refs/changes/59/20959/17; base=merge-base(clusterby, gerrit/master))
and run with --force. `clusterby-dev` (main checkout) = master base + 3702@PS17 + P1-P7 replayed clean;
dev-base marker 1ce02bb9eb. Validated: hyracks + asterix build green, all 12 cluster-by tests pass.
DURABLE ISOLATED MAVEN REPO for dev-branch builds: -Dmaven.repo.local=$HOME/.m2-clusterby-dev
-Dmaven.repo.local.tail=$HOME/.m2/repository (never install dev-branch artifacts to ~/.m2 — that
caused the PS3 CI trap). Parked: psi/Bernoulli desugar work, option B CSE.
**v3 PIVOT-CORRECTION (team review 2026-07-09, supersedes 3702 plan):** CLUSTER BY input = the
block's upstream OPERATOR OUTPUT (post-WHERE/pushdown), not necessarily a scan → 3702 CANNOT sample
a stream → 3702 DEPENDENCY DROPPED, M3 removed. M1+ now builds on CLEAN `clusterby` (clusterby-dev
not needed for this). Store materializes the WHOLE input stream (must tap existing pipeline, never
its own scan); future sampling = stream-level (reservoir in Store). Endgame: Lloyd + labeling also
re-stream the run file. P8 `00315d9c4b` "qualified input and leaner round sorts" (Id97eac12...) COMMITTED, clusterby tip:
(1) block WHERE now copied into __vecs (verified: APE filter 1→14 occurrences in plan; was a real
semantics bug — centroids over unfiltered data); block LET rejected + let-in-block negative test
(13 exec cases now); (2) direct-score calls (13→8 full sorts; partial — scale fix = runtime operator).
M1a DONE — P9 `5fc2ad2df2` (I24555d93...): KMeansInitCandidatesOperatorDescriptor in
asterix-runtime/.../runtime/operators/ — 2 activities (Store: MaterializerTaskState run file +
setStateObject; Score: broadcast pool collect → state.writeOut re-stream → bounded top-l heap,
score DESC/arrival tie-break, d2=0 excluded → emit strongest-first). Direct-drive unit test
KMeansInitCandidatesOperatorTest PASSES (TestUtils ctx; hyracks-test-support added test-scope).
NOT compiler-wired yet. M1b FORK to decide with user: datasource-function route IMPOSSIBLE
(leaf-only, we need 2 inputs); options = (a) marker function in desugar + optimizer rule swaps
assign→operator pair, (b) minimal internal logical operator (mini Phase-A revival, translator
emits it when flag on), (c) physical-rewrite matching desugar plan shape (fragile, disfavored).
Movie query = M2 gate.
M1b APPROVED (2026-07-10): option (b) logical operator per M1B_LOGICAL_OPERATOR_DESIGN.md.
User decisions: Q1 = 5 self-contained Store/Score pairs for M2 first (share later); Q2 = classes
live in asterix-algebra. M1b-1 scope: KMeansInitCandidatesOperator logical op (new tag) + visitor
surface + KMeansInitCandidatesPOperator (input1 REQUIRES BROADCAST; contributes P9 descriptor) +
translator interception of internal fn kmeans-init-candidates(vecs,pool,l) in
SqlppExpressionToPlanTranslator (plan born honest — no optimizer-rule window) + compiler property
`cluster_by.runtime_init` default false. TEMPLATE for the ~27 visitor files: the Phase-A retire
commit `12afa60a9a` (on OLD clusterby-dev history, main checkout) — its REVERSE diff lists every
visitor/interface file + trivial impls to re-add for the new op. EXPLAIN-on-fixture = M1b-1 gate. Reverse diff SAVED to
~/.claude/jobs/7215a24d/tmp/phaseA-visitor-template.patch (34 files; +++ sections = files to touch,
their added hunks = the trivial visit() impls to mirror for the new op). M1b-1 IN PROGRESS in worktree (clusterby @ P9). CORRECTION to Q2: operator classes MUST live in
algebricks-core (visitor interface is there; asterix not visible from it) — Phase-A precedent, user
informed. DONE: KMeansInitCandidatesOperator.java (algebricks-core logical pkg; 2 inputs, produces
candidateVar only, no owned exprs, blocking, fields vectorVar/poolVar/candidateVar/candidateVarType/
topCount) + LogicalOperatorTag.KMEANS_INIT_CANDIDATES + PhysicalOperatorTag.KMEANS_INIT_CANDIDATES.
NEXT: visitor sweep from ~/.claude/jobs/7215a24d/tmp/visitor-impls.txt (34 files, per-file Phase-A
bodies; rename ClusterAssign→KMeansInitCandidates; NON-TRIVIAL adaptations needed where Phase-A
fields (clusteringExpr/producedVars) differ from ours: UsedVariableVisitor (add vectorVar+poolVar),
ProducedVariableVisitor (candidateVar), SubstituteVariableVisitor, both DeepCopy visitors,
Isomorphism visitors, pretty-printers. Then POperator (adapt ClusterAssignPOperator template body in
same file), then translator hook + `cluster_by.runtime_init` property, then EXPLAIN gate.
M1b-1 DONE — P10 `36943a4c3f` (Ib7390d9d...) GATE PASSED: flag off = plans unchanged (goldens
green); flag on (SET `cluster_by_runtime_init` "true") = EXPLAIN shows KMEANS_INIT_CANDIDATES,
input0 partitioned scan, input1 BROADCAST_EXCHANGE (from POperator required property). Key
integration lessons: (1) LET translation casts binding root to AssignOperator → top the construct
with an ASSIGN; (2) SET params must be whitelisted in SqlppCompilationProvider.getCompilerOptions;
(3) operator input branches must be SELF-CONTAINED (deep-copied subqueries) AND the construct must
ride a SubplanOperator over the enclosing chain or upstream LET vars type to null (NPE in
InferTypesRule); (4) translator intercepts via overridden visit(CallExpr) matching fn name.
M1 DONE — P11 `80d345f0cb` (Ied2de88f..., amended to add promoted flag-on optimizer test
kmeans-syntax-runtime-validate + golden; runtime lessons in commit msg: blocking-edge connectors,
column resolution, expression-held variable refs, honest delivered properties, OPEN-format emission
for ANY-typed outputs).
M2 DONE (2026-07-09) — P12 `8c0349527c`: ALL 5 rounds runtime via a linear OPERATOR TOWER — round
r's pool input IS round r-1's output stream (desugar emits nested kmeans-init-candidates CallExprs;
translator recurses on the pool arg via translateKMeansCallAsStream, only the outermost call is
listified; each level output gets the anchor-ASSIGN discipline). KEY ENABLER: operator now emits
POOL ∪ CANDIDATES — partition 0 echoes the (broadcast, hence complete) pool through before its
local picks; without the echo, a self-contained pool arg needs exponential re-derivation of prior
rounds. Flag-on plan: 5 chained KMEANS_INIT_CANDIDATES + broadcast exchanges between rounds, seed
limit-1 at the base, ZERO full-dataset sorts in init (the pre-M2 flag-on plan compiled each
remaining desugared round to a constant-key full sort + whole-input micro-sort). Gates: kmeans-
syntax-runtime byte-identical to desugar golden (14/14 exec green), 4 flag-off optimizer goldens
unchanged, tower plan golden pinned, unit test covers echo.
KNOWN SEMANTIC DIVERGENCE AT SCALE (fixture too small to show it): operator keeps LOCAL top-l per
partition, merged WITHOUT a global re-limit → pool can grow ≤ P×l per round, vs the desugar
reference's GLOBAL top-l (it serializes through one partition via hash-on-constant). Harmless for
k-means|| quality (more candidates only helps; recluster prunes to k) but the two paths can diverge
once any partition holds > l qualifying points; a global re-limit (second reduce) is backlog.
MOVIE-SCALE GATE PASSED (2026-07-09): embedded 2-NC instance (AsterixHyracksIntegrationUtil main,
cc-main.conf, port 19002) built from P12; 99,936 × 384-dim movies (asterixdb/clusterbyTests/,
localfs load host = asterix_nc1, 17s). Flag-on CLUSTER BY (k=8, CLUSTER AS members, full
acceptance query): **77s SUCCESS** vs 499s-CANCELLED baseline — 8 balanced clusters (9k–16k),
dims all 384, sizes sum exactly 99936. radius still 0.0 placeholder.
P13 DONE — `fa88c7581d` global re-limit: inter-stage rows are ENVELOPES [kind, partition, seq,
score, vector] (kind 0=pool/1=cand/2=partial); consumers normalize intake deterministically (pool
by seq; global top-l by score DESC/partition ASC/seq ASC — order from FIELDS, never frame arrival);
FINALIZE mode unwraps to plain vectors. Movie: 41s (pool 321→81), partition-count independent,
restart-deterministic (same cluster multiset across cold restarts, labels permute).
P14 DONE (2026-07-10) — `19b9b1276a` WEIGH+RECLUSTER as tower stages (Mode enum on the operator;
2 new internal fns kmeans-weigh-candidates/kmeans-recluster ride translator recursion; WEIGH =
terminal re-limit + decode-once tight-loop partials kind=2 [2,partition,poolIdx,count,sumVec];
RECLUSTER = deterministic partial merge (poolIdx,partition order fixes float summation), top-k
means by weight DESC/poolIdx ASC, pool-member pad; desugar runtime branch binds C0 directly,
__wpairs/__top/pad stay flag-off reference). Movie: **22s** (weighting 184s CPU → tight loops).
P15 DONE (2026-07-10) — `db5210f5f3` Lloyd folded into tower: per iteration WEIGH(prev centroids,
plain intake) + new LLOYD merge mode (ALL non-empty means in pool order, no rank/no pad — empty
member drops like reference GROUP BY); translator derives envelope-intake from nested call's MODE
(ROUND/WEIGH=envelopes, RECLUSTER/LLOYD/FINALIZE=plain); only final C LET-bound. Flag-on plan =
13-stage tower; remaining desugared: seed, final labeling GROUP BY (members), plumbing.
FULL JOURNEY: 499s-CANCELLED → 70s (M2) → 41s (P13) → 22s (P14) → **13s (P15)**; 14/14 exec
byte-identical throughout; 5 unit tests cover all modes.
STALL MYSTERY SOLVED: all wall≫server anomalies (667s/436s/32min/1081s) were the MacBook's IDLE
SLEEP ON BATTERY freezing the JVM (pmset log sleep blocks match wall gaps exactly; server metrics
use monotonic clocks that pause in sleep). NOT a code bug. Wrap long benchmark commands in
`caffeinate -i` from now on.
KNOWN NONDETERMINISM (pre-existing since P4, both paths): the SEED is `LIMIT 1` w/o ORDER over a
4-partition random-merge — partition race; first-query-after-fresh-load once picked a different
seed → different (valid) clustering. Steady-state runs reproduce the SAME multiset as P13/P14.
Follow-up: deterministic seed (min-by-pk before projecting embedding; __vecs has no id — needs
desugar rework in BOTH paths).
P16 DONE (2026-07-10) — `2d0d95e81a` deterministic seed: lexicographically SMALLEST vector
(ORDER BY value LIMIT 1 → streaming topK-1) in BOTH paths; min-by-pk impossible (arbitrary FROM
sources, no known key; __vecs drops fields). GAUNTLET: first-after-load + warm + cold-restart all
identical multiset at movie scale (~14s). Fixture results unchanged (first row was already lex-min
there); 3 syntax plan goldens regen'd. Seed race CLOSED.
P17 DONE (2026-07-10) — `454b942659` real cluster_radius = sqrt(MAX(dist to ASSIGNMENT centroid))
via pre-group block LET (block LETs become GROUP AS fields — satisfies aggregation sugar AND keeps
two-step decomposition; when CLUSTER AS used, members carry extra "__cbdist" field). THREE compiler
lessons: (1) aggregate args with free outer vars can't decompose two-step → group materializes →
HYR0089 budget blowup at scale; (2) scalar builtins CANNOT be name-based null-dataverse CallExprs
(NPE in resolution) — only AGGREGATE names (count/max) get sugar special-casing, use fids for
scalars; (3) NEAREST_CENTROID/NEAREST_CENTROID_DISTANCE added to AsterixInlineVariablesRule
doNotInlineFuncs (spatial-fn precedent): inlining assigns of these past projections dragged the
broadcast C-list per-row through the labeling sort (44KB tuples, 5× slower). ALSO: final centroid
list now SORTED BY VALUE pre-labeling — cid labels were merge-arrival-order nondeterministic
(masked for weeks by label-symmetric fixtures cnt 3/3 radius 0.0; radius made rows distinguishable).
Labels now stable across runs AND identical across all 3 paths (3 goldens byte-identical; 3
consecutive suite runs green; movie labeled output identical across runs). Movie 15.4-16.5s
(+~1.5s for radius). radii ~1.03-1.10 on movies.
P18 DONE — `ac901f4a18` abort polling: failIfInterrupted per frame in collectPool/streamVectors.
Cancel mid-init-round → job dead <1s, client gets HYR0025, instance healthy after. NOTE: correct
cancel API = DELETE /admin/requests/running?request_id=<uuid> (QUERY PARAM, not path!) — earlier
"cancels ignored" observations were malformed calls (path-style uuid → silent BAD_REQUEST).
P19 DONE (2026-07-12) — `391758937e` DEFAULT FLIPPED: cluster_by_runtime_init defaults TRUE; plain
CLUSTER BY = operator tower (movie 15.6s, no hint). SET "false" = pure-desugar REFERENCE path
(executable spec + escape hatch), pinned in CI by new kmeans-syntax-reference exec case +
kmeans-syntax-reference-validate plan golden. Fixture RESULTS unchanged (paths byte-identical since
P17); only the 2 no-hint plan goldens flipped to tower shape. USER-AGREED ENDGAME: delete the flag
+ desugared init/weighting/Lloyd blocks entirely (one path) after review+soak+M5.
P20 DONE (2026-07-12) — `c062ea3d17` initMode WITH option: "kmeansPP" (default, full workflow) |
"random" (C0 = k lex-smallest vectors — deterministic; no init tower; shares Lloyd/labeling/radius
on BOTH paths). Movie: random ~11s vs kmeansPP ~15.5s; fixture results byte-identical between
modes (shared golden); negative test unknown-init-mode. 17 exec cases now.
FORMAL GERRIT SERIES PLAN (user-directed, THREE patches):
  G1 = grammar (already Gerrit PS6, untouched).
  G2 = end-to-end CLUSTER BY with initMode=random ONLY (WITH validation incl initMode, first-k C0,
       desugared Lloyd, labeling, members, cluster_radius, deterministic labels — the path-agnostic
       correctness work folded in). Complete reviewable feature w/ naive seeding.
  G3 = kmeansPP: k-means|| init (desugar reference + runtime operator tower + flag default-true) —
       user chose 3 patches not 4 (algorithm+runtime together in G3). Full-mode name: kmeansPP.
  Re-slicing = NEW branch, code-level re-slice of P2..P20 (not just squashes: G2 needs desugar
  WITHOUT kmeans|| paths). Each G-patch must build + pass its own tests. Present series to user
  BEFORE any push (per-push approval rule!).
Stack: P1(Gerrit PS6) P2..P20 local on clean `clusterby` (branch in analytics/asterixdb repo).

*** INCIDENT 2026-07-13 (RESOLVED, zero committed loss): the /private/tmp scratch WORKTREE
(asterix-ci-repro) was reaped by external tmp-cleanup mid-G2-extraction — .git pointer + poms gone.
PANIC was false: scratch was a git WORKTREE of /Users/hongyu.shi/Proj/workspace/analytics/asterixdb,
so ALL commits P1-P20 live safe in that repo's object store (branch `clusterby` = P20 tip
c062ea3d17). Only the UNCOMMITTED G2 strip was lost. LESSON: never work in /private/tmp scratch for
anything that must persist — use a worktree under $HOME. Recovery salvage was at
~/clusterby-recovery-2026-07-13 (deleted after G2 committed).

FORMAL GERRIT SERIES BUILT (2026-07-13) on branch `clusterby-gerrit`, worktree at
~/clusterby-gerrit-wt (DURABLE, under $HOME). REBUILT on new G1 PS7, then distanceFunction review
item (7) implemented and MOVED INTO G1 + G2/G3 rebased (2026-07-13). CURRENT hashes:
  G1 = 20dfee2acc — grammar, desugar, EXPLAIN. Change-Id I250cf1a... (was cbc6314320 before the
       distance fix moved in). Bundles random-init desugar + EXPLAIN tests (compile-only) + negatives.
       *** distanceFunction FIX lives HERE (review item 7, on G1's own diff — user chose G1 over G2 so
       the change that raised the comment resolves it, and CI builds G1 self-consistently). Replaced
       ad-hoc {euclidean,cosine,dot} Set with validation against the MERGED VectorDistanceMetric vocab
       (asterix-lang-common; 3676 now on master w/ euclidean/cosine/dot-distance builtins +
       VECTOR_DISTANCE(a,b,metric)). Accepts ONLY the Euclidean family (SUPPORTED_DISTANCE_BUILTINS =
       EUCLIDEAN_DISTANCE + EUCLIDEAN_SQUARED_DISTANCE .getName()); L2/EUCLIDEAN/L2_SQUARED/
       EUCLIDEAN_SQUARED pass, COSINE/DOT + typos REJECTED ("CLUSTER BY distanceFunction 'x' is not
       supported. Only Euclidean-family metrics are supported: L2, EUCLIDEAN, L2_SQUARED,
       EUCLIDEAN_SQUARED."), closing the silent-Euclidean-fallback. Tests: unknown-distance msg updated
       + NEW unsupported-distance(cosine) negative. G1 STANDALONE TESTED GREEN (own hyracks, no KMeans
       op): 4/4 opt validates, 10 negatives incl unsupported-distance. RATIONALE (user-agreed): k-means
       arithmetic-mean centroid update only minimizes squared-Euclidean; cosine needs normalized-mean
       (spherical k-means) update, dot needs norm constraint — supporting them = separate feature across
       5 sites (oversample score, recluster, Lloyd, labeling, nearest_centroid evals) + tests, NOT a
       validation tweak. Spherical k-means = tracked follow-up.
  G2 = 8f6d065966 — distributed k-means runtime with random init (executable end-to-end). Change-Id
       I9b0c643... = runtime CENTROID/nearest_centroid evaluators + initMode(random) + lex-min C0 +
       real cluster_radius + deterministic labels + inliner exclusion. Distance fix NOT in G2's diff
       (inherited from G1; only a context line references SUPPORTED_DISTANCE_BUILTINS).
  G3 = 1306687f37 — k-means|| init + runtime operator tower + flag default-true + kmeansPP default.
       Change-Id I287f30b... FINAL TREE byte-identical to prior fixed-G3 139983bec0 (same edits, just
       redistributed across commits) — that tree passed 6/6 optimizer + full cluster-by exec.
       COMMENTS CLEANED (kept kmeans||/Bernoulli/psi/VLDB). Rebase conflict each pass = trivial
       (G1 SUPPORTED_DISTANCE_BUILTINS already present; only G2's INIT_MODE_RANDOM block to keep).
  Overlay recipe used for G2/G3 rebuild: git checkout <old-Gx> -- asterixdb hyracks-fullstack, then
  git checkout cbc6314320 -- <9 shared lang-sqlpp files>, then rename hasMemberFieldList→hasCluster..
  + finder fix (clusterByBlockOf scans all set-op inputs) via Edit, then full offline build + tests.
  INTEGRITY: G1+G2+G3 == P20 functionally. Review rule: self-contained comments, no G1/G2/G3/Phase/
  Px/Mx/follow-up-patch/v1/design-doc/internal-dev refs. k-means||/Bernoulli/psi = legit algo names.
  FALSE-POSITIVE grep hits (leave): v0/v1 = vector components in OperatorTest; v1/v2 = param names in
  SubstituteVariableVisitor.

GERRIT REVIEW OF G1 (change #21384, asterix-gerrit.ics.uci.edu via SSH, was PS6): reviewer Shahrzad
Shirazi left 13 inline comments (2026-07-10); CI all-green. ALL 13 ADDRESSED in G1 PS7 (2026-07-13,
amended → `cbc6314320`, Change-Id I250cf1a preserved, ISSUE# corrected 3783→3785):
  - Visitor-family: ClusterbyClause was dropped by SelectBlock visitors → added clusterByClause
    handling to DeepCopyVisitor, SqlppCloneAndSubstituteVariablesVisitor, SqlppAstPrintVisitor,
    SqlppFormatPrintVisitor, FreeVariableVisitor; SelectBlock.equals() was missing clusterByClause.
  - Set-op finder (clusterByBlockOf) now scans ALL inputs (left+right) so a CLUSTER BY in a right
    UNION branch is caught by the set-op guard, not silently ignored.
  - WHERE-in-block forced a DESIGN CHANGE: the APE 33 example (kmeans-syntax-full-validate) USES
    WHERE, so rejecting breaks it. G1 now SUPPORTS WHERE (copies block-WHERE into __vecs,
    substituted fromVar→v0; 8-arg selectValueFrom + binaryOp ported from G2), REJECTS LET (new
    let-in-block negative test replacing where-in-block; syntax-full-validate golden regen'd).
  - Rename memberFieldList→clusterFieldList across ClusterbyClause, DeepCopy, CloneSubstitute,
    SqlppClusterByVisitor, AbstractSqlppExpressionScopingVisitor, SQLPP.jj.
  - ClusterbyClause: clusterFieldList inits to empty ArrayList + null-guarded ctor/setter (mirrors
    GroupbyClause); fixed confusing "(CLUSTER AS rvc)" comment.
  - distanceFunction (item 7): FIXED IN G1 (user moved it there 2026-07-13 so G1 self-resolves its own
    inline comment; first put it in G2 then relocated). G1 now validates via VectorDistanceMetric,
    Euclidean-only, rejects cosine/dot + typos (see G1 entry above). Gerrit reply on item 7 points to
    G1's own new patchset. No silent-fallback window.
  - centroid/nearest_centroid: DECISION = KEEP PUBLIC (user-facing vector primitives; private breaks
    kmeans-validate/full-validate template tests that call them directly). Item 13 = no code change.
  Green: 4/4 optimizer, 9/9 negatives. Also cleaned G1's own v1/follow-up comment wording.
  *** NOT PUSHED — awaiting per-push approval. Per-comment reply DRAFTS for all 13 done (2026-07-13);
  items 7 & 13 are the two rationale replies (7 = fix in G2, VectorDistanceMetric/Euclidean-only;
  13 = keep centroid/nearest_centroid public, distributed step fns already private, evals validate).
  *** SERIES CURRENT (2026-07-13, item 7 fix in G1): G1 20dfee2acc, G2 8f6d065966, G3 1306687f37
  (all green; see FORMAL GERRIT SERIES section). Branch clusterby-gerrit tip = 1306687f37, clean tree,
  all on 3785, all Change-Ids preserved. Superseded hashes (recoverable via reflog): G1 cbc6314320,
  G2 bfbd53134c/884dcca92d/2fde0229da, G3 139983bec0/01b542e9e8/08294b498e.
  *** G1 PUSHED to Gerrit 2026-07-13 (`git push gerrit 20dfee2acc:refs/for/master`, user-approved):
  new patchset on change #21384 (I250cf1a), SUCCESS. Prior CI +1s (Contrib/Integration/Verified) auto-
  removed → Jenkins re-runs. Push warnings (non-blocking): subject >50 chars, body lines >72. G2/G3
  still LOCAL — push each only on explicit per-push approval. Item-7 reply (points to this G1 PS) +
  other 12 replies still to be posted on Gerrit.
  BUILD NOTE: fresh worktree needs `mvn install -DskipTests -Dmaven.gitcommitid.skip=true` (worktree
  .git-as-file breaks git-commit-id-plugin) once to populate m2head, then tests run with `-o` (offline,
  inline -D flags — NOT via a shell var, quoting breaks) to dodge cached BOM-resolution failure.
AFTER series review/push: M5 Bernoulli; shared vectors materialization; flag deletion (post-soak); docs. Backlog: global top-l reduce; shared vectors
materialization (scan is REPLICATE-shared but each of the 5 instances stores its own run file);
real cluster_radius; distance-fn routing.
M1b-2 HISTORY (resolved): FIXED so far, all uncommitted
in worktree: (1) Score never scheduled → restructured to 3 activities (StoreVectors sink/input0,
StorePool sink/input1, Score SOURCE after 2 blocking edges — input connectors across blocking
edges are never delivered!); (2) Score read field 0 blindly → column indices resolved at jobgen;
(3) recorded vars invisible to substitution (plain fields, acceptExpressionTransform=false) →
renames drifted → anchor-ASSIGN per branch in translateStreamBranch + POSITIONAL single-column
resolution (resolveSingleColumn helper). NOW: operator RUNS CORRECTLY (all partitions: pool=1,
scanned total 6, kept 5 = 6 minus seed — right!). OPEN: downstream NPE "ATypeTag.ordinal typeTag
null" = garbage tag byte read by a consumer of the emitted candidates (or later round). Server
stack NOT in surefire log — next: get NC-side stack (embedded cluster logs / response stack), or
inspect emitted byte layout (Candidate.field copies of tagged field bytes; FrameTupleAppender
fieldEndOffsets pattern). Debug prints (KMINIT-DBG) still in code — STRIP before commit.
M1b-2 EARLIER NOTES: the flagged path COMPILES AND EXECUTES (jobgen fine; optimizer flattened the
subplan, CSE'd branches into REPLICATE). Fails at runtime: "ASX0002 list-accessor expects
multiset/array, actual input type is null" — a NULL where the candidate list (or derived pool)
should be. Scratch test kmeans-syntax-runtime (uncommitted, worktree; = kmeans-syntax + SET
cluster_by_runtime_init true; golden shared with kmeans-syntax). NEXT: EXPLAIN the exec-query
shape (with CLUSTER AS members) under the flag to trace where the listified candVar flows;
suspects: (a) Score emits 0/NULL tuples in executed layout (RecordDescriptor/serializer of
ANY-typed candidateVar vs my raw byte[] field copies — CHECK: test used ByteArraySerializerDeserializer
but real jobs use AObjectSerDe — the emitted field bytes ARE tagged ADM lists so likely ok),
(b) listify var lost in subplan flattening, (c) the LIMIT-1 seed branch broadcast produced
nothing at Score before close (ordering of input-1 open/close vs blocking edge on input-0!!
— Score consumes pool in nextFrame; if activity scheduling delivers input1 AFTER Store's blocking
edge but Score.close before?? verify activity/input mapping: Score = activity consuming BOTH
input1 frames and emitting in close — the M1 unit test drove it manually; in a REAL job the
Score activity's input is input index 1 via addSourceEdge(1, score, 0) ✓). After fix: candidates
must equal desugar round-1 (shared golden). Then M2 = 5 rounds via 5 pairs + movie gate.
M1 STUDY DONE (2026-07-09): MaterializerTaskState has BUILT-IN numConsumers (ctor arg; file deleted
when count hits 0) + joblet workspace GC safety net; fresh RunFileReader per read. Template =
hyracks dataflow-std misc/MaterializingOperatorDescriptor (Store sink activity: state.open/append/
close + ctx.setStateObject keyed by TaskId(ActivityId(odId, N), partition); Reader source activity:
ctx.getStateObject + blocking edge). M1 REALIZATION DECISION: single KMeansInitCandidatesOperatorDescriptor
with TWO activities (Store<-input0 vectors; Score<-input1 broadcast pool, ->output candidates,
blocking edge) — TaskId keying suffices, no UUID class until M2 (cross-operator multi-round).
Score internals: createReader() re-stream + VectorListDecoder + top-l heap (deterministic rule).
Next: write the descriptor in asterix-runtime .../runtime/operators/, compile on clusterby-dev
(~/.m2-clusterby-dev split repo), then translator/flag wiring, then round-1 equivalence test.
M1 APPROVED by user (2026-07-09): implement per RUNTIME_INIT_OPERATOR_DESIGN.md **v2**
(shared-materialization + GLOBAL rounds — user rejected v1 local-rounds; dataflow must stay
map+reduce per round, one global seed). M1 = KMeansInitStoreOperator (run file + job-scoped
PartitionedUUID task state) + ONE KMeansInitScoreOperator round wired behind a session flag,
on clusterby-dev ONLY, local commits, no push. Gate M2 = bit-identical to desugar path.
Bernoulli = M5 deferred. (Superseded note below:)
R2 design doc done: init runtime operator — HKPP two-activity + MaterializerTaskState
re-stream pattern, fed by a 3702 sample, hybrid global sync (operator does local passes, existing
exchanges do global merges). Present for user review BEFORE implementing.
- P3 `55a6f203e7` "k-means|| initialization (round 1)" (Change-Id I45c50dd1...) — local only.
  Seed = first point; one oversampling round = top-2k by d2 (local topK + merge = the map+reduce);
  C0 = first-k of seed+cands+pad. psi intentionally dropped (top-l needs no normalization; returns
  with Bernoulli). Follow-ups in PHASE2_DESIGN.md "k-means|| initialization" section: rounds 2-5
  (need distance-to-set fn), psi+Bernoulli, steps 6-7 weighting/recluster, materialize/sample init
  input (user: needed eventually). Goldens regenerated; exec tests pass unchanged.
- Test-filter gotchas: optimizerts `only.txt` wants full relative paths; SqlppExecutionTest reads
  `only_sqlpp.xml` (NOT `only.xml`); restore both files before committing (only.txt is RAT-checked).

**Dependency status:** ASTERIXDB-3676 vector distance = MERGED to master — no longer an in-flight dep.
ASTERIXDB-3702 LSM sampling (change 20959) still in-flight; only needed for future phases, not P1/P2.
The old `clusterby-dev` two-branch model is stale (still holds pre-repack retire-operator history);
rebuild from new `clusterby` via build-integration.sh --force only if 3702-dependent work resumes.
Scripts in `asterixdb/doc-clusterby/design/distributed-kmeans-design/`.

**P2 API adaptation (2026-07-08):** merged 3676 differs from the pinned PS31 it was written against —
`VectorListDecoder` is now no-arg ctor with `createArrayFromList(ListAccessor, double[])` (numeric→double,
non-numeric→NaN; no `resolveArgType`), and the kernel is `VectorDistanceCalculation.euclideanSquared`
(`VectorDistanceMetric` enum is gone). NearestCentroidScalarEvaluator adapted; NaN distance → warn+NULL.

**Gerrit CI gotchas (learned from 21384 PS3 total failure):**
- Jenkins builds the change ON ITS PARENT, not rebased-on-master; `ensure-ancestor` only checks lineage.
  A stale base + code written against newer-master APIs = every asterixdb-compiling job fails while
  hyracks-only jobs pass. Fix by rebasing the change onto master, not by pinning to old symbols
  (upstream master renamed AiProvenance `Tool.CLAUDE_UI` → `Tool.CLAUDE_CODE_UI`; keep CLAUDE_CODE_UI).
- Never trust `mvn -o` validation for upstream-bound patches: stale ~/.m2 hyracks artifacts (built from
  a dep branch) mask missing/renamed symbols. Build hyracks from the patch tree first (use
  `-Dmaven.repo.local=<scratch> -Dmaven.repo.local.tail=~/.m2/repository` to keep ~/.m2 intact).
- Gerrit rejects pushes whose committer email isn't registered: re-stamp with
  `-c user.email=leoshy1005@gmail.com` after any rebase/amend (rebase re-stamps committer from repo config).
- git-commit-id-plugin (JGit) fails in linked worktrees — build with `-Dmaven.gitcommitid.skip=true`.

**Local test filtering (asterix-app):**
- optimizerts: `src/test/resources/optimizerts/only.txt` (tracked file — restore after!) needs FULL relative
  paths like `cluster-by/kmeans-validate.sqlpp`; a bare directory entry loses the results subdir and errors.
- runtimets `only.xml` did NOT restrict SqlppExecutionTest in practice — it ran the full suite (~32 min).
  Full-suite result on the new stack: 3161/3162 pass; the 1 error (cbo-join/ch2 "path not found") is
  environmental (skipped data-gen phase when invoking surefire:test directly), not a regression.

Validated on the new stack: full compile, ClusterByParserTest, formatter+impsort, both kmeans EXPLAIN
optimizer tests, and all 7 cluster-by runtimets cases end-to-end.
