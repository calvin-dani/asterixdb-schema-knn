---
name: project-clusterby-optimizer-test-coverage
description: "CLUSTER BY next task — add APE-proposal-aligned optimizer golden tests (movie⋈review join, nested unnest, GROUP BY combo); plus how plan-goldens are generated"
metadata: 
  node_type: memory
  type: project
  originSessionId: 6a3ceaba-e2d8-44dc-b6f7-4c8d6b14b6a2
---

Paused CLUSTER BY task (2026-06-25), resume after the vector-index work. Relates to [[project-clusterby-architecture]] and [[bug-clusterby-scoping-traversal]].

**The gap:** The 3 OptimizerTest cases in `optimizerts/queries/cluster-by/` (`cluster-by-descriptor-only`, `cluster-by-with-members`, `cluster-by-listify-members`) all use a single flat `Reviews` dataset with a bare `from Reviews r cluster by r.reviewEmbedding`. They deliberately isolate one axis — the **output shape of the CLUSTER BY clause** (descriptor-only → no LISTIFY; CLUSTER AS + array_count; CLUSTER AS + listify rvc). The **APE proposal** (`doc-clusterby/bootstrap/ORIGINAL_APE_PROPOSAL.md`, "APE 33: CLUSTER BY") drives all canonical examples off `movies ⋈ movie_reviews` and has NO test mirroring them.

**Next task — add proposal-aligned optimizer goldens (increasing surface area):**
1. Design #1 join: `FROM movies m, movie_reviews r WHERE r.movie_id=m.movie_id AND m.movie_year<2020 CLUSTER BY r.reviewEmbedding AS sc CLUSTER AS rvc` — closest to the proposal's hero query. Drafting this also tells us whether join-before-CLUSTER-BY compiles in the current rewrite or surfaces another scoping gap (like the first plan-golden did).
2. Design #3 unnest: `FROM movies m, m.reviews re ... CLUSTER BY re.reviewEmbedding`.
3. GROUP BY + CLUSTER BY combination (subquery-unnest example).
Deferred originally because each pulls in orthogonal optimizer surface (join order, selection pushdown, unnest) that makes goldens brittle, and the rewrite is still young.

**How to generate/verify a plan golden (OptimizerTest, compile-only):**
- `OptimizerTest` reads each `optimizerts/queries/**/*.sqlpp` as ONE string, compiles all statements (DDL registers metadata; last stmt's optimized logical plan is emitted), writes actual to `target/opttest/<sub>/<name>.plan`, diffs vs expected `optimizerts/results/<sub>/<name>.plan`. PASS deletes actual; FAIL leaves it.
- Run from `asterixdb/asterix-app`: `mvn -o test-compile surefire:test -Dtest=OptimizerTest`. Scope to one case via `src/test/resources/optimizerts/only.txt` (add e.g. `cluster-by/cluster-by-descriptor-only.sqlpp`); revert before commit.
- To (re)create golden: blank the expected file → test fails → `cp target/opttest/.../X.plan src/test/resources/optimizerts/results/.../X.plan`.

**Why no live-engine (APE) / runtimets execution or EXPLAIN test yet:** CLUSTER BY runtime is Phase 2 — `ClusterAssignPOperator.contributeRuntimeOperator` throws `"CLUSTER BY runtime is not implemented yet (Phase 2)"`. Live paths (HTTP, compile-only, EXPLAIN) keep `generateJobSpec=true` (QueryServiceServlet sets only `setExecuteQuery`, never `setGenerateJobSpec(false)`), so `APIFramework.compileQuery` always reaches `compiler.createJob()` (line 394) → throws. The optimized plan IS computed at line 343/352 before the throw but is never returned. OptimizerTest works ONLY because `AsterixJavaClient.compile(...,generateBinaryRuntime=false,...)` sets `generateJobSpec=false`, short-circuiting at `APIFramework:379-383` before createJob. To enable live plan inspection pre-Phase-2: make explain/compile-only set `generateJobSpec=false`. runtimets cluster-by cases today are negative-only (`runtimets/queries_sqlpp/cluster-by/negative/...`).
