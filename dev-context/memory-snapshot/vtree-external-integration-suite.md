---
name: vtree-external-integration-suite
description: "The VTree \"external integration test suite\" — live-cluster movie/glove harness, distinct from the in-JVM SqlppExecutionTest golden runtimets"
metadata: 
  node_type: memory
  type: project
  originSessionId: 7215a24d-5931-4f2d-a7f7-01a58dabdad1
---

The VTree **external integration test suite** lives at `asterixdb/integration/` (in the analytics repo). It is
distinct from the in-JVM golden/runtimets suite (`SqlppExecutionTest`, the `vector` group in
`asterix-app/.../runtimets/queries_sqlpp/vector/VectorQueries.xml`). When the user says "external integration
test suite," they mean THIS one, not the JUnit runtimets.

- **Driver (current):** `integration/scripts/movie_test.py`. NB: `INTEGRATION_TEST_GUIDE.md` /
  `integration_test.sh` still reference an older `movie_index_test.py` name that no longer exists — the real
  script is `movie_test.py`. It has subcommands (create index, drop, load, insert, delete, query/verify).
- **Target:** a **remote** AsterixDB cluster over an **SSH tunnel** at `http://localhost:29002/query/service`
  (`/admin/cluster/summary` for health). NOT a local 19002 cluster. So it needs the build-under-test **deployed
  to that remote cluster** and the **SSH tunnel up** before it can run — both user-driven (I can't deploy
  remotely or open the tunnel).
- **Datasets are git-ignored** (`integration/.gitignore` ignores `datasets/`) and live OUTSIDE the repo at
  `/Users/hongyu.shi/Projects/asterixdb-schema-knn/integration/datasets/`:
  `movie_embeddings_384d.json` (85 MB, 384-dim), `glove-100-angular_train.jsonl` (+`.limited_50000`),
  `movie_filtered_indexed.jsonl`.
- **What it exercises:** real DDL → index build → insert/delete/query verification across metrics
  (euclidean, euclidean-squared, cosine) and quantization (SQ7/SQ8) on the movie/glove data — i.e. the full
  end-to-end path a metric/factory refactor touches. See [[clusterby-runtime-dataflow]] for the movie-scale
  context and [[dont-mutate-cluster-state]] (this suite creates/drops its OWN scratch dataset+index, which is
  its intended use, but confirm the target is a throwaway test cluster, not the user's working cluster).

**The real suite is `integration/tests/`** (NOT `scripts/`, which are ad-hoc benchmark helpers). Driver:
`integration/tests/run_tests.py`, 4 parts (`test_part1_index_creation` .. `part4_flush_compact`), config
`config.yaml` / `config_no_gist.yaml` (use the no-gist one — the `gist-960-euclidean` dataset isn't
present). Config `url` = **localhost:19002** (local cluster). Data loads via `localfs` (cluster reads the
file), so the datasets must be reachable at the config's `datasets_dir` (default `../datasets`, git-ignored).

**Working run recipe (validated 2026-07-28, all green):**
1. Build the refactor: `mvn -pl asterix-app -am install -DskipTests`, then repackage the server:
   `mvn -pl asterix-server install -DskipTests` (asterix-server is a *dependent*, so `-am` up to asterix-app
   does NOT rebuild it — you must rebuild it explicitly, else the assembly is stale).
2. Point the suite at the real datasets: `ln -s ~/Projects/asterixdb-schema-knn/integration/datasets
   integration/datasets` (the symlink shows as untracked — `datasets/` ignore rule doesn't match a
   symlink-to-dir — so `rm` it when done to keep the tree clean).
3. `cd integration/tests && JAVA_HOME=<jdk21> python3 run_tests.py --config config_no_gist.yaml`
   — **lifecycle-managed** (no `--skip-lifecycle`): it starts its OWN 2-NC cluster from the built
   `target/classes` via `AsterixHyracksIntegrationUtil` + `cc-main.conf`, so it runs your latest code, and
   **Part 4's `restart_Movie` sub-test actually runs** (it kills+restarts the cluster and re-queries —
   this is the ONLY test that validates reload-from-disk / persisted-format changes; `--skip-lifecycle`
   FAILs it with "Lifecycle manager not available", which is a harness artifact, not a defect).
4. Expected: ~14 tests, 0 FAIL. The 3 Part-2 recall WARNs (movie_esq_filter/glove_cosine/movie_esq_no_include,
   ~62-70%) are normal recall variance (above the 50% fail threshold) — see [[vtree-integration-recall-variance]],
   NOT a regression. A broken metric would collapse recall, not leave it in-band.

SQL++ vector index syntax (this build): `CREATE INDEX <n> ON <ds>(<field> VECTOR) TYPE VTREE WITH
{"dimension":D,"similarity":"cosine|euclidean|euclidean_squared|l2_squared|dot","num_clusters":N,
"train_list_fraction":F} EXCLUDE UNKNOWN KEY;` — NOT `CREATE VECTOR INDEX`, and needs `ANALYZE DATASET` first.
Query: `ORDER BY ann_distance(field, <constant-literal-vector>, "<metric>", <min_probe_fraction>) LIMIT k`
(query vector must be an inlined literal to hit the index — see [[vtree-ann-query-conditions]]).
