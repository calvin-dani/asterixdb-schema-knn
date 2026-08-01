---
name: analytics-project-switching
description: Routine for switching between the CLUSTER BY and VTree projects in the analytics workspace
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 7215a24d-5931-4f2d-a7f7-01a58dabdad1
---

Two projects share the analytics workspace repos (analytics/asterixdb, analytics/cbas) + ~/.m2.
See [[clusterby-branch-model]] and [[vtree-branch-model]].

**⚠️ VTREE DEV BRANCH (confirmed 2026-07-27) — the right branch to develop on is `vtree-fold`.**
Its patch commits are byte-identical to what is LIVE on Gerrit: p1=`df787da994` (21099),
p2=`f5726536e6` (21100), p3=`f4a691ba5a` (21101), then 3760'=`ff3ff616f0`, 3771'=`fe1fc3ee46`, spann,
docs, +round-2-docs `6377d6b628`; base = merged `3d6992d0e7`. Verified by matching each Gerrit
`current_revision` (`curl --netrc https://asterix-gerrit.ics.uci.edu/a/changes/2109{9,0,1}?o=CURRENT_REVISION`)
against `git branch --contains`. Re-derive the same way if SHAs move.
- `vtree-3754-p1-review-fixes` = the SAME fixes as 7 discrete batch commits (batches 1–5b + comment
  cleanup) on top of `vtree-split`; same source tree as `vtree-fold` (only the 2 round-2 doc files differ).
  Use it to see/cherry-pick individual batches; use `vtree-fold` as the Gerrit-aligned base for new work.
- **DEPRECATED / STALE, do NOT dev on:** `vtree-integ` (deprecated), `vtree-split` (p1 still the pre-fix
  `9fa222a5c9`), `vtree` / `vtree-migration` (predate the fold).
- `~/switch-analytics-project.sh vtree` STILL lands on the dead `vtree-integ` — after the switch, manually
  `git switch vtree-fold`. (Fix the script's `vtree` mapping to `vtree-fold`, and/or rename `vtree-fold`
  to a clearer name like `vtree-gerrit`, once the user decides — not yet done.) Switching to `vtree-fold`
  also restores the round-2 review docs (see [[vtree-review-fixes-patchset-mapping]]).

**CHOSEN WORKFLOW (user decided 2026-07-15): SINGLE-FOLDER + rebuild-per-switch.** The user found the
multi-worktree "cd between folders" model confusing and switches projects infrequently, so they prefer the
traditional one-directory model: work in the MAIN superproject `~/Proj/workspace/analytics`, `git switch`
between branches there (asterixdb=`clusterby`/`clusterby-gerrit` ↔ `vtree-integ`; cbas=`cbas-master` ↔ `vtree`),
and rebuild each switch. Accepts the ~15-min hyracks-first rebuild as the cost. Rebuild recompiles hyracks +
asterixdb (+ cbas/`make install` for vtree) into ~/.m2 because git swaps SOURCE while target/ + ~/.m2 keep the
old branch's compiled jars. NOTE: pushing to Gerrit / posting review replies need NO rebuild (git-only ops);
only building/running tests does. **AUTOMATION SCRIPTS (created 2026-07-15, syntax-checked + tested):**
- `~/switch-analytics-project.sh <clusterby|clusterby-p20|vtree> [--build]` — switches the main superproject's
  branch set (asterixdb+cbas). Refuses on a dirty tree (commit first). `clusterby`→clusterby-gerrit+cbas-master,
  `clusterby-p20`→clusterby+cbas-master, `vtree`→vtree-integ+cbas `vtree`. `--build` rebuilds into ~/.m2
  (clusterby: hyracks-first mvn clean install; vtree: `make -j4 install`). WITHOUT --build it only switches
  branches (fast) — correct for Gerrit push / review replies which need no build.
- `~/vtree-cluster.sh start|stop|status` — controls the VTree cloud cluster (CloudClusterIntegrationUtil).
  start = launch detached + wait for 9600; stop = graceful SIGTERM (deinit) + sweep orphaned beam.smp/memcached
  under $WORKSPACE/install + docker rm the couchbase/s3mock/ryuk containers (the cleanup that the manual
  restart needed); status = up/down + pids + containers. Requires the vtree profile built + Docker.

(The worktree model below is the DEPRECATED alternative, kept for reference.)

**CONSOLIDATED to single-folder 2026-07-15:** removed the extra worktrees `~/clusterby-gerrit-wt` and
`~/vtree-wt` (branches survive in the repo; only rebuildable compiled output discarded). Now ONE worktree:
`~/Proj/workspace/analytics/asterixdb` (currently `vtree-integ`). All work happens here via `git switch`.
Branches available: asterixdb — `clusterby-gerrit` (G1/G2/G3 series), `clusterby` (P20), `vtree-integ`
(vtree on couchbase/master), `vtree` (asterix Gerrit chain), `couchbase-master`, `vtree-migration`,
`clusterby-dev`; cbas — `cbas-master`, `vtree` (gate patch). To work CLUSTER BY: `git switch clusterby-gerrit`
in the main asterixdb folder (rebuild only if building/running tests; push & review-replies need none). The
clusterby-gerrit `m2head` isolation is gone — building the Gerrit series now uses the real ~/.m2 (standard;
just re-stamp committer email before Gerrit push per [[clusterby-branch-model]]).

**TWO HOMES — switch by `cd`, usually no rebuild:**
- **VTree = the MAIN superproject** `~/Proj/workspace/analytics` — needs the full stack (asterixdb couchbase/master
  + cbas + ~/.m2 + $WORKSPACE/install + KV/CloudClusterIntegrationUtil). Profile: asterixdb=`vtree-integ`,
  cbas=`vtree` (the gate patch, committed on cbas branch `vtree` = 220041b28). ~/.m2 = couchbase-master+VTree.
  Leave the main superproject sitting on this profile as vtree's home. Work vtree / run the cloud cluster here.
- **CLUSTER BY (Gerrit series) = the ISOLATED worktree** `~/clusterby-gerrit-wt` (branch clusterby-gerrit, own
  `m2head` split maven repo). Fully isolated — never touches ~/.m2 or the main checkout. Work the G1/G2/G3
  series here. **To switch to clusterby: just `cd ~/clusterby-gerrit-wt`** — already built, no rebuild, no branch
  churn. To switch to vtree: `cd ~/Proj/workspace/analytics` (already on the vtree profile; if ~/.m2 got
  disturbed, `cd analytics && make -j4 install` to restore).

**PRESERVATION RULE (why switching is lossless):** each project's state lives on committed branches, never in a
dirty working tree. vtree cbas patch → cbas branch `vtree`. vtree asterix → `vtree-integ`. clusterby → `clusterby`
(P20) / `clusterby-gerrit`. So switching = checkout, never discard. Commit WIP to its branch before switching.

**EDGE CASE — clusterby P20 dev IN the main superproject** (collides with vtree in ~/.m2, needs rebuild):
use `~/switch-analytics-project.sh vtree|clusterby`. It refuses if the tree is dirty (commit first), switches both
repos' branches, and rebuilds: clusterby → hyracks-first `mvn clean install` (Apache-master asterix, asterix-only);
vtree → `make -j4 install` (asterix+cbas → ~/.m2 + $WORKSPACE/install). Prefer the isolated-worktree model above
over this flip whenever the clusterby work is the Gerrit series.

**STALE JAVACC PARSER ON SWITCH (added 2026-07-16):** a branch switch does NOT regenerate the JavaCC
parsers, so `SQLPPParser`/`CBSQLPPParser` under `target/generated-sources` keep the PREVIOUS profile's
grammar and fail to compile against the new branch — clusterby→vtree gives `cannot find symbol
ClusterbyClause`; vtree→clusterby gives `cannot find symbol VTREE` in `DatasetConfig.IndexType`. An
incremental/IDE build reuses the stale generated file (only a clean regenerates). `~/switch-analytics-project.sh`
now **wipes `asterix-lang-sqlpp/target/generated-sources` and `cbas-connector/target/generated-sources` on
every switch** so the next build / IDE Maven reimport regenerates them. After a no-`--build` switch: reimport
Maven in the IDE before running. Manual fix if hit: `rm -rf` those two dirs, then `mvn -pl asterix-lang-sqlpp
generate-sources` (and same for cbas-connector). Running `CloudClusterIntegrationUtil` (vtree) still needs
`--build` regardless — it boots the columnar cluster via `cluster_run` from `$WORKSPACE/install`, which a bare
switch leaves stale.

**STALE VTREE MODULE ORPHANS ON vtree→clusterby SWITCH (added 2026-07-19):** switching vtree-integ→
clusterby-gerrit leaves behind orphaned VTree module directories that exist only as gitignored `target/`
build output (no src/pom on clusterby): `hyracks-fullstack/hyracks/hyracks-storage-am-vtree`,
`.../hyracks-storage-am-lsm-vtree`, `.../hyracks-tests/hyracks-storage-am-lsm-vtree-test`. `git switch`
can't remove them (target/ is gitignored, dir untracked) and `mvn clean` skips them (not in the clusterby
reactor), so the RAT license check walks their stray target files and fails: `Too many files with unapproved
license: 22 ... project hyracks`. FIX: `rm -rf` the three orphan dirs before `--build`. Detect any orphan
generically: for each `target/` dir, if `git ls-files <moduleDir>` is empty, the module is an untracked
orphan — delete it. (`--build`'s clean won't; the switch script does not yet auto-purge these.)

**GOTCHAS:** (1) ~/.m2 holds ONE asterix lineage at a time (Apache-master for clusterby vs couchbase/master+VTree
for vtree) — flipping the main worktree needs the hyracks-first rebuild. (2) The Gerrit worktree's m2head sidesteps
this entirely. (3) To stop the vtree cloud cluster cleanly: SIGTERM the exec:java launcher (shutdown hook = deinit),
never hard-kill (orphans beam.smp/memcached + the Docker KV container). (4) cbas `vtree` branch commit is a local
spike (uncommitted-to-remote); AiProvenance/formatter may reflow test files on build — ignore that noise.
