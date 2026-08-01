---
name: build-main-worktree
description: How to rebuild the asterixdb reactor in the MAIN worktree for running (not the Gerrit worktree)
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 7215a24d-5931-4f2d-a7f7-01a58dabdad1
---

To rebuild AsterixDB in the MAIN worktree (`~/Proj/workspace/analytics/asterixdb`) for running/testing, use
`mvn clean package -DskipTests` **inside the inner `asterixdb/asterixdb/` reactor folder** — NOT
`mvn install` on the combined `analytics/asterixdb` reactor (which also pulls in hyracks-fullstack).

**Why:** the user corrected this (2026-07-14). `package` (not `install`) is enough to run; `clean` gives a
fresh build after a branch switch; and building just the `asterixdb` reactor is the intended scope —
hyracks-fullstack is expected to already be available in `~/.m2` (or built separately / via the IDE).

**How to apply:** `cd ~/Proj/workspace/analytics/asterixdb/asterixdb && mvn clean package -DskipTests`
(add `-Dmaven.gitcommitid.skip=true` — the submodule `.git`-as-file trips the git-commit-id plugin).
Uses the standard `~/.m2`, not the scratch `m2head` (m2head is only for the [[clusterby-branch-model]]
Gerrit worktree at ~/clusterby-gerrit-wt). Layout: `analytics/asterixdb/pom.xml` is a combined reactor
(modules: `hyracks-fullstack`, `asterixdb`); the AsterixDB reactor proper is the inner `asterixdb/` subdir.

**CAVEAT (learned 2026-07-14): install hyracks-fullstack FIRST when switching milestones that changed it.**
The `asterixdb` reactor resolves hyracks from `~/.m2`. Switching the main worktree M2 (`clusterby-m2`) →
G3 (`clusterby-g3-run`) and running only `package` in `asterixdb/` FAILS to compile — `~/.m2` still had M2's
older `KMeansInitCandidatesOperator` (no `Mode` enum / `setPoolFromPriorRound`). Fix, in order:
`cd ~/Proj/workspace/analytics/asterixdb/hyracks-fullstack && mvn clean install -DskipTests
-Dmaven.gitcommitid.skip=true`, THEN the `asterixdb` `clean package`. (Only needed when hyracks source
differs from what's in `~/.m2`; within the same milestone, the asterixdb-only package is fine.)
