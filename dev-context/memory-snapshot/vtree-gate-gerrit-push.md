---
name: vtree-gate-gerrit-push
description: "How to push the cbas VTree gate patch to Couchbase Gerrit (remote, username, topic-build coordination)"
metadata: 
  node_type: memory
  type: project
  originSessionId: 7215a24d-5931-4f2d-a7f7-01a58dabdad1
---

Pushing the cbas VTree gate patch (opens CBStatementExecutor DDL gate for TYPE VTREE) to **Couchbase
Gerrit** (`review.couchbase.org`), part of VTree→Columnar integration. See [[vtree-branch-model]].
Respect [[ask-before-implementing]]: never push without explicit per-push approval.

**CORRECTED 2026-07-29 — see [[vtree-cbas-gate-manifest-bridge]] for the working mechanism.** Two claims
below are now wrong: (1) the same-topic build does NOT push an asterixdb change to Couchbase Gerrit — there
is **no asterixdb project on Couchbase Gerrit**; the bridge is a **manifest** change (project `manifest`)
repointing asterixdb, topic-linked with the cbas gate. (2) The real topic is **`hongyu/vtree`** (already set
on cbas change 249142), not `hongyushi-creator/vtree-gate`. The ACL blocker below is **RESOLVED** (cbas-core
+ manifest now visible/pushable). The gate is now Couchbase Gerrit change **249142** (`MB-72975`).

- **Couchbase Gerrit username:** `hongyushi-creator` (distinct from Apache Gerrit `Le0shy` and the
  clusterby gmail `leoshy1005@gmail.com`). Couchbase Gerrit uses the couchbase email
  `hongyu.shi@couchbase.com` as author/committer — the gmail rule is Apache-only.
- **cbas repo (`analytics/cbas` = project `cbas-core`)**: `gerrit` remote configured =
  `ssh://hongyushi-creator@review.couchbase.org:29418/cbas-core`. The gate commit is on branch `vtree`,
  reworded to `MB-XXXXX: allow VTREE index type on columnar (VTree integration gate)` (Change-Id
  `I15afe711a8ce09ebba445592cf8b02504722f052` preserved; swap MB-XXXXX for the real ticket before push).
- **Two Gerrits, don't confuse:** asterixdb VTree chain (ASTERIXDB-3754/3760/3771) → **Apache** Gerrit
  `asterix-gerrit.ics.uci.edu` (remote `gerrit` in asterixdb points there). cbas → **Couchbase** Gerrit.

**Coordination — get CI to build without merging.** CI builds asterixdb FROM SOURCE at the manifest-
pinned revision (repo project `asterixdb`, rev `morpheus`, built to `0.9.10-SNAPSHOT` that cbas consumes).
`morpheus` has no VTree code, so the cbas gate won't compile alone. Fix = Couchbase Gerrit **same-topic
cross-project build**: push BOTH the cbas gate (cbas-core) AND a VTree asterixdb change
(`ssh://hongyushi-creator@review.couchbase.org:29418/asterixdb`) under the SAME topic; commit-validation
checks out same-topic open changes together and builds them — WITHOUT merging. Nothing lands until submit.
If not exposing the full chain, squash the vtree-integ delta into one `[DO NOT SUBMIT]` build-dep change.

Push (only after MB# + asterixdb topic change exist, and explicit approval):
`cd ~/Proj/workspace/analytics && git push gerrit HEAD:refs/for/master%topic=hongyu/vtree`
SSH auth check (read-only): `ssh -p 29418 hongyushi-creator@review.couchbase.org gerrit version`

**STATUS 2026-07-17 — BLOCKED on Gerrit project access (resume here once granted):**
- SSH key WORKS: `~/.ssh/id_rsa.pub` (RSA 4096, hongyu.shi@couchbase.com) registered on Gerrit; both
  `review.couchbase.org` and `review.couchbase.com` return `gerrit version 3.10.1` and authenticate.
- `.com` vs `.org` is a RED HERRING — same Gerrit (identical project list, same version, key works on both).
  Remote stays `.org`. (`git gerrit init` used `.com`; doesn't matter.)
- **BLOCKER:** `git push gerrit HEAD:refs/for/master%topic=hongyu/vtree` → `fatal: Project not found:
  cbas-core`. `cbas-core` is NOT visible in `gerrit ls-projects` on either host (siblings cbas-ui,
  cbas-perf-support, cbas-thirdparty, cbas-maven-plugins ARE visible; cbas-core, asterixdb are NOT).
  Gerrit says "Project not found" for BOTH nonexistent AND no-read-access — so this is an **ACL/group-
  membership gap**, not a config error. ACTION: request push-for-review (`refs/for/*`) access to the
  `cbas-core` Gerrit group for `hongyushi-creator`, and confirm the exact project name.
- `git gerrit` is NOT installed anywhere on this machine (searched ~/Proj/workspace + ~; no executable,
  no alias, no shell fn) — it's an internal Couchbase helper (`git-gerrit`) that just wraps remote+hook
  setup. NOT NEEDED: the plain `gerrit` remote is already set, the commit-msg hook is present (repo-tool
  symlink `.git/hooks/commit-msg -> ../../../repo/hooks/commit-msg`), and the commit already has a
  Change-Id — so no scp-hook step needed either.
- DONE: SSH ✅, `gerrit` remote ✅ (`ssh://hongyushi-creator@review.couchbase.org:29418/cbas-core`),
  reworded commit `MB-XXXXX: ...` with Change-Id ✅. Only project access + real MB# + paired asterixdb
  topic change remain.

**CONSOLIDATED 2026-07-20 — access blocker triply-confirmed; `git gerrit` is a dead end.**
- The blocker is DEFINITIVELY an ACL/privileges gap for `hongyushi-creator`, not naming/URL/tooling. Three
  independent signals agree: (1) SSH `gerrit ls-projects` → `cbas-core` and `cbas` BOTH absent (only open
  siblings cbas-ui/cbas-perf-support/cbas-thirdparty/cbas-maven-plugins/columnar-ui-v1 visible); (2)
  `git push --dry-run gerrit HEAD:refs/for/master` → `fatal: Project not found: cbas-core`; (3) Gerrit WEB
  UI on a change link → `404 / not enough privileges` (`cbas~248472` — note project=`cbas`, the Go layer,
  also gated). Gerrit returns 404/"not found" for BOTH nonexistent AND no-read-access, on SSH and web alike.
- **README VERIFIED our setup is 100% correct** (`analytics/README.md`): cbas-core repo path = `analytics`
  (repo root `~/Proj/workspace/analytics`; `cbas/` is a SUBDIR, no own .git — git walks up); remote
  `ssh://$USER@review.couchbase.org:29418/cbas-core`; product branch for current EA = `master`; push form
  `git push gerrit HEAD:refs/for/<product branch>`; coordinated changes via `%topic=<user>/my-topic`. Our
  `vtree` branch is EXACTLY ONE commit ahead of the product branch (c3318ec9f gate patch) → a clean
  single-change push, no ancestor drag. Topic convention: use `%topic=hongyushi-creator/vtree-gate`.
- **`git gerrit` is NOT the npm pkg (404) NOR pypi meffie/git-gerrit** (that one uses `git gerrit-checkout` +
  `git config gerrit.host`, no `init -u -p`). The `git gerrit init -u ssh://... -p <project>` / `git gerrit
  push` CLI teammates use is a Couchbase-INTERNAL helper — not publicly installable; get from internal dev
  docs/Slack. NOT NEEDED: plain `git push ... refs/for/master` is the whole workflow (proven via Apache
  dry-run `[new reference] HEAD -> refs/for/master`). Installed node v26.5/npm 11 chasing the npm route — a
  dead end but harmless.
- ACTION unchanged: get `hongyushi-creator` added to the `cbas-core` (and likely `cbas`) access group with
  read + push-for-review (`refs/for/*`), via the analytics Gerrit admin / dev-setup Slack. Then the ready
  push works: `cd ~/Proj/workspace/analytics && git push gerrit HEAD:refs/for/master%topic=hongyushi-creator/vtree-gate`.
- NOTE: local file-memory (this dir) is account-INDEPENDENT (same OS user + project path reads it, any
  claude.ai login). Artifacts ARE account-bound (re-login breaks the artifact URL → must republish).
