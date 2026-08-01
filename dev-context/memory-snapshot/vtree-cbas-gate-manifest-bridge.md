---
name: vtree-cbas-gate-manifest-bridge
description: "How to get the cbas VTREE gate (MB-72975) to compile in Couchbase CV against the unmerged ASF asterix VTree stack, via a manifest topic-bridge"
metadata: 
  node_type: memory
  type: project
  originSessionId: 7215a24d-5931-4f2d-a7f7-01a58dabdad1
---

Goal: get the **cbas VTREE index-DDL gate** to COMPILE in Couchbase commit-validation (CV) against the
**unmerged** ASF AsterixDB VTree stack. Nothing merges — pure pre-merge CV. See [[vtree-gate-gerrit-push]]
(now corrected), [[vtree-branch-model]], [[vtree-review-fixes-patchset-mapping]]. Respect [[ask-before-implementing]].

## The two changes
- **cbas gate** = Couchbase Gerrit change **249142** (`cbas-core`/`master`, `MB-72975: Allow VTREE index type
  on columnar`, Change-Id `I15afe711a8ce09ebba445592cf8b02504722f052`). Edits ONE file
  `cbas/cbas-connector/.../lang/CBStatementExecutor.java`: allow `IndexType.VTREE` in the index-type gate +
  skip the primitive-field-type check for VTREE. Its ONLY asterix compile dependency is the enum constant
  `DatasetConfig.IndexType.VTREE`. Local branch `vtree` (in `analytics/`) amended to match PS4 message
  (`f21098e04`, code byte-identical to Gerrit `7a51b5d7c`).
- **manifest override** = the bridge (below).

## Why a manifest bridge (NOT a Couchbase-Gerrit asterixdb topic)
Couchbase Gerrit hosts **NO asterixdb project** (`ls-projects` shows only cbas, cbas-core,
analytics-dcp-client, java-dcp-client; `asterix-opt` is a dead alias of cbas-core). The Couchbase build gets
asterixdb ONLY from the **github.com/couchbase/asterixdb mirror**, pinned in the active manifest
`.repo/manifests/enterprise-analytics/helios.xml` (includes `base-manifest_helios.xml`) via
`<extend-project name="asterixdb" revision="master" dest-branch="master"/>`. CV can't reach ASF change refs
on its own (its cross-repo trick is `patch_via_gerrit -C` = same Change-Id, scoped to review.couchbase.org).
So the bridge = push a **manifest change** (repointing asterixdb) + the cbas gate under the **same Gerrit
topic**; CV assembles the workspace from the topic's manifest and builds both, merging neither.

## Variant A (APPLIED) — point asterixdb at the ASF change ref
Edit `helios.xml`: add `<remote name="asterix-asf" fetch="https://asterix-gerrit.ics.uci.edu/"
review="asterix-gerrit.ics.uci.edu"/>` and change the asterixdb line to
`<extend-project name="asterixdb" remote="asterix-asf" revision="refs/changes/87/21287/26" dest-branch="master"/>`.
`refs/changes/87/21287/26` = **ASF change #21287 (ASTERIXDB-3771) PS26 == commit `d6d5834ea6`**, the VTree
stack TIP. VERIFIED it carries `IndexType.VTREE` and the full stack in one ref: 3771→3760(#21159, adds
IndexType.VTREE)→p3→p2→p1→base `3d6992d0e7`. This is the "outdated but enough" round-2 fold (p1''=8c311dcf74
etc.), NOT the latest round-3 — but it compiles and has every symbol the gate needs. Committed as
**`deb8f8e6`** on branch **`vtree-gate-manifest`** in `.repo/manifests` (`[DO NOT SUBMIT]`, Change-Id
`I62dbe46d…`); `.repo/manifests` was restored to `default` so local sync/builds are unaffected. Draft files:
`$CLAUDE_JOB_DIR/tmp/helios_variantA.xml` / `helios_variantB.xml`.

## Variant B (fallback) — mirror branch
`git push couchbase a2fe3fbe8f:refs/heads/vtree-integ` (asterixdb repo already has `couchbase` remote =
github mirror), then `revision="vtree-integ"`. Robust (mirror is always reachable, SHA on a real branch);
use if Variant A's CI unknowns bite.

## Push (needs explicit approval per [[ask-before-implementing]])
Topic is **`hongyu/vtree`** — cbas gate 249142 ALREADY has it set, so no set-topic needed; just push the
manifest change under the same topic:
```
cd $WORKSPACE/.repo/manifests && git checkout vtree-gate-manifest
git push ssh://hongyushi-creator@review.couchbase.org:29418/manifest HEAD:refs/for/master%topic=hongyu/vtree
```

## Two CI unknowns that ONLY Variant A carries (confirm with build-team colleague)
1. Does CV have network egress to `asterix-gerrit.ics.uci.edu`? (build normally only hits the mirror.)
2. Will `repo sync` fetch a `refs/changes/…` revision? If CV runs `repo sync -c` (shallow), an arbitrary
   change ref may not fetch → pin SHA `d6d5834ea6` or fall back to Variant B.
Also unconfirmed: whether CV honors a topic-linked manifest change vs. needing the manifest merged first.

## Toy build (alternative to CV — build on demand from the same topic)
Job `toy-unix-simple` on **server.jenkins.couchbase.com** (NOT analytics jenkins). Same topic bridge:
`GERRIT_TOPICS` / `GERRIT_CHANGE_IDS` / `GERRIT_REVIEW_IDS` cherry-pick Couchbase Gerrit changes (mutually
exclusive, pick one); patch_via_gerrit is manifest-aware so a `manifest`-project change in the topic is
applied + re-synced BEFORE building. Our params: `RELEASE=master` (slave class only, not the manifest),
`DISTRO=linux`, `ARCH=x86_64`, `EDITION=enterprise`, `MANIFEST_REPO=https://github.com/couchbase/manifest`,
`MANIFEST_FILE=enterprise-analytics/helios.xml` (MUST change from default couchbase-server/morpheus.xml),
`GERRIT_TOPICS=hongyu/vtree`. Pulls 249142 (gate) + 249676 (manifest→asterixdb ASF ref), no merge.
Trigger via API (crumb needed): `POST .../job/toy-unix-simple/buildWithParameters --data-urlencode ...`.
First run = build #13138 (2026-07-29). Same Variant-A risk at `repo sync` (ASF ref fetch + egress).

## RESOLVED 2026-07-29 — toy build #13139 GREEN (full EA installer built)
The bridge works, but the FIRST attempt failed on **version skew**, not the mechanism: our VTree review
chain is based on asterix `3d6992d0e7`, which is **15 commits behind** `couchbase/master` (asterixdb mirror
`49d9d44fe3`). Pointing the manifest at that old base rewound asterixdb and hid symbols current cbas-core
master needs (`QueryPlanCacheKey.statementParameters()`, `ExternalDataConstants.KEY_ILLEGAL_CHARACTER_HANDLING`,
a changed `CBCompilationProvider` lambda) → `cannot find symbol`. NOT VTree code; purely base age.
Fix sequence that worked:
1. **Rebased** VTree stack (p1..3771 = round-3 `a2fe3fbe8f`, excl. spann) onto `couchbase/master`
   `49d9d44fe3` → local branch `vtree-integ` (CLEAN, no conflicts; has IndexType.VTREE + the newer symbols).
   Deprecated old `vtree-integ` backed up as `vtree-integ-deprecated-bak`. Rebase is asterixdb-repo only.
2. **ASF forge-author block:** 3760 is Calvin-authored (`calvinthomas.dani@gmail.com`); Le0shy lacks
   `forge author`, so ASF rejects a chain push. WORKAROUND (user's idea): **squash the whole stack into ONE
   Le0shy-authored WIP commit** (new Change-Id) → no Calvin commit to reject, and a standalone change that
   doesn't touch the real reviews. Pushed `git push gerrit vtree-toybuild-squash:refs/for/master%wip` →
   **ASF change 21470** (`[DO NOT REVIEW][WIP]`, ref `refs/changes/70/21470/1`, tip `db08cddcc1`).
3. **Re-pointed** manifest change 249676 `revision="refs/changes/70/21470/1"` (still `remote="asterix-asf"`),
   moved BOTH 249142 + 249676 to topic **`hongyu/vtree-integ`** (fresh topic for the integ attempt).
4. **Toy build** `GERRIT_TOPICS=hongyu/vtree-integ` → **#13139 SUCCESS** (21.7 min): patch_via_gerrit applied
   249676 to manifest + re-synced asterixdb from 21470 + applied 249142 to cbas-core; Maven (asterix+cbas+gate)
   BUILD SUCCESS, C++ 100%, RPM+DEB `enterprise-analytics-2.3.0-23139` produced. No merges.
Takeaway: to build cbas-core master against VTree, the VTree asterix MUST be rebased onto current master
first (the review chain's old base alone won't compile against today's cbas). jjdoc `VM_global_library.vm`
`[ERROR]` lines + post-build `No file path provided for script file #0` are benign noise.

## DECISION 2026-07-29 — keep cbas-core LIVE, regen squash onto LIVE master before every push
The manifest pins ONLY asterixdb (→ 21470 squash); **cbas-core stays `revision="master"` (live)**. Because
asterix+cbas advance in lockstep via coordinated merges (CVE deps e.g. httpclient5 5.5.2→5.6.2 in a430b1f877,
cost-methods MB-73032/ASTERIXDB-3797, hash-distinct), a pinned-asterix + live-cbas-core setup DRIFTS every time
master merges — this is the recurring CV skew (e.g. `ClusterIntegrationIT` NoClassDefFound TlsSocketStrategy).
User chose to keep cbas-core live and accept the treadmill. **Standing rule: `git fetch couchbase master` then
regen the squash onto LIVE `couchbase/master` immediately before each push** (do NOT push a stale-pinned squash).
`regen-squash.sh` already defaults to `couchbase/master`; rerere auto-resolves the recurring
SqlppCompilationProvider conflict. (Round-4 used MASTER=a010add9e6 for a clean one-off delta — that was the
exception, not the rule.) Alternative rejected: pinning cbas-core too (coherent frozen snapshot).

## COROLLARY 2026-07-29 — the GATE also drifts; rebase it onto cbas-core master before each push
"Keep cbas-core live" means the gate (249142) itself goes stale too. CV **checks out the gate commit** (cbas-core
= the gate's tree on its parent), so if the gate's base lags cbas-core master, the build MISSES recent coordinated
merges. Concretely: the gate sat on `310a31dbb` (−4), missing **MB-72431 (CVE deps)** which bumps httpclient5 to
**5.5** and edits `cbas-cbserver-test/pom.xml` → the cbserver cluster tests failed with
`NoClassDefFoundError TlsSocketStrategy` (httpclient5 5.4 class). FIX = `git checkout vtree; git rebase gerrit/master`
(cbas-core master) → picks up MB-72431 → push PS. So: **before each gate push, rebase the gate onto current
cbas-core `gerrit/master`** (analytics repo), just as the squash is rebased onto asterix master. Two treadmills.

## NOTE — 21100 (p2) mergeable=False is NOT a real conflict
ASF submit_type=**CHERRY_PICK**. Gerrit tests each stacked change standalone; p1 and p2 BOTH edit
`hyracks-fullstack/hyracks/pom.xml` (module registration), so p2 can't cherry-pick onto master without p1 →
mergeable=False. p1/p3 = True. It clears on ordered submit (p1 first). Don't chase it. See
[[vtree-asf-commit-message-workflow]].

## Access / creds
- Couchbase Gerrit user `hongyushi-creator`; **prior ACL blocker RESOLVED** — `cbas-core` and `manifest`
  projects now visible + pushable (was blocked 2026-07-20). `manifest` project = github.com/couchbase/manifest.
- ASF Jenkins console auth: `curl -u hshi@scu.edu:$(cat ~/.jenkins_auth_asf)` (API token, 34 chars).
  Gerrit ASF creds in `~/.netrc` (`curl --netrc`). Couchbase Gerrit via SSH key (hongyushi-creator).
- Jenkins tokens (all token-only files, user = the email shown): `~/.jenkins_auth_asf` → asterix-jenkins.ics.uci.edu
  (`hshi@scu.edu`); `~/.jenkins_auth` → analytics.jenkins.couchbase.com (`hongyu.shi@couchbase.com`);
  **`~/.jenkins_auth_cb` → server.jenkins.couchbase.com (`hongyu.shi@couchbase.com`)** = the toy-build host.
  Trigger toy build: crumb via `/crumbIssuer/api/xml`, then `POST /job/toy-unix-simple/buildWithParameters`.
