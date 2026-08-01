---
name: vtree-infra-access-cheatsheet
description: "VTree project infra cheat-sheet — how to reach BOTH Gerrits and ALL THREE Jenkins (each different auth), the push workflows, and current change/branch state"
metadata: 
  node_type: memory
  type: reference
  originSessionId: 7215a24d-5931-4f2d-a7f7-01a58dabdad1
---

Consolidated access + workflow for the VTree/columnar work. **All workflow artifacts now live in ONE place:
`analytics/asterixdb/.vtree/`** (git-ignored via `.git/info/exclude` — never reaches Gerrit; survives branch
checkouts): `workflow.md` (human-readable doc), `rounds.md` (round journal), `*.sh` (regen/snapshot/bundle
scripts), `bundles/`, `rebased-bundle/`, `gerrit-notes/`. Only credential files stay in `~/` (`.jenkins_auth*`,
`.netrc`). See also [[vtree-branch-model]],
[[vtree-cbas-gate-manifest-bridge]], [[vtree-gerrit-committer-email]], [[vtree-gate-gerrit-push]],
[[vtree-review-fixes-patchset-mapping]]. Respect [[ask-before-implementing]] (ask before every push).

## Two Gerrits (do NOT confuse)
- **ASF AsterixDB** `asterix-gerrit.ics.uci.edu` — the asterix engine review (ASTERIXDB-3754/3760/3771).
  - SSH: `ssh -p 29418 Le0shy@asterix-gerrit.ics.uci.edu gerrit query --format=JSON --comments change:NNNNN`
  - HTTP API: `curl --netrc "https://asterix-gerrit.ics.uci.edu/a/changes/NNNNN/comments"` (creds in `~/.netrc`;
    strip the `)]}'` XSSI prefix: `sed "1s/^)]}'//"`). Drafts: `/a/changes/N/drafts`, PUT to
    `/a/changes/N/revisions/{sha}/drafts/{id}` (draft anchored to the patchset it was written on).
  - Push (asterixdb repo, remote `gerrit`): `git push gerrit <branch>:refs/for/master`. **Committer email MUST be
    `leoshy1005@gmail.com`** (couchbase email rejected). **Le0shy lacks `forge author`** → CANNOT push commits
    authored by others (e.g. Calvin `calvinthomas.dani@gmail.com` on 3760) → squash to one Le0shy-authored commit.
- **Couchbase** `review.couchbase.org` — cbas-core, manifest, cbas (Go).
  - SSH: `ssh -p 29418 hongyushi-creator@review.couchbase.org gerrit query --format=JSON --comments change:NNNNNN`
    / `gerrit set-topic NNNNNN --topic hongyu/vtree-integ`. **`--netrc` does NOT cover this host's HTTP API — use
    SSH `gerrit query` for reads.** Author/committer = `hongyu.shi@couchbase.com` (gmail rule is ASF-only).
  - Push cbas-core (analytics repo, remote `gerrit`): `git push gerrit HEAD:refs/for/master%topic=hongyu/vtree-integ`.
  - Push manifest (`.repo/manifests`): `git push ssh://hongyushi-creator@review.couchbase.org:29418/manifest HEAD:refs/for/master%topic=hongyu/vtree-integ`.

## Three Jenkins (each DIFFERENT auth; all token files are 1-line, 34-char, user = the email)
| Host | Purpose | curl auth |
|---|---|---|
| `asterix-jenkins.ics.uci.edu` | ASF asterix CV | `-u hshi@scu.edu:$(cat ~/.jenkins_auth_asf)` |
| `analytics.jenkins.couchbase.com` | Couchbase cbas CV (Gerrit-triggered) | `-u hongyu.shi@couchbase.com:$(cat ~/.jenkins_auth)` |
| `server.jenkins.couchbase.com` | Couchbase server/**toy builds** | `-u hongyu.shi@couchbase.com:$(cat ~/.jenkins_auth_cb)` |
- **cbas-core CV re-trigger:** the analytics CV is **patchset-upload triggered ONLY** (every build batch on 249142 followed an "Uploaded patch set N"). A `reverify`/`recheck` **comment does NOT re-run it** (confirmed 2026-07-30, exit-2 after 30 min no build). To re-run, upload a NEW patchset (amend for a fresh SHA, or push a rebase) — which is a Gerrit push (approval-gated). Jenkins consoleText needs the QUOTED inline auth `-u "hongyu.shi@couchbase.com:$(cat ~/.jenkins_auth)"` (an unquoted `$AUTH` var 401s).
- Console: `GET <host>/job/<job>/<n>/consoleText`. Params/status: `<job>/api/json?tree=building,result` or
  `.../api/json?tree=property[parameterDefinitions[name,choices,description]]`.
- Trigger a build: get crumb `<host>/crumbIssuer/api/xml?xpath=concat(//crumbRequestField,":",//crumb)`, then
  `POST <host>/job/<job>/buildWithParameters -H "$CRUMB" --data-urlencode K=V`. Resolve queue item →
  `<host>/queue/item/<id>/api/json` (`.executable.number`).
- **Toy build** = `server.jenkins…/job/toy-unix-simple`: params `RELEASE=master DISTRO=linux ARCH=x86_64
  EDITION=enterprise MANIFEST_FILE=enterprise-analytics/helios.xml GERRIT_TOPICS=hongyu/vtree-integ`. Delegates the
  real build to `analytics-toy-docker-new #NNNN`. Artifacts → `latestbuilds/enterprise-analytics/toybuilds/<bldnum>/`
  (internal host, 30-day retention).

## Fixed push workflow (both sides need CURRENT masters — two skews bit us, see [[vtree-cbas-gate-manifest-bridge]])
1. ASF asterix review: keep `vtree-dev` = canonical round-3 chain; push `git push gerrit vtree-dev:refs/for/master`
   updates 21099/21100/21101/21159/21287 (Change-Id→change). 3760 is Calvin-authored → that push is rejected;
   3760/spann go via **admin bundle** or the squash.
2. Columnar build dependency: the review chain is on OLD asterix base → REBASE onto `couchbase/master` before any
   Couchbase build (`vtree-integ`), and the cbas gate must be on cbas-core `gerrit/master`. Host the rebased asterix
   as a **squash WIP** on ASF (single Le0shy commit, dodges forge-author) → change 21470 → manifest points at it.
3. Couchbase CV/toy: manifest change (249676) repoints asterixdb + cbas gate (249142), both on topic
   `hongyu/vtree-integ`; CV/toy build via `GERRIT_TOPICS`.

## Current state (2026-07-29)
- **ASF**: 21099/21100/21101 (storage p1/p2/p3), 21159 (3760 Training), 21287 (3771 DDL/ANN) — round-3 on base
  `3d6992d0e7`. **21470** = WIP squash of the whole stack rebased onto asterix master (for the toy build).
- **Couchbase**: **249142** cbas gate `MB-72975` (PS6 = rebased onto cbas master + 10 VTree error codes excluded);
  **249676** manifest → `asterixdb refs/changes/70/21470/1`. Topic `hongyu/vtree-integ`.
- **Toy build** `toy-unix-simple #13139` GREEN (full EA installer `2.3.0-23139`). cbas CV on 249142 iterating.
- **Keeper local branches** (asterixdb repo): `vtree-dev` (working branch = review chain + `doc-vtree/` +
  `integration/` in the TOP commits only), `vtree-integ` (rebased onto master = 21470 source),
  `vtree-toybuild-squash` (= 21470 `db08cddcc1`). cbas-core (analytics repo): branch `vtree` = the gate.
- **`doc-vtree/` + `integration/` must NEVER reach ASF Gerrit.** Tracked only in vtree-dev's top commits; the 5
  review commits (p1..3771) don't carry them. A `.git/hooks/pre-push` guard rejects any push whose tip tree
  contains them to `asterix-gerrit.*`. Push a review SHA (3771 `a2fe3fbe8f`), not the vtree-dev tip. Portability:
  `git push vtree vtree-dev` (remote `vtree` = github.com/calvin-dani/asterixdb-schema-knn) to work elsewhere.
  Human-readable full doc: `~/vtree-infra-workflow.md`.
- **ASF open review threads on 21099**: only 6 truly open (68 threads, 62 resolved) — the "unresolved comment" count
  over-counts; reconstruct THREADS (tip comment's `unresolved` flag) to get the real number.
