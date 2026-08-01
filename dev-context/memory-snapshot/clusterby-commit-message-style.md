---
name: clusterby-commit-message-style
description: "Do NOT add Co-Authored-By: Claude to CLUSTER BY / clusterby-branch commit messages"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 2147a65d-7f57-419c-ad59-1b3f153979ce
---

For the CLUSTER BY work on the `clusterby`/`clusterby-dev` branches (Gerrit-bound, ASTERIXDB-3783), do NOT append
the `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` trailer to commit messages.

**Why:** AI authorship is already recorded in-code via the `@AiProvenance` annotations (per asterixdb/CLAUDE.md),
so the Co-Authored-By trailer is redundant on these commits. This OVERRIDES the global "end commit messages with
Co-Authored-By: Claude…" instruction for this project's Gerrit patches.

**How to apply:** When committing/amending clusterby patches, keep the `Change-Id:` trailer but omit Co-Authored-By.

**AUTHOR EMAIL (critical for Gerrit push):** the repo git config is `Hongyu <hongyu.shi@couchbase.com>`, but the
Apache Gerrit account only has `leoshy1005@gmail.com` registered — pushing a commit authored by the couchbase email
is REJECTED ("invalid author … lack 'forge author' permission"). So author AND committer of clusterby commits must be
`Hongyu <leoshy1005@gmail.com>`. When committing/amending/cherry-picking, set it explicitly:
`GIT_COMMITTER_NAME=Hongyu GIT_COMMITTER_EMAIL=leoshy1005@gmail.com git commit --amend --author="Hongyu <leoshy1005@gmail.com>" …`
(cherry-pick preserves the source author, so re-amend after). Gerrit remote: `ssh://…asterix-gerrit.ics.uci.edu:29418/asterixdb`.

**Also:** watch two non-blocking Gerrit warnings — subject line >50 chars and message lines >72 chars; wrap for cleanliness.

**ALWAYS ask before pushing to Gerrit (user feedback 2026-07-08):** never run `git push gerrit ...` (any
`refs/for/*` push, new patchset or new change) without explicit user confirmation for THAT push. One approved
push does not authorize the next — e.g. approval for PS4 did not cover PS5. Prepare everything (commit, validate,
verify Change-Id/committer), then ask.

**No internal jargon in upstream patches (user feedback 2026-07-08):** scrub internal roadmap wording from code
comments, test-file comments, and AiProvenance `notes` before pushing to Apache Gerrit — no "Phase 2", "Phase 2 (B)",
"Milestone", and no references to untracked internal docs (`doc-clusterby/...PHASE2_*.md`). Use neutral wording
("CLUSTER BY", "desugared-plan validation"). Sweep with:
`grep -rn "Phase 2\|Phase A\|Phase B\|PHASE2\|doc-clusterby\|clusterby-dev" <changed files>` before every push.

Status: P1 on Gerrit change #21384 PS5 (commit ad45101524, 2026-07-08). See [[clusterby-branch-model]].
