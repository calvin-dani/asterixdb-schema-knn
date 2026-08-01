---
name: clusterby-docs-workflow
description: Where CLUSTER BY design/status docs live and why they can never reach Gerrit
metadata: 
  node_type: memory
  type: project
  originSessionId: 7215a24d-5931-4f2d-a7f7-01a58dabdad1
---

CLUSTER BY working docs (design, implementation notes, status) live in a **separate git repo**
`~/clusterby-docs` — deliberately NOT inside the asterixdb repo, so they can never be part of a Gerrit
patch. Set up 2026-07-13 (user chose separate-repo over a nested repo for maintainability + durability:
no nested-repo footgun, survives worktree deletion).

Layout:
- `~/clusterby-docs/` — standalone git repo (author Hongyu <leoshy1005@gmail.com>). Files:
  `CLUSTER_BY_GERRIT_STATUS.md` (series/review/branch status snapshot), `README.md`, `design/`.
- `~/clusterby-gerrit-wt/asterixdb/doc-clusterby` → **symlink** to `~/clusterby-docs` (co-located for
  editing; edits go straight into the docs repo).

Leak guard (TWO layers, do NOT remove):
1. The symlink is git-ignored via `.git/info/exclude` in the asterixdb COMMON git-dir
   (`/Users/hongyu.shi/Proj/workspace/.repo/projects/analytics/asterixdb.git/info/exclude`), shared by
   all worktrees. Pattern must be `doc-clusterby` (NO trailing slash, NO leading slash) — a trailing
   slash matches dirs only (misses the symlink), and the repo TOPLEVEL is `~/clusterby-gerrit-wt` with
   `asterixdb/` one level down, so a leading-slash anchor would point at the wrong level.
2. `info/exclude` is local-only (never pushed) — a tracked `.gitignore` would itself be a Gerrit-bound
   change, so it is intentionally NOT used.

Verified: `git add -A --dry-run` from both `asterixdb/` and the toplevel stages nothing for
doc-clusterby. RULE: never `git add doc-clusterby` explicitly in the asterixdb repo (that would force
past the ignore). Update these docs alongside development (decision made / patch lands / design changes),
same cadence as memory. See [[clusterby-branch-model]].
