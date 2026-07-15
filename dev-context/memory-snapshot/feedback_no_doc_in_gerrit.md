---
name: Never commit doc/ to Gerrit
description: Internal working docs like vtree-cleanup-calvin-todos.md must never be staged in Gerrit commits
type: feedback
originSessionId: a6c00089-526e-4d76-a025-5219da303cc6
---
Never stage files under `doc/` (like `vtree-cleanup-calvin-todos.md`) as part of commits destined for Gerrit. These are internal working documents, not for code review.

**Why:** Gerrit is for code review submissions only. Internal docs/TODOs should stay local.
**How to apply:** When preparing commits for Gerrit push, always verify no doc/ files are included. Keep them untracked.
