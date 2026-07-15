---
name: feedback-revert-from-base-not-head
description: "When dropping a stray edit from a Gerrit change, restore from the base branch (master), not from HEAD"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: e8da6b5b-eb59-469f-9d56-05ade6986d72
---

When dropping an unintended edit from a Gerrit change before amending, restore the file from the **upstream remote target branch** (e.g., `git fetch gerrit master && git checkout FETCH_HEAD -- <file>`), not from local `master` (which may be stale) and not from `HEAD` (which already contains the bad edit).

**Why:** Three layers of trap, all hit on ASTERIXDB-3771 PS11→PS14:
1. `git checkout HEAD -- <file>` "reverts" to the previous-patchset's corrupted content (HEAD is just the last commit of *this* change, not upstream-clean).
2. `git checkout master -- <file>` restores from *local* master, which may be weeks behind real upstream master. Caught when local master `b248800041` still had `USE_DYNAMIC_RANGE` but upstream `ec1773881f` had deleted it via ASTERIXDB-3665 (`e0c3ccc207`). My PS13 reintroduced a line that upstream had already shipped a deletion for.
3. Even the patchset's parent (e.g., 3760 tip `4b8601ee0e`) may differ from local master — what matters for Gerrit's review diff is **upstream remote master at push time**.

**How to apply:**
- Before reverting: `git fetch <gerrit-remote> master` to refresh.
- Restore: `git checkout FETCH_HEAD -- <file>`.
- Verify three-way: `git diff FETCH_HEAD -- <file>` must be empty (this is what Gerrit will show).
- Sanity: `git diff <parent-branch-tip> -- <file>` should also be empty for files we don't intend to touch.
- Don't trust `git diff master -- <file>` if you haven't fetched recently — `master` is local.

See also [[storage-cleanup-followups]] for the cleanup pass this came out of.
