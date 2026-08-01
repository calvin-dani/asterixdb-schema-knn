---
name: feedback-gerrit-reply-threading
description: "Posting Gerrit reply drafts via REST must replicate the parent comment's exact anchor (patch_set + range + line + side), not just in_reply_to"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 7215a24d-5931-4f2d-a7f7-01a58dabdad1
---

When posting reply drafts to Gerrit review comments via the REST drafts API
(`PUT /a/changes/{n}/revisions/{rev}/drafts`), `in_reply_to` alone does NOT thread the reply. The reply
must replicate the **parent comment's exact anchor**, or it renders as a standalone/orphan comment and the
parent stays `unresolved`:

1. **patch_set** — create the draft on the SAME revision the parent comment is on (put that revision SHA in
   the URL). Reviewer comments are often scattered across old patchsets (e.g. one on ps20, another on ps25),
   NOT all on "current". Fetch each parent's `patch_set` from `/comments` and map number→SHA via
   `?o=ALL_REVISIONS`. Do NOT post everything to `revisions/current/drafts`.
2. **range** — copy the parent's full `range` object (`start_line/start_character/end_line/end_character`).
   Shahrzad's r2 comments were all range comments; a reply with only `line` (no range) orphaned even when the
   patch_set matched. This was the decisive missing piece.
3. **line** and **side** — copy from the parent too.
4. `in_reply_to` = parent's FULL id (e.g. `d87fcd91_d356cad8`), not the 8-char prefix (400 "comment not found").
5. `unresolved: false` on the reply marks the thread resolved (state = last comment in the chain).

**Verify** after posting: fetch `/comments` + `/drafts`, and for each draft confirm `patch_set` AND `range`
equal the parent's. Ground truth: existing correctly-threaded replies on the change sit on the same
patch_set as their parent with a matching anchor.

**Why:** got the storage-p2 (change 21100) replies wrong twice - first posted all to current patchset, then
matched patch_set but still omitted the range, orphaning all 8. See [[vtree-branch-model]],
[[vtree-infra-access-cheatsheet]].
