---
name: vtree-asf-commit-message-workflow
description: "Mentor's required commit-message format for pushing VTree storage patches to ASF Gerrit (template, Ext-ref, no cherry-pick)"
metadata: 
  node_type: memory
  originSessionId: 7215a24d-5931-4f2d-a7f7-01a58dabdad1
---

Before pushing/submitting a VTree patch to **ASF AsterixDB Gerrit**, the commit message MUST follow the
mentor's template (template file kept at `asterixdb/.vtree/commit-template.txt`):

```
[ASTERIXDB-XXXX] <subject>

- user model changes: no
- storage format changes: no
- interface changes: no

Details:
<what the change does>

Ext-ref: MB-XXXXX
Change-Id: I...   (PRESERVE the existing Change-Id)
```

Rules:
- The three `changes:` flags are yes/no, set honestly. VTree storage: **p1** = storage yes / interface yes;
  **p2** = storage yes / interface yes; **p3** (tests) = all no. User-model = no for all storage patches.
- **`Ext-ref:`** = the Couchbase MB ticket. For VTree we use **`MB-72975`** (the columnar gate ticket).
- **Strip `(cherry picked from commit ...)`** lines — never leave them (see [[feedback-no-cherry-pick-annotation]]).
- Author = `Le0shy <leoshy1005@gmail.com>`; **committer must also be `leoshy1005@gmail.com`** (couchbase email
  rejected on ASF) — see [[vtree-gerrit-committer-email]].

## How it was applied (2026-07-29)
Per-patch message files drafted in `asterixdb/.vtree/msg-p1.txt` / `msg-p2.txt` / `msg-p3.txt`. Reworded the
whole `vtree-dev` chain non-interactively with `git filter-branch --msg-filter "bash .vtree/msgfilter.sh"`
(matches by Change-Id → writes the right file; else strips cherry-pick) plus
`--env-filter` forcing committer=Le0shy. Message-only rewrite → tree unchanged → squash/CV unaffected.
Then `git push gerrit ${P3}:refs/for/master` (p3 tip) updated **21099/21100/21101** by Change-Id.
NOTE (zsh gotcha): use **`${VAR}:refs/for/master`** with braces — bare `$VAR:refs` / `$VAR:h` triggers zsh
`:r`/`:h` modifiers and mangles the refspec/path. p1 also now carries the round-4 EnforcedIndexCursor fix.
