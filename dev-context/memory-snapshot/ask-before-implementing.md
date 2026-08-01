---
name: ask-before-implementing
description: Present design and get explicit user permission BEFORE writing implementation code
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 7215a24d-5931-4f2d-a7f7-01a58dabdad1
---

For feature/design work (especially CLUSTER BY / asterixdb), do NOT start editing implementation code
right after proposing an approach.

**Why:** user was unhappy when I sketched a design and immediately began implementing it (k-means||
init, 2026-07-08): "before you implementing, get a permission from me." Design decisions are theirs
to ratify; they may want to check alignment with the source papers/design docs first.

**How to apply:** propose the design (with the decisions called out), then STOP and wait for explicit
approval before any Edit/Write to product code. Same spirit as [[clusterby-commit-message-style]]'s
ask-before-Gerrit-push rule. Small mechanical fixes the user already asked for are exempt.
