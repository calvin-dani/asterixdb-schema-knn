---
name: feedback-hold-storage-patch-push
description: Prepare the storage-patch/squash push but STOP before pushing — user reviews and often adds changes first
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 7215a24d-5931-4f2d-a7f7-01a58dabdad1
---

When a round is ready to push to Gerrit (especially the storage patches / the 21470 squash), **prepare
everything but do NOT push** — stop and wait for explicit confirmation. The user frequently wants to
double-check and **add something to the storage patches first**, so "everything prepared" ≠ "ready to push".

**How to apply:** regen the squash / fold the round / stage the manifest bump, then report the staged state
(SHA, delta, hook-safety) and HOLD. Push only on an explicit "push now". Extends [[ask-before-implementing]]
(ask before every Gerrit push). Relates to [[vtree-cbas-gate-manifest-bridge]] / [[vtree-infra-access-cheatsheet]].
