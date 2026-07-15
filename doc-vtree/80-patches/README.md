# Patch walkthroughs

One document per Gerrit patch of the VTree stack. Each walkthrough explains **what layer the
patch adds**, breaks it down module by module, and traces example data through the new code
("data-stream walkthrough"). These are commit-anchored: they describe the patch as it exists
on the branch named in the header, not the original Gerrit patchset (this stack has been
rebased and amended — see each doc's caveats).

Convention: `NNNN-short-title.md` where `NNNN` is the ASTERIXDB issue number. Multi-part
patches get a letter suffix (`3754a-...`, `3754b-...`).

| Patch | Commit (integrate-newbase) | Doc |
|---|---|---|
| 3754 p1 — VTree core module (`hyracks-storage-am-vtree`) | `f87cba1ca7` | [3754a-storage-layer-p1.md](3754a-storage-layer-p1.md) |
| 3754 p2 — LSM wrapper module (`hyracks-storage-am-lsm-vtree`) | `8ccd96d69d` | [3754b-storage-layer-p2.md](3754b-storage-layer-p2.md) |
| 3754 p3 — unit-test suite (`hyracks-storage-am-lsm-vtree-test`) | `8d911ed996` | [3754c-storage-layer-p3.md](3754c-storage-layer-p3.md) |
| 3760 — training VTree index | `15c888f490` | [3760-training-vtree-index.md](3760-training-vtree-index.md) |
| 3771 — ANN query optimizer rule + DDL surface | `e36bfa0681` | [3771-ann-optimizer-rule.md](3771-ann-optimizer-rule.md) |

Suggested reading order for newcomers: 3754a → 3754b → 3760 → 3771, with 3754c alongside
3754a/b as the executable specification of the storage contracts.
