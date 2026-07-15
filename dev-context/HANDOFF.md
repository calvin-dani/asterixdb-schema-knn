# VTree development migration — handoff (2026-07-08)

Restore the working environment for the VTree vector-index project on a new machine with
Claude Code. Everything you need is on this branch (`vtree-development-migration`) except the
large datasets (excluded from git — see §4).

## 1. Restore context files

```bash
# From the repo root, after cloning and checking out this branch:
cp dev-context/CLAUDE.local.md ./CLAUDE.local.md          # private project instructions (not committed on other branches)
git apply dev-context/local-tweaks.patch                   # single-NC cc-main.conf tweak for local integration runs

# Claude Code persistent memory — the directory name is the repo path with '/'→'-':
# if the repo lives at /Users/<you>/Projects/dev/asterixdb-schema-knn the slug matches below;
# otherwise adjust the slug to your actual absolute path.
mkdir -p ~/.claude/projects/-Users-<you>-Projects-dev-asterixdb-schema-knn/
cp -r dev-context/memory-snapshot ~/.claude/projects/-Users-<you>-Projects-dev-asterixdb-schema-knn/memory
```

`MEMORY.md` in the snapshot is the index Claude loads each session — it carries the full
project state (stack, open bugs, decisions). Read it first; it links everything else.

## 2. Branch topology (fetch from origin = github.com/calvin-dani/asterixdb-schema-knn)

- **`vtree-spann-integrate`** — CANONICAL 6-commit Gerrit-ready stack (head `9c5fd8c704`):
  SPANN / 3771(+composite-PK fix+runtimets) / 3760(+trainseed+leak fixes+JUnit) /
  3754 p3(+tests) / p2(+merge-bug fixes) / p1(+flush-root fix). All validated:
  35/35 module, 6/6 runtimets, 7/7 integration Parts 3+4.
- `vtree-merge-fixes` — same tree, but the two merge fixes as ONE standalone commit
  (`a7b0546991`) for isolated review.
- `vtree-development-migration` — this branch (docs + integration suite + context).
- Local-only on the old laptop (push if needed): `fix-ann-completeness` (top-K dedup fix,
  deferred by design), backups (`*-old`, `*-pre-fix2`, `*-pre-mergefix`), `tmp-3760-ps20/21/22`.

## 3. Docs

`doc-vtree/` is the project documentation home — start at `doc-vtree/README.md` (index +
conventions). `60-quality/bug-archive.md` is the canonical bug ledger (10 fixed, several open
— k-means leaf-drop is the top open item, task-chip diagnosis lives in the archive entry).

## 4. Datasets (NOT in git — 27 GB)

`integration/datasets/` is gitignored. Restore by either `scp` from the old laptop, or
re-download: glove-100-angular + gist-960-euclidean from ann-benchmarks.com (convert via
`integration/scripts/`), movie embeddings per `integration/MOVIE_INDEX_TEST.md`. Only the
`*.limited_50000` variants are needed for the standard suite. `integration/tests/config.yaml`
is included — note `k_multiplier: 3` in Parts 3/4 (the cross-pollination headroom contract).

## 5. Build & test quickstart

```bash
mvn clean install -DskipTests -q -B                                  # full tree (~3 min)
cd hyracks-fullstack && mvn -pl hyracks/hyracks-tests/hyracks-storage-am-lsm-vtree-test test  # storage 35/35
# runtimets: add vector cases to asterix-app/src/test/resources/runtimets/only_sqlpp.xml,
#   then: cd asterixdb/asterix-app && mvn test -Dtest=SqlppExecutionTest  (restore only_sqlpp.xml after)
cd integration/tests && python3 run_tests.py --parts 3 4             # integration (needs datasets)
```

Remember (from memory): surefire reads ~/.m2 — `mvn install` changed modules before
`surefire:test`; user rebuilds manually — don't auto-run installs uninvited.

## 6. Pending items at migration time

1. Gerrit: p2′ (`250230d228`) + p3′ (`10703e9c19`) need re-upload (Le0shy authorship — push
   directly); then regenerate the admin bundle for 3760/3771/SPANN
   (`git bundle create <f> 10703e9c19..vtree-spann-integrate`) — needs forge-author admin.
2. Open bugs: k-means leaf-drop (top priority; suspect for Part 2 recall WARNs), field-2
   semantics decision, index-only+WHERE `select (missing)`, dot_product() ORDER BY sign
   hazard, manhattan ghost metric, Job-2 sample-scan unseeded (trainseed incomplete for
   card ≥ 10k). Details + fix sketches: bug-archive.md and MEMORY.md.
3. Part 2 recall WARNs: re-judge after seeding training or fixing leaf-drop.
