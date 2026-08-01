---
name: vtree-integration-recall-variance
description: "VTree integration ANN suite glove_cosine recall is non-deterministic and sits on the 50% fail-threshold; a FAIL there is variance, not a regression"
metadata: 
  node_type: memory
  type: project
  originSessionId: 7215a24d-5931-4f2d-a7f7-01a58dabdad1
---

The VTree integration suite (`asterixdb/integration/tests/run_tests.py`, run with `--config config_no_gist.yaml` since the gist-960 dataset is absent locally) has non-deterministic Part 2 ANN recall because the hierarchical k-means training seed is not fixed. Observed run-to-run on **byte-identical code**: `glove_cosine` 47.6% (FAIL) then 56.4% (WARN); `movie_esq` bounces ~69–70% around its WARN threshold.

Thresholds: WARN < 70%, FAIL < 50%. `glove_cosine` sits right on the 50% floor, so it intermittently FAILs purely from variance.

**Why:** approximate ANN recall + random k-means++ seeding; the fail-threshold is fragile against the real 47–56% range.

**How to apply:** a lone `glove_cosine` FAIL is NOT a regression — re-run parts `1 2` to confirm it swings back above 50% before investigating code. The deterministic signals are the build, the unit suites (lsm-vtree 26/26, asterix-runtime 18/18), the vector runtimets golden files (7/7), and integration Part 3/Part 4 (insert/delete, flush/compact). `ann_pre_delete_Movie` passing is the [[vtree-review-fixes-patchset-mapping]] seed-orphan fix validation. See [[clusterby-tests-label-invariance]] for the related "k-means output is not deterministic" theme.
