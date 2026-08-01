---
name: vtree-ci-verification-workflow
description: "The standard 4-tier verification flow for VTree changes (storage unit → runtime unit → golden runtimets → external integration), fast-to-slow"
metadata: 
  node_type: memory
  type: project
  originSessionId: 7215a24d-5931-4f2d-a7f7-01a58dabdad1
---

The VTree project's de-facto CI / pre-push verification flow is **4 tiers, run fast-to-slow (fail fast)**.
A change to the VTree stack should pass all four before pushing. Established during the metric-in-factory
refactor (2026-07-28), which exercised the whole flow green.

**Tier 1 — Storage-layer unit tests** (in-JVM, seconds). The LSM VTree suite:
`cd hyracks-fullstack && mvn -pl hyracks/hyracks-tests/hyracks-storage-am-lsm-vtree-test -am install`
→ 26 tests (build / flush / search / insert / delete / merge / multithread / quantized). Note:
`hyracks-storage-am-vtree` itself has no `src/test`; the storage tests live in the lsm-vtree-test module.

**Tier 2 — Runtime (operator) unit tests** (in-JVM, seconds). The asterix-side VTree operator tests
(the 3760 operators): `cd asterixdb && mvn -pl asterix-runtime test` → ~18 tests
(QuantizationConstantsAggregate + HierarchicalKMeansPlusPlus + VectorComponentExtractor).

**Tier 3 — Golden-file suite** (in-JVM cluster, ~1–2 min + a build). The `vector` runtimets group via
`SqlppExecutionTest` (Clean-JSON result goldens + plan goldens); 8 cases incl. cosine glove, all-metrics
distance-functions, movie, spann, composite-pk, include-filter, invalid-field-type. To run just this group:
temporarily put the `<test-group name="vector">` block (from
`asterix-app/.../runtimets/queries_sqlpp/vector/VectorQueries.xml`) into `only_sqlpp.xml`, then
`mvn -pl asterix-app -Dtest=SqlppExecutionTest test` (restore `only_sqlpp.xml` after). Needs a prior
`mvn -pl asterix-app -am install -DskipTests`. NB `make install` leaves algebricks stale — do the full
hyracks→asterix `mvn install`.

**Tier 4 — External integration suite** (live 2-NC cluster, ~1 min + a build). `integration/tests/run_tests.py`
(4 parts: index-creation, ANN-recall, insert/delete, flush+compact+**restart**), lifecycle-managed so the
restart sub-test actually runs and validates reload-from-disk / persisted-format changes. Full recipe
(build asterix-server, link datasets, run) in [[vtree-external-integration-suite]]. Recall WARNs in Part 2
are normal variance, see [[vtree-integration-recall-variance]].

Rationale for the ordering: Tiers 1–2 catch logic/compile regressions in seconds; Tier 3 catches
plan/result drift; Tier 4 is the only tier that exercises a real restart + on-disk format reload, so it's
the one that validates persistence/serialization changes (e.g. resource-format changes) end to end. Only
Tier 4 costs a cluster bringup, so it runs last.
