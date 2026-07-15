---
name: bug-clusterby-scoping-traversal
description: Two CLUSTER BY rewrite-pass bugs (Module C) found via Module F plan-golden test; both fixed
metadata: 
  node_type: memory
  type: project
  originSessionId: d3654f88-1bbd-4551-b0b3-49373a1d7da3
---

CLUSTER BY (see [[project-clusterby-architecture]]) Phase-1 had **two rewrite-pass bugs**, both
fixed 2026-06-21 while building the first Module F plan-golden test
(`optimizerts/queries/cluster-by/cluster-by-with-members.sqlpp`). Both were in SQL++ visitor
base classes that drive variable resolution/scoping but were never taught about `ClusterbyClause`.

**Bug 1 — clustering expr never resolved (ASX1038 "Illegal state. $r").**
`AbstractSqlppSimpleExpressionVisitor.visit(SelectBlock)` traversed from/let-where/**groupby**/
let-having/select but had **no `clusterByClause` branch**, so the variable-resolution pass never
descended into the clustering expression. Fix: add
`if (selectBlock.hasClusterbyClause()) selectBlock.getClusterbyClause().accept(this, arg);`
parallel to the groupby branch.

**Bug 2 — `sc`/`rvc` not in scope (type-inference NPE `inputType is null`).**
`AbstractSqlppExpressionScopingVisitor` had a `visit(GroupbyClause)` that builds a new scope
(group key/decor/group-as vars) and `replaceCurrentScope`, but **no `visit(ClusterbyClause)`**.
So after CLUSTER BY the pre-cluster FROM var (`r`) stayed in scope and `sc`/`rvc` were never
added. SQL++ implicit field-resolution then rewrote `sc.cluster_id` → `$$r.getField("sc").getField("cluster_id")`;
since `r` (Reviews) has no field `sc`, the field-access type is null → NPE in
`OpenRecordConstructorResultType`/`TypeComputeUtils.getActualType`. Fix: add a
`visit(ClusterbyClause)` mirroring `visit(GroupbyClause)` — resolve clustering + member-field
exprs in the old scope, then new scope with `clusterDescriptorVar` (+ `clusterMembersVar` if
present), `replaceCurrentScope`.

**Lesson:** every place that special-cases `GroupbyClause` (traversal + scoping) needs the
`ClusterbyClause` twin. Module F's plan-golden is what surfaces these — they are NOT compile
errors (unlike Module A's 27 visitors). Grep for `GroupbyClause` across the rewrite visitors to
find any remaining gaps before declaring Phase-1 done.

**Debug technique that nailed it:** surefire `-DtrimStackTrace=false` for the real stack;
temporary `System.err` dump in `AlgebricksOptimizationContext.computeAndSetTypeEnvironmentForOperator`
printing `AbstractAssignOperator.getExpressions()` revealed the mis-resolved
`{"cid": $$r.getField("sc")...}` expression.
