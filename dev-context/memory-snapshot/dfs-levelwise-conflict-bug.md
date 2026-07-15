# DFS vs Level-wise Cluster Conflict Bug

## Summary

ANN queries on VCTree vector indexes returned fewer PKs than expected because one leaf cluster was permanently lost during search. The root cause was a conflict between the two cluster selection methods (DFS and level-wise) in `LSMVCTreeSearchCursor.doOpen()`.

## Symptom

- Part 3 integration test: ~50-100 PKs missing from ANN query results (pre-delete count)
- One specific centroidId (e.g., cid=54 or cid=44, varies per run) was loaded during bulkload but never searched
- The visited centroid set at DFS exhaustion was missing exactly one centroid (49 out of 50)

## Root Cause

### Two Cluster Selection Methods

The VCTree search uses two methods to find clusters:

1. **Level-wise** (`findCloseCentroidsLevelWiseGlobalSort`): BFS through tree, epsilon-bounded at each interior level. Collects all reachable leaf centroids, sorts globally, applies global epsilon threshold.

2. **DFS** (`initializeClusterIterator` + `findNextClosestCluster`): Greedy top-down traversal picking the single closest child at each interior level.

### Why They Find Different "Closest" Clusters

The interior centroid that routes to a subtree is NOT the same as the leaf centroids within that subtree. Example from actual test run (Partition 2):

```
                    Root
                   /            \
          Interior Child A    Interior Child B
          (closer to q)      (farther from q)
               |                    |
        Leaf page=5           Leaf page=3→10→11→12
        (1 centroid)          (33 centroids, overflow chain)
               |                    |
           cid=44              cid=9, 10, ..., 38, ..., 41
           d=0.837             d=0.668 ... 1.57
```

- **DFS**: Picked Interior Child A (closer to query at interior level) → descended → found cid=44 (d=0.837)
- **Level-wise**: Explored BOTH subtrees → collected all leaf centroids → globally sorted → cid=38 (d=0.668) is true closest → applied epsilon filter (0.668 + 0.05 = 0.718) → cid=44 (d=0.837 > 0.718) filtered out → returned only cid=38

Key insight: A subtree with a closer interior centroid can have more distant leaf centroids, and vice versa. The hierarchical routing is approximate — the interior centroid is a rough summary, not a guarantee.

### The Permanent Loss Mechanism

In `LSMVCTreeSearchCursor.doOpen()` (lines 275-298):

1. DFS initializes first → `initializeClusterIterator()` returns cid=44, advances `nextIndex` from 0→1
2. Level-wise computes first cluster → returns cid=38 (different from DFS)
3. Code detects conflict → re-opens ALL cursors to level-wise's cid=38
4. **cid=44 is permanently lost** because:
   - `initializeClusterIterator` consumed index 0 (nextIndex is now 1)
   - The centroid was NOT marked as visited (bug #1)
   - `findNextClosestCluster` later resumes from nextIndex=1, skipping index 0 forever
   - cid=44 never appears in visitedCentroidIds, never returned by DFS

## Fix (3 changes)

### 1. Mark visited in `initializeClusterIterator` (VCTreeNavigationUtils.java ~line 384)

```java
VCTreeLeafCentroid first = leafFrame_nav.nextCentroid();
state.markVisited(first.centroidId);  // <-- ADDED
return ClusterSearchResult.create(...);
```

### 2. Add `resetDfsFirstCentroid()` to VectorClusteringSearchCursor.java

```java
public void resetDfsFirstCentroid() {
    if (iteratorState != null && iteratorState.initialized && !iteratorState.stack.isEmpty()) {
        VCTreeNavigationFrame topFrame = iteratorState.stack.peek();
        if (topFrame.isLeaf && topFrame.nextIndex > 0) {
            topFrame.nextIndex--;
        }
    }
}
```

### 3. Un-mark and reset in `LSMVCTreeSearchCursor.doOpen()` (after re-opening cursors)

```java
// The DFS init centroid was discarded - un-mark it from visited
// and reset the DFS leaf frame so it can be re-discovered later
visitedSet.remove(dfsCluster.centroidId);
firstCursor.resetDfsFirstCentroid();
```

## Verification

After fix: All 50 centroids (cid=9-58) appear in the visited set at DFS exhaustion across all 4 partitions. Part 3 tests pass with 0 missing PKs.

## Files Modified

- `hyracks-storage-am-btree/.../vector/utils/VCTreeNavigationUtils.java` — markVisited in initializeClusterIterator
- `hyracks-storage-am-btree/.../vector/impls/VectorClusteringSearchCursor.java` — added resetDfsFirstCentroid()
- `hyracks-storage-am-lsm-btree/.../lsm/vector/impls/LSMVCTreeSearchCursor.java` — un-mark + reset on conflict
