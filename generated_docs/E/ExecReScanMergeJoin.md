# ExecReScanMergeJoin

## Location
[src/backend/executor/nodeMergejoin.c:1657-1678](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMergejoin.c#L1657-L1678)

## Overview
Resets a merge join node to its initial state and optionally rescans its child plans, enabling the merge join to be re-executed from the beginning.

## Definition
```c
void ExecReScanMergeJoin(MergeJoinState *node)
```

## Detailed Description
ExecReScanMergeJoin implements the rescan capability for merge join nodes, which is essential for supporting nested loop joins that have merge joins as inner plans, correlated subqueries, and other scenarios where a plan node needs to be executed multiple times. The function performs a comprehensive reset of the merge join's internal state while intelligently handling child plan rescanning.

The rescan process involves several key operations:
- **State reset**: Resets the join state machine to EXEC_MJ_INITIALIZE_OUTER, effectively starting the merge join algorithm from the beginning
- **Tuple slot cleanup**: Clears the marked tuple slot and resets all tuple slot references to NULL
- **Match status reset**: Resets the MatchedOuter and MatchedInner flags that track whether tuples have found join partners
- **Conditional child rescanning**: Only rescans child plans if their parameters haven't changed, relying on the executor's change parameter tracking mechanism

The function implements an optimization where child plans are only rescanned when necessary. If a child plan's chgParam field is non-null, it indicates that the plan will be automatically rescanned when first accessed due to parameter changes, so an explicit rescan is unnecessary.

## Parameters / Member Variables
- `node`: Pointer to the MergeJoinState structure representing the merge join node to be rescanned

## Dependencies
- Functions called/Symbols referenced:
  - outerPlanState/innerPlanState (to access child plan nodes)
  - ExecClearTuple (to clear the marked tuple slot)
  - ExecReScan (to rescan child plans when needed)
  - EXEC_MJ_INITIALIZE_OUTER (initial state constant)
- Called from (representative examples):
  - ExecReScan (executor rescan dispatcher)

## Notes and Other Information
- Essential for supporting nested loops with merge join inner plans
- Optimizes child plan rescanning by checking chgParam to avoid unnecessary work
- Does not reset expression contexts, which are managed by the executor framework
- The marked tuple slot is explicitly cleared since it may contain stale data from previous execution
- Tuple slot references are set to NULL to ensure fresh tuples are fetched on re-execution
- Part of the standard executor node interface along with Init, Execute, and End functions
- Critical for correct behavior in parameterized and correlated query scenarios
- Must preserve the node's configuration while resetting execution state