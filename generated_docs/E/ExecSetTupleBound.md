# ExecSetTupleBound

## Location
src/backend/executor/execProcnode.c: 843 - 982

## Overview
ExecSetTupleBound propagates tuple count limits down through a PostgreSQL query plan tree to enable performance optimizations in child nodes that can benefit from knowing the maximum number of tuples their parent will consume.

## Definition
```c
void ExecSetTupleBound(int64 tuples_needed, PlanState *child_node)
```

## Detailed Description
ExecSetTupleBound implements a sophisticated optimization technique in PostgreSQL's query executor by propagating tuple bound information from parent nodes to their children. This allows child nodes to optimize their execution strategies when they know in advance that their parent will only consume a limited number of tuples.

The function employs a selective propagation strategy:

1. **Sort Optimization**: For SortState and IncrementalSortState nodes, it enables bounded sorting algorithms that can terminate early when the required number of tuples is reached, significantly improving performance for TOP-N style queries.

2. **Parallel Node Optimization**: For AppendState, MergeAppendState, GatherState, and GatherMergeState nodes, it propagates the bound to all child plans since none of the individual children need to produce more tuples than the parent requires.

3. **Transparent Propagation**: For ResultState nodes (without quals) and SubqueryScanState nodes (without quals), it transparently passes the bound through since these nodes don't modify the tuple count.

4. **Conservative Approach**: The function only propagates bounds through nodes that are guaranteed not to discard or combine tuples, ensuring correctness by stopping propagation at nodes that might change the tuple count relationship.

The bound can only be set between scans (after initialization or before ExecReScan) to maintain consistency, and negative values indicate "no limit".

## Parameters / Member Variables
- `tuples_needed`: The maximum number of tuples the parent node will consume; negative values mean unlimited
- `child_node`: PlanState pointer to the child node that should be informed of the tuple bound

## Dependencies
- Functions called/Symbols referenced:
  - IsA (node type checking macro)
  - outerPlanState (access to child plan state)
  - [ExecSetTupleBound](ExecSetTupleBound.md) (recursive calls for tree propagation)
  - [SortState](../S/SortState.md), IncrementalSortState (sort node state structures)
  - [AppendState](../A/AppendState.md), MergeAppendState (append node state structures)
  - [ResultState](../R/ResultState.md), SubqueryScanState (scan node state structures)
  - [GatherState](../G/GatherState.md), GatherMergeState (parallel execution state structures)
- Called from (representative examples):
  - [ParallelQueryMain](../P/ParallelQueryMain.md) (parallel worker setup)
  - [recompute_limits](../r/recompute_limits.md) (LIMIT node optimization)
  - EvalPlanQualSetSlot (EPQ optimization)
  - [ExecSetTupleBound](ExecSetTupleBound.md) (recursive propagation)

## Notes and Other Information
- Critical optimization for TOP-N queries and LIMIT operations where early termination can save significant work
- Particularly effective with Sort nodes where bounded sorting algorithms (like top-k heaps) provide substantial performance improvements
- Essential for parallel query optimization where worker processes can be informed of tuple limits
- The recursive nature handles complex plan trees but stops propagation at nodes that might filter or combine tuples
- Must be called with the same set of nodes each time to maintain consistency during plan rescans
- Conservative design ensures correctness by only propagating bounds where it's guaranteed to be safe
- Integrates with PostgreSQL's parameter-change mechanism for dynamic query optimization
- Stack depth checking is omitted as an optimization since earlier initialization traversals would have consumed more stack