# ExecParallelEstimate

## Location
[src/backend/executor/execParallel.c:229-309](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execParallel.c#L229-L309)

## Overview
Traverses a plan state tree to estimate shared memory requirements for parallel execution and counts instrumentation nodes for parallel query coordination.

## Definition
```c
static bool ExecParallelEstimate(PlanState *planstate, ExecParallelEstimateContext *e)
```

## Detailed Description
ExecParallelEstimate performs a recursive traversal of the plan state tree to accomplish two main objectives:

1. **Shared Memory Estimation**: For parallel-aware plan nodes, it calls their specific estimation functions to determine how much shared memory they need for coordination between parallel workers
2. **Node Counting**: Increments a counter for each plan state node encountered, which is used to allocate the correct number of Instrumentation structures for EXPLAIN ANALYZE in parallel queries

The function uses a switch statement to handle different node types, calling the appropriate estimation function for each parallel-aware node type. Some nodes (Hash, Sort, IncrementalSort, Agg, Memoize) are processed even when not parallel-aware to support EXPLAIN ANALYZE instrumentation.

The function operates as a tree walker, recursively processing all nodes in the plan state tree through the planstate_tree_walker mechanism.

## Parameters / Member Variables
- `planstate`: The current PlanState node being processed in the tree traversal
- `e`: Context structure containing the parallel context and node counter

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag
  - ExecSeqScanEstimate
  - ExecIndexScanEstimate  
  - ExecIndexOnlyScanEstimate
  - ExecForeignScanEstimate
  - ExecAppendEstimate
  - ExecCustomScanEstimate
  - ExecBitmapHeapEstimate
  - ExecHashJoinEstimate
  - ExecHashEstimate
  - ExecSortEstimate
  - ExecIncrementalSortEstimate
  - ExecAggEstimate
  - ExecMemoizeEstimate
  - planstate_tree_walker
- Called from:
  - ExecInitParallelPlan
  - ExecParallelEstimate (recursive)

## Notes and Other Information
- The function is designed to work with PostgreSQL's parallel query execution framework
- Only parallel-aware nodes contribute to shared memory estimation, except for certain nodes needed for EXPLAIN ANALYZE
- The recursive nature allows it to handle complex nested plan structures
- The node count is used later to allocate instrumentation arrays for performance monitoring
- Located in src/backend/executor/execParallel.c:229-309