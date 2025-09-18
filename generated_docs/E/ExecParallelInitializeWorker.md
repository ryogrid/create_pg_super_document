# ExecParallelInitializeWorker

## Location
src/backend/executor/execParallel.c: 1309 - 1399

## Overview
Initializes PlanState nodes and their descendants with information retrieved from shared memory for parallel worker execution.

## Definition
```c
static bool ExecParallelInitializeWorker(PlanState *planstate, ParallelWorkerContext *pwcxt)
```

## Detailed Description
This static function is a critical component of PostgreSQL's parallel query execution system that initializes plan state nodes for parallel workers. It performs a type-based dispatch to call appropriate initialization functions for different node types that support parallel execution. The function distinguishes between nodes that are parallel-aware (check the parallel_aware flag) and those that need initialization even when not parallel-aware (primarily for EXPLAIN ANALYZE support).

The function uses a switch statement based on the nodeTag to identify the specific plan node type and calls the corresponding worker initialization function. It handles various scan types (SeqScan, IndexScan, etc.), join operations (HashJoin), and utility operations (Sort, Agg, etc.). Some nodes like HashState, SortState, and AggState are initialized regardless of parallel_aware status to support instrumentation collection.

## Parameters / Member Variables
- `planstate`: PlanState node to initialize for parallel execution
- `pwcxt`: Parallel worker context containing shared memory information and worker-specific data

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag
  - planstate_tree_walker
  - ExecSeqScanInitializeWorker
  - ExecIndexScanInitializeWorker
  - ExecIndexOnlyScanInitializeWorker
  - ExecForeignScanInitializeWorker
  - ExecAppendInitializeWorker
  - ExecCustomScanInitializeWorker
  - ExecBitmapHeapInitializeWorker
  - ExecHashJoinInitializeWorker
  - ExecHashInitializeWorker
  - ExecSortInitializeWorker
  - ExecIncrementalSortInitializeWorker
  - ExecAggInitializeWorker
  - ExecMemoizeInitializeWorker
- Types used:
  - PlanState (and various specific subtypes)
  - ParallelWorkerContext
- Called from:
  - ParallelQueryMain
  - ExecParallelInitializeWorker (recursive self-call via planstate_tree_walker)

## Notes and Other Information
- This is a static function, only accessible within execParallel.c
- Must be called after ExecutorStart() has allocated and initialized the PlanState
- The function checks the parallel_aware flag for most node types to determine if parallel-specific initialization is needed
- Some nodes (Hash, Sort, IncrementalSort, Agg, Memoize) are always initialized to support EXPLAIN ANALYZE even in non-parallel contexts
- Uses planstate_tree_walker for recursive traversal to ensure all nodes in the plan tree are properly initialized
- Part of the parallel query infrastructure that enables workers to properly execute their assigned portions of the query plan
- The recursive nature ensures that complex nested plans are fully initialized across all worker processes