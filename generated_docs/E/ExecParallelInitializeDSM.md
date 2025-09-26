# ExecParallelInitializeDSM

## Location
[src/backend/executor/execParallel.c:438-534](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execParallel.c#L438-L534)

## Overview
ExecParallelInitializeDSM recursively traverses a plan tree and initializes the dynamic shared memory (DSM) segment for each plan node that requires shared memory coordination in parallel query execution.

## Definition

```c
static bool
ExecParallelInitializeDSM(PlanState *planstate,
						  ExecParallelInitializeDSMContext *d)
```
## Detailed Description
This function is a static helper that performs a depth-first traversal of the plan state tree to initialize DSM segments for parallel-aware plan nodes. It serves as the core coordination mechanism for setting up shared memory structures that will be used across multiple parallel worker processes.

The function operates in two phases:
1. **Node-specific initialization**: For each plan node type, it calls the appropriate DSM initialization function if the node is parallel-aware
2. **Tree traversal**: It recursively processes child nodes using planstate_tree_walker

Key behaviors include:
- Tracking instrumentation slots for performance monitoring when enabled
- Counting nodes for resource allocation
- Dispatching to specialized DSM initializers based on node type (SeqScan, IndexScan, Hash, Sort, etc.)
- Some nodes (Hash, Sort, IncrementalSort, Agg, Memoize) initialize DSM even when not parallel-aware to support EXPLAIN ANALYZE

The function allows each plan node to allocate shared memory space and insert keys into the shared memory table of contents (shm_toc) before parallel workers are launched.

## Parameters / Member Variables
- : The current plan state node being processed in the tree traversal
- : Context structure containing DSM initialization state including:
  - : Parallel context with shared memory table of contents
  - : Optional instrumentation data for performance tracking
  - : Counter for tracking number of processed nodes

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag (to determine plan node type)
  - [ExecSeqScanInitializeDSM](ExecSeqScanInitializeDSM.md), ExecIndexScanInitializeDSM, ExecIndexOnlyScanInitializeDSM
  - [ExecForeignScanInitializeDSM](ExecForeignScanInitializeDSM.md), ExecAppendInitializeDSM, ExecCustomScanInitializeDSM
  - [ExecBitmapHeapInitializeDSM](ExecBitmapHeapInitializeDSM.md), ExecHashJoinInitializeDSM, ExecHashInitializeDSM
  - [ExecSortInitializeDSM](ExecSortInitializeDSM.md), ExecIncrementalSortInitializeDSM, ExecAggInitializeDSM
  - [ExecMemoizeInitializeDSM](ExecMemoizeInitializeDSM.md)
  - planstate_tree_walker (for recursive traversal)
- Called from:
  - [ExecInitParallelPlan](ExecInitParallelPlan.md) (main parallel plan initialization)
  - Recursively calls itself via planstate_tree_walker

## Notes and Other Information
- This is a static function internal to execParallel.c
- The function returns false if planstate is NULL, otherwise continues traversal
- [Node](../N/Node.md) counting and instrumentation setup occur before type-specific initialization
- The recursive nature ensures all nodes in the plan tree are properly initialized for parallel execution
- [Hash](../H/Hash.md), Sort, IncrementalSort, Agg, and Memoize nodes always initialize DSM regardless of parallel_aware flag to support EXPLAIN ANALYZE functionality