# SharedIncrementalSortInfo

## Location
src/include/nodes/execnodes.h: 2371 - 2375

## Overview
SharedIncrementalSortInfo is a shared memory structure used to collect and aggregate incremental sort instrumentation data from multiple parallel workers.

## Definition


## Detailed Description
This structure serves as a shared memory container for collecting incremental sort performance data from parallel workers in PostgreSQL's parallel query execution. When incremental sort operations are executed in parallel, each worker process generates its own IncrementalSortInfo data. The SharedIncrementalSortInfo structure aggregates this data from all workers, allowing the main process to retrieve comprehensive performance statistics for the entire parallel operation.

The structure uses a flexible array member to store a variable number of IncrementalSortInfo structures, one for each worker that participated in the parallel incremental sort operation. This design allows for efficient sharing of instrumentation data across process boundaries in PostgreSQL's shared memory architecture.

## Parameters / Member Variables
- : The number of parallel workers that participated in the incremental sort operation
- : A flexible array of IncrementalSortInfo structures, one for each worker, containing their individual performance metrics

## Dependencies
- Functions called/Symbols referenced:
  - [IncrementalSortInfo](../I/IncrementalSortInfo.md) (struct type for the array member)
  - FLEXIBLE_ARRAY_MEMBER (macro for variable-length array declaration)
- Called from (representative examples):
  - [ExecIncrementalSortEstimate](../E/ExecIncrementalSortEstimate.md) (src/backend/executor/nodeIncrementalSort.c:1182)
  - [ExecIncrementalSortInitializeDSM](../E/ExecIncrementalSortInitializeDSM.md) (src/backend/executor/nodeIncrementalSort.c:1202)
  - [ExecIncrementalSortRetrieveInstrumentation](../E/ExecIncrementalSortRetrieveInstrumentation.md) (src/backend/executor/nodeIncrementalSort.c:1236, 1241)
  - [IncrementalSortState](../I/IncrementalSortState.md) (src/include/nodes/execnodes.h:2409)

## Notes and Other Information
- This structure is specifically designed for parallel query execution scenarios where multiple workers contribute to incremental sort operations
- The flexible array member allows for dynamic sizing based on the actual number of workers at runtime
- Used primarily for DSM (Dynamic Shared Memory) operations in parallel execution contexts
- The structure facilitates aggregation of performance metrics from distributed workers for comprehensive execution analysis
- Essential for providing accurate EXPLAIN ANALYZE output in parallel incremental sort operations