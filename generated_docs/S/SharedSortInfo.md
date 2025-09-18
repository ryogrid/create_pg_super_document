# SharedSortInfo

## Location
[src/include/nodes/execnodes.h:2322-2326](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L2322-L2326)

## Overview
SharedSortInfo is a shared memory container that holds per-worker sort instrumentation information for parallel sort operations in PostgreSQL.

## Definition


## Detailed Description
SharedSortInfo serves as a shared memory structure for coordinating and collecting instrumentation data from multiple worker processes during parallel sort operations. It maintains an array of TuplesortInstrumentation structures, with one entry per worker process, allowing the system to aggregate performance statistics and monitoring data across all parallel workers involved in a sort operation. The structure uses a flexible array member to accommodate varying numbers of workers dynamically.

## Parameters / Member Variables
- : Number of worker processes participating in the parallel sort operation
- : Flexible array of TuplesortInstrumentation structures, one per worker, containing detailed instrumentation data for each worker's sort operation

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER
  - TuplesortInstrumentation
- Called from (representative examples):
  - [ExecSortEstimate](../E/ExecSortEstimate.md)
  - [ExecSortInitializeDSM](../E/ExecSortInitializeDSM.md)
  - [ExecSortRetrieveInstrumentation](../E/ExecSortRetrieveInstrumentation.md)
  - [SortState](SortState.md)

## Notes and Other Information
SharedSortInfo is essential for parallel query execution where multiple workers perform sorting operations concurrently. The flexible array design allows it to scale with the number of available workers. This structure enables PostgreSQL to collect comprehensive performance metrics from all workers and present unified statistics for the overall parallel sort operation. It's particularly important for query optimization and performance monitoring in parallel execution environments.