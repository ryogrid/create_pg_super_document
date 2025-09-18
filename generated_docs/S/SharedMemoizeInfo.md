# SharedMemoizeInfo

## Location
src/include/nodes/execnodes.h: 2257 - 2261

## Overview
SharedMemoizeInfo is a shared memory structure that aggregates memoization instrumentation data from all worker processes in parallel query execution.

## Definition
```c
typedef struct SharedMemoizeInfo
{
    int                     num_workers;
    MemoizeInstrumentation  sinstrument[FLEXIBLE_ARRAY_MEMBER];
} SharedMemoizeInfo;
```

## Detailed Description
SharedMemoizeInfo serves as a container for collecting memoization performance metrics from multiple worker processes during parallel query execution. In PostgreSQLs parallel query architecture, each worker process maintains its own local memoization cache and instrumentation data. This structure provides a centralized location in shared memory where all workers can store their instrumentation data, allowing the leader process to aggregate the metrics for comprehensive performance analysis.

The structure uses a flexible array member to accommodate a variable number of worker processes, with each worker having its own MemoizeInstrumentation entry. This design allows the system to scale to different degrees of parallelism while maintaining detailed per-worker statistics that can be combined for overall query performance analysis.

## Parameters / Member Variables
- `num_workers`: Number of worker processes participating in the parallel memoization operation
- `sinstrument`: Flexible array containing MemoizeInstrumentation structures, one for each worker process

## Dependencies
- Functions called/Symbols referenced:
  - MemoizeInstrumentation (per-worker instrumentation data)
  - FLEXIBLE_ARRAY_MEMBER (C99 flexible array member macro)
- Called from (representative examples):
  - ExecMemoizeEstimate (for shared memory size calculation)
  - ExecMemoizeInitializeDSM (for shared memory initialization)
  - ExecMemoizeRetrieveInstrumentation (for collecting worker data)
  - MemoizeState (contains reference to shared info)

## Notes and Other Information
- Used exclusively in parallel query execution contexts where multiple workers perform memoization
- The leader process aggregates data from all workers to provide comprehensive EXPLAIN output
- Memory for this structure is allocated in Dynamic Shared Memory (DSM) segments
- Each worker process writes to its own sinstrument slot to avoid contention
- The structure size is calculated dynamically based on the number of parallel workers
- Supports PostgreSQLs parallel-safe memoization operations across worker boundaries
- Data collection is coordinated through the parallel query execution framework
- Final aggregated metrics represent the combined effectiveness of memoization across all workers