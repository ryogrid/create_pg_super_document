# parallel_vacuum_process_all_indexes

## Location
[src/backend/commands/vacuumparallel.c:609-771](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuumparallel.c#L609-L771)

## Overview
This static function coordinates the parallel processing of all indexes during vacuum operations, managing worker distribution, task assignment, and synchronization between vacuum and cleanup phases.

## Definition
```c
static void parallel_vacuum_process_all_indexes(ParallelVacuumState *pvs, int num_index_scans,
                                               bool vacuum)
```

## Detailed Description
This function serves as the central coordinator for parallel index processing in PostgreSQL vacuum operations. It handles both vacuum (bulkdelete) and cleanup phases by determining the appropriate number of workers, setting up shared memory state, launching parallel workers, and managing the execution flow. The function distinguishes between safe and unsafe indexes for parallel processing, processes unsafe indexes on the leader process first, then coordinates parallel workers for safe indexes.

The function manages complex coordination including cost-based vacuum delay sharing, worker lifecycle management (launch, monitor, cleanup), and progress tracking. It also handles dynamic worker count adjustments between bulkdelete and cleanup phases, and ensures all indexes are properly processed before completion.

## Parameters / Member Variables
- `pvs`: ParallelVacuumState pointer containing all shared state for the parallel vacuum operation
- `num_index_scans`: The current index scan number (0 for first scan, increments for subsequent scans)
- `vacuum`: Boolean flag indicating whether this is a vacuum operation (true) or cleanup operation (false)

## Dependencies
- Functions called/Symbols referenced:
  - IsParallelWorker
  - [parallel_vacuum_index_is_parallel_safe](parallel_vacuum_index_is_parallel_safe.md)
  - [pg_atomic_write_u32](pg_atomic_write_u32.md)
  - [pg_atomic_read_u32](pg_atomic_read_u32.md)
  - [ReinitializeParallelDSM](../R/ReinitializeParallelDSM.md)
  - [ReinitializeParallelWorkers](../R/ReinitializeParallelWorkers.md)
  - [LaunchParallelWorkers](../L/LaunchParallelWorkers.md)
  - [WaitForParallelWorkersToFinish](../W/WaitForParallelWorkersToFinish.md)
  - [InstrAccumParallelQuery](../I/InstrAccumParallelQuery.md)
  - [parallel_vacuum_process_unsafe_indexes](parallel_vacuum_process_unsafe_indexes.md)
  - [parallel_vacuum_process_safe_indexes](parallel_vacuum_process_safe_indexes.md)
  - PARALLEL_INDVAC_STATUS_NEED_BULKDELETE
  - PARALLEL_INDVAC_STATUS_NEED_CLEANUP
  - PARALLEL_INDVAC_STATUS_INITIAL
  - PARALLEL_INDVAC_STATUS_COMPLETED
- Called from (representative examples):
  - [parallel_vacuum_bulkdel_all_indexes](parallel_vacuum_bulkdel_all_indexes.md)
  - [parallel_vacuum_cleanup_all_indexes](parallel_vacuum_cleanup_all_indexes.md)

## Notes and Other Information
- Must be called only by the parallel vacuum leader process (enforced by Assert(!IsParallelWorker()))
- Handles both vacuum and cleanup phases based on the 'vacuum' parameter
- Manages dynamic worker allocation - [cleanup](../c/cleanup.md) phase may include conditional cleanup indexes only on first scan
- Implements sophisticated cost-based vacuum delay sharing among workers
- Processes unsafe indexes sequentially on leader before starting parallel processing of safe indexes
- Includes comprehensive error checking to ensure all indexes are completed
- Reinitializes parallel context for multiple index scans to reuse worker processes
- Accumulates buffer and WAL usage statistics from all workers for performance monitoring