# parallel_vacuum_process_safe_indexes

## Location
src/backend/commands/vacuumparallel.c: 772 - 825

## Overview
This static function processes indexes that are safe for parallel vacuum operations, used by both the leader process and parallel worker processes to vacuum indexes concurrently.

## Definition
```c
static void parallel_vacuum_process_safe_indexes(ParallelVacuumState *pvs)
```

## Detailed Description
This function implements the core work distribution mechanism for parallel index vacuum operations. It uses atomic operations to coordinate between multiple worker processes (including the leader) to safely distribute index processing work. Each worker (including the leader process acting as a worker) calls this function to participate in the parallel processing loop.

The function implements a simple but effective work-stealing pattern: workers atomically increment a shared index counter to claim the next available index for processing. Only indexes marked as safe for parallel processing are actually processed; unsafe indexes are skipped as they are handled separately by the leader process in parallel_vacuum_process_unsafe_indexes().

The function also manages the active worker count for vacuum delay calculations, ensuring proper cost-based vacuum delay sharing among all active workers.

## Parameters / Member Variables
- `pvs`: ParallelVacuumState pointer containing the shared state and index information for the parallel vacuum operation

## Dependencies
- Functions called/Symbols referenced:
  - pg_atomic_add_fetch_u32
  - pg_atomic_fetch_add_u32
  - pg_atomic_sub_fetch_u32
  - parallel_vacuum_process_one_index
  - VacuumActiveNWorkers
  - PVIndStats
- Called from (representative examples):
  - parallel_vacuum_process_all_indexes
  - parallel_vacuum_main

## Notes and Other Information
- Used by both leader and worker processes - this is the main work distribution function for parallel vacuum
- Implements atomic work-stealing pattern for load balancing across workers
- Only processes indexes marked with parallel_workers_can_process = true
- Manages active worker count for proper vacuum delay cost sharing
- Continues processing until all indexes in the array have been claimed (idx >= pvs->nindexes)
- Thread-safe due to atomic operations for index claiming and worker count management
- The leader process participates as a worker after handling unsafe indexes separately