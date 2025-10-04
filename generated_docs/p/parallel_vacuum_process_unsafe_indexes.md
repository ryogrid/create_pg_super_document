# parallel_vacuum_process_unsafe_indexes

## Location
[src/backend/commands/vacuumparallel.c:826-862](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuumparallel.c#L826-L862)

## Overview
This static function handles vacuum processing of indexes that cannot be safely processed in parallel, executed exclusively by the leader process.

## Definition
```c
static void parallel_vacuum_process_unsafe_indexes(ParallelVacuumState *pvs)
```

## Detailed Description
This function complements parallel_vacuum_process_safe_indexes by handling indexes that are not suitable for parallel processing. These may include indexes that don't support parallel vacuum operations, indexes smaller than the size threshold, or indexes that are unsafe for parallel processing under current conditions (such as certain cleanup scenarios).

The function operates sequentially on the leader process only, processing each unsafe index one by one. It maintains consistency with the parallel processing model by participating in the active worker count for vacuum delay calculations, ensuring proper cost-based vacuum delay behavior across the entire operation.

Unlike the parallel-safe index processing, this function uses a simple sequential loop rather than atomic work distribution, since only the leader process executes this code path.

## Parameters / Member Variables
- `pvs`: ParallelVacuumState pointer containing the shared state and index information for the parallel vacuum operation

## Dependencies
- Functions called/Symbols referenced:
  - IsParallelWorker
  - [pg_atomic_add_fetch_u32](pg_atomic_add_fetch_u32.md)
  - [pg_atomic_sub_fetch_u32](pg_atomic_sub_fetch_u32.md)
  - [parallel_vacuum_process_one_index](parallel_vacuum_process_one_index.md)
  - VacuumActiveNWorkers
  - [PVIndStats](../P/PVIndStats.md)
- Called from (representative examples):
  - [parallel_vacuum_process_all_indexes](parallel_vacuum_process_all_indexes.md)

## Notes and Other Information
- Must only be called by the leader process (enforced by Assert(!IsParallelWorker()))
- Processes indexes marked with parallel_workers_can_process = false
- Handles indexes that fell below the size cutoff from parallel_vacuum_compute_workers()
- Handles indexes that are not parallel-safe for the current operation type (vacuum vs. cleanup)
- Manages active worker count to participate in cost-based vacuum delay calculations
- Executes before parallel workers start processing safe indexes
- Uses sequential processing since parallelization is not safe for these indexes
- Essential for ensuring all indexes are processed even when they can't participate in parallel operations

## Simplified Source

```c
static void
parallel_vacuum_process_unsafe_indexes(ParallelVacuumState *pvs)
{
    Assert(!IsParallelWorker());

    // Register as active worker for vacuum delay calculations
    if (VacuumActiveNWorkers)
        pg_atomic_add_fetch_u32(VacuumActiveNWorkers, 1);

    // Process each unsafe index sequentially
    for (int i = 0; i < pvs->nindexes; i++) {
        PVIndStats *indstats = &(pvs->indstats[i]);

        // Skip indexes that are safe for parallel processing
        // (these will be handled by parallel_vacuum_process_safe_indexes)
        if (indstats->parallel_workers_can_process)
            continue;

        // Process this unsafe index
        parallel_vacuum_process_one_index(pvs, pvs->indrels[i], indstats);
    }

    // Unregister as active worker
    if (VacuumActiveNWorkers)
        pg_atomic_sub_fetch_u32(VacuumActiveNWorkers, 1);
}
```