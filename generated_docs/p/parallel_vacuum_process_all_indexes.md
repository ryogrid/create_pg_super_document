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

## Simplified Source

```c
static void
parallel_vacuum_process_all_indexes(ParallelVacuumState *pvs, int num_index_scans,
                                   bool vacuum)
{
    int nworkers;
    PVIndVacStatus new_status;

    Assert(!IsParallelWorker());

    // Determine operation type and worker count needed
    if (vacuum) {
        new_status = PARALLEL_INDVAC_STATUS_NEED_BULKDELETE;
        nworkers = pvs->nindexes_parallel_bulkdel;
    } else {
        new_status = PARALLEL_INDVAC_STATUS_NEED_CLEANUP;
        nworkers = pvs->nindexes_parallel_cleanup;

        // Add conditional cleanup indexes on first scan only
        if (num_index_scans == 0)
            nworkers += pvs->nindexes_parallel_condcleanup;
    }

    // Leader participates, so reduce worker count
    nworkers--;
    nworkers = Min(nworkers, pvs->pcxt->nworkers);

    // Set up index status for this processing phase
    for (int i = 0; i < pvs->nindexes; i++) {
        PVIndStats *indstats = &(pvs->indstats[i]);
        indstats->status = new_status;
        indstats->parallel_workers_can_process =
            (pvs->will_parallel_vacuum[i] &&
             parallel_vacuum_index_is_parallel_safe(pvs->indrels[i],
                                                   num_index_scans, vacuum));
    }

    // Reset progress counter
    pg_atomic_write_u32(&(pvs->shared->idx), 0);

    // Launch parallel workers if needed
    if (nworkers > 0) {
        // Reinitialize for subsequent scans
        if (num_index_scans > 0)
            ReinitializeParallelDSM(pvs->pcxt);

        // Set up cost-based vacuum delay sharing
        pg_atomic_write_u32(&(pvs->shared->cost_balance), VacuumCostBalance);
        pg_atomic_write_u32(&(pvs->shared->active_nworkers), 0);

        ReinitializeParallelWorkers(pvs->pcxt, nworkers);
        LaunchParallelWorkers(pvs->pcxt);

        // Enable shared cost balance for leader if workers launched
        if (pvs->pcxt->nworkers_launched > 0) {
            VacuumCostBalance = 0;
            VacuumCostBalanceLocal = 0;
            VacuumSharedCostBalance = &(pvs->shared->cost_balance);
            VacuumActiveNWorkers = &(pvs->shared->active_nworkers);
        }
    }

    // Process unsafe indexes on leader first
    parallel_vacuum_process_unsafe_indexes(pvs);

    // Leader joins workers to process safe indexes
    parallel_vacuum_process_safe_indexes(pvs);

    // Wait for all workers and collect statistics
    if (nworkers > 0) {
        WaitForParallelWorkersToFinish(pvs->pcxt);

        for (int i = 0; i < pvs->pcxt->nworkers_launched; i++)
            InstrAccumParallelQuery(&pvs->buffer_usage[i], &pvs->wal_usage[i]);
    }

    // Verify all indexes completed and reset status
    for (int i = 0; i < pvs->nindexes; i++) {
        PVIndStats *indstats = &(pvs->indstats[i]);

        if (indstats->status != PARALLEL_INDVAC_STATUS_COMPLETED)
            elog(ERROR, "parallel index vacuum on index \"%s\" is not completed",
                 RelationGetRelationName(pvs->indrels[i]));

        indstats->status = PARALLEL_INDVAC_STATUS_INITIAL;
    }

    // Restore cost balance and disable sharing
    if (VacuumSharedCostBalance) {
        VacuumCostBalance = pg_atomic_read_u32(VacuumSharedCostBalance);
        VacuumSharedCostBalance = NULL;
        VacuumActiveNWorkers = NULL;
    }
}
```