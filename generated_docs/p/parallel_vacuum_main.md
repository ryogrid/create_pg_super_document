# parallel_vacuum_main

## Location
[src/backend/commands/vacuumparallel.c:987-1104](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuumparallel.c#L987-L1104)

## Overview
Main entry point for parallel vacuum worker processes, responsible for setting up the worker environment and processing indexes assigned to this worker.

## Definition

```c
void
parallel_vacuum_main(dsm_segment *seg, shm_toc *toc)
```
## Detailed Description
This function serves as the main execution routine for parallel vacuum worker processes. It initializes the worker environment by setting up shared memory access, opening relations and indexes, configuring vacuum cost parameters, and establishing error handling. The function then processes the indexes assigned to this worker through parallel vacuum operations.

Key responsibilities include:

**Environment Setup**:
- Validates worker process status (must have PROC_IN_VACUUM flag)
- Sets up shared memory access for coordination with leader and other workers
- Opens the target table and all its indexes with appropriate lock modes
- Configures maintenance_work_mem from shared settings

**Cost-Based Vacuum Delay Configuration**:
- Initializes vacuum cost tracking variables
- Sets up shared cost balance for coordination between workers
- Creates buffer access strategy for this worker

**Worker State Initialization**:
- Populates ParallelVacuumState structure with shared data
- Attaches to shared TidStore for dead tuple information
- Sets up error context for meaningful error reporting

**Index Processing**:
- Processes safe indexes through 
- Tracks buffer and WAL usage during execution
- Reports progress back to shared structures

**Cleanup**:
- Detaches from shared memory structures
- Closes relations and indexes with proper lock modes
- Frees allocated resources

## Parameters / Member Variables
- `*seg`: DSM segment containing shared memory for parallel vacuum coordination
- `*toc`: Shared memory table of contents for locating different data structures
## Dependencies
- Functions called/Symbols referenced:
  - [shm_toc_lookup](../s/shm_toc_lookup.md)
  - [table_open](../t/table_open.md)
  - [vac_open_indexes](../v/vac_open_indexes.md)
  - [TidStoreAttach](../T/TidStoreAttach.md)
  - [VacuumUpdateCosts](../V/VacuumUpdateCosts.md)
  - [GetAccessStrategyWithSize](../G/GetAccessStrategyWithSize.md)
  - [parallel_vacuum_error_callback](parallel_vacuum_error_callback.md)
  - [InstrStartParallelQuery](../I/InstrStartParallelQuery.md)
  - [parallel_vacuum_process_safe_indexes](parallel_vacuum_process_safe_indexes.md)
  - [InstrEndParallelQuery](../I/InstrEndParallelQuery.md)
  - [TidStoreDetach](../T/TidStoreDetach.md)
  - [vac_close_indexes](../v/vac_close_indexes.md)
  - [table_close](../t/table_close.md)
  - [FreeAccessStrategy](../F/FreeAccessStrategy.md)
- Called from (representative examples):
  - Background worker process entry point (via parallel worker infrastructure)

## Notes and Other Information
- This is a public function called by the parallel worker infrastructure
- Workers only perform index vacuum/cleanup operations, not heap scanning
- Each worker gets its own buffer access strategy to avoid contention
- Error context is established to provide meaningful error messages specific to the current index being processed
- Progress reporting is disabled for workers since only the leader reports overall progress
- The function assumes indexes are sorted by OID to match the leader's order
- Cost-based vacuum delay is shared among all workers to prevent overwhelming the system
- Buffer and WAL usage tracking allows the leader to aggregate statistics from all workers

## Simplified Source

```c
void
parallel_vacuum_main(dsm_segment *seg, shm_toc *toc)
{
    ParallelVacuumState pvs;
    Relation rel;
    Relation *indrels;
    PVIndStats *indstats;
    PVShared *shared;
    TidStore *dead_items;
    BufferUsage *buffer_usage;
    WalUsage *wal_usage;
    int nindexes;
    char *sharedquery;
    ErrorContextCallback errcallback;

    // Validate worker process status
    Assert(MyProc->statusFlags == PROC_IN_VACUUM);
    elog(DEBUG1, "starting parallel vacuum worker");

    // Get shared structures from DSM
    shared = (PVShared *) shm_toc_lookup(toc, PARALLEL_VACUUM_KEY_SHARED, false);
    sharedquery = shm_toc_lookup(toc, PARALLEL_VACUUM_KEY_QUERY_TEXT, true);
    debug_query_string = sharedquery;
    pgstat_report_activity(STATE_RUNNING, debug_query_string);

    // Open target table and indexes
    rel = table_open(shared->relid, ShareUpdateExclusiveLock);
    vac_open_indexes(rel, RowExclusiveLock, &nindexes, &indrels);
    Assert(nindexes > 0);

    // Configure worker memory
    if (shared->maintenance_work_mem_worker > 0)
        maintenance_work_mem = shared->maintenance_work_mem_worker;

    // Get index statistics and dead items from shared memory
    indstats = (PVIndStats *) shm_toc_lookup(toc, PARALLEL_VACUUM_KEY_INDEX_STATS, false);
    dead_items = TidStoreAttach(shared->dead_items_dsa_handle, shared->dead_items_handle);

    // Set up cost-based vacuum delay
    VacuumUpdateCosts();
    VacuumCostBalance = 0;
    VacuumPageHit = 0;
    VacuumPageMiss = 0;
    VacuumPageDirty = 0;
    VacuumCostBalanceLocal = 0;
    VacuumSharedCostBalance = &(shared->cost_balance);
    VacuumActiveNWorkers = &(shared->active_nworkers);

    // Initialize parallel vacuum state
    pvs.indrels = indrels;
    pvs.nindexes = nindexes;
    pvs.indstats = indstats;
    pvs.shared = shared;
    pvs.dead_items = dead_items;
    pvs.relnamespace = get_namespace_name(RelationGetNamespace(rel));
    pvs.relname = pstrdup(RelationGetRelationName(rel));
    pvs.heaprel = rel;
    pvs.indname = NULL;
    pvs.status = PARALLEL_INDVAC_STATUS_INITIAL;
    pvs.bstrategy = GetAccessStrategyWithSize(BAS_VACUUM,
                                              shared->ring_nbuffers * (BLCKSZ / 1024));

    // Set up error callback
    errcallback.callback = parallel_vacuum_error_callback;
    errcallback.arg = &pvs;
    errcallback.previous = error_context_stack;
    error_context_stack = &errcallback;

    // Track buffer usage and process indexes
    InstrStartParallelQuery();
    parallel_vacuum_process_safe_indexes(&pvs);

    // Report usage statistics
    buffer_usage = shm_toc_lookup(toc, PARALLEL_VACUUM_KEY_BUFFER_USAGE, false);
    wal_usage = shm_toc_lookup(toc, PARALLEL_VACUUM_KEY_WAL_USAGE, false);
    InstrEndParallelQuery(&buffer_usage[ParallelWorkerNumber],
                          &wal_usage[ParallelWorkerNumber]);

    // Cleanup
    TidStoreDetach(dead_items);
    error_context_stack = errcallback.previous;
    vac_close_indexes(nindexes, indrels, RowExclusiveLock);
    table_close(rel, ShareUpdateExclusiveLock);
    FreeAccessStrategy(pvs.bstrategy);
}
```