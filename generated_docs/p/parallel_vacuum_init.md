# parallel_vacuum_init

## Location
[src/backend/commands/vacuumparallel.c:242-433](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuumparallel.c#L242-L433)

## Overview
Initializes parallel vacuum execution by creating a parallel context, setting up shared memory state, and preparing coordination structures for workers to process vacuum operations on indexes concurrently.

## Definition

```c
ParallelVacuumState *
parallel_vacuum_init(Relation rel, Relation *indrels, int nindexes,
					 int nrequested_workers, int vac_work_mem,
					 int elevel, BufferAccessStrategy bstrategy)
```
## Detailed Description
This function sets up the infrastructure for parallel vacuum operations by:

1. **Worker Computation**: Determines the optimal number of parallel workers based on index characteristics and resource constraints using 
2. **Parallel Context Creation**: Enters parallel mode and creates a parallel context for worker coordination
3. **Shared Memory Setup**: Allocates and initializes shared memory segments for:
   - Index vacuum statistics ()
   - Shared state information () 
   - Dead tuple storage via 
   - Buffer and WAL usage tracking
   - Query text for worker processes
4. **Resource Management**: Configures maintenance work memory distribution among workers and buffer access strategy
5. **Capability Assessment**: Counts indexes supporting different parallel vacuum phases (bulkdel, cleanup, conditional cleanup)

The function returns  if parallel vacuum cannot be performed (e.g., no suitable workers can be allocated).

## Parameters / Member Variables
- `rel`: The heap relation being vacuumed
- `*indrels`: Array of index relations to be processed in parallel
- `nindexes`: Number of indexes in the  array
- `nrequested_workers`: Desired number of parallel workers
- `vac_work_mem`: Memory limit for dead tuple storage (in KB)
- `elevel`: Error level for reporting issues
- `bstrategy`: Buffer access strategy for I/O operations
## Dependencies
- Functions called/Symbols referenced:
  -  - Determines optimal worker count
  -  - Enables parallel execution mode
  -  - Creates parallel worker context
  -  - Creates shared dead tuple storage
  -  /  - Gets handles for shared TidStore
  -  - Initializes dynamic shared memory
  -  functions - Shared memory table-of-contents management
- Called from (representative examples):
  -  (src/backend/access/heap/vacuumlazy.c:2853)

## Notes and Other Information
- Function is located at src/backend/commands/vacuumparallel.c:242-433
- Requires at least one index and non-negative worker request to proceed
- Automatically calculates per-worker maintenance work memory based on indexes using maintenance work memory
- Sets up atomic counters for cost balancing and worker coordination
- Handles both conditional and unconditional parallel cleanup modes for indexes
- Memory allocation uses palloc0 for zero-initialized structures

## Simplified Source

```c
ParallelVacuumState *
parallel_vacuum_init(Relation rel, Relation *indrels, int nindexes,
                     int nrequested_workers, int vac_work_mem,
                     int elevel, BufferAccessStrategy bstrategy)
{
    ParallelVacuumState *pvs;
    ParallelContext *pcxt;
    PVShared *shared;
    TidStore *dead_items;
    bool *will_parallel_vacuum;
    int parallel_workers = 0;

    // Validate inputs
    Assert(nrequested_workers >= 0);
    Assert(nindexes > 0);

    // Determine which indexes can participate and how many workers needed
    will_parallel_vacuum = (bool *) palloc0(sizeof(bool) * nindexes);
    parallel_workers = parallel_vacuum_compute_workers(indrels, nindexes,
                                                      nrequested_workers,
                                                      will_parallel_vacuum);
    if (parallel_workers <= 0) {
        // No parallel workers available
        pfree(will_parallel_vacuum);
        return NULL;
    }

    // Create parallel vacuum state
    pvs = (ParallelVacuumState *) palloc0(sizeof(ParallelVacuumState));
    pvs->indrels = indrels;
    pvs->nindexes = nindexes;
    pvs->will_parallel_vacuum = will_parallel_vacuum;
    pvs->bstrategy = bstrategy;
    pvs->heaprel = rel;

    // Enter parallel mode and create parallel context
    EnterParallelMode();
    pcxt = CreateParallelContext("postgres", "parallel_vacuum_main", parallel_workers);
    pvs->pcxt = pcxt;

    // Estimate and allocate shared memory segments
    // - Index statistics, shared state, buffer/WAL usage, query text
    Size est_indstats_len = mul_size(sizeof(PVIndStats), nindexes);
    Size est_shared_len = sizeof(PVShared);

    // Set up shared memory table of contents
    shm_toc_estimate_chunk(&pcxt->estimator, est_indstats_len);
    shm_toc_estimate_keys(&pcxt->estimator, 1);
    // ... similar estimates for other shared data ...

    InitializeParallelDSM(pcxt);

    // Initialize index statistics in shared memory
    PVIndStats *indstats = (PVIndStats *) shm_toc_allocate(pcxt->toc, est_indstats_len);
    MemSet(indstats, 0, est_indstats_len);

    // Count indexes supporting different parallel phases
    for (int i = 0; i < nindexes; i++) {
        if (!will_parallel_vacuum[i]) continue;

        uint8 vacoptions = indrels[i]->rd_indam->amparallelvacuumoptions;
        if (vacoptions & VACUUM_OPTION_PARALLEL_BULKDEL)
            pvs->nindexes_parallel_bulkdel++;
        if (vacoptions & VACUUM_OPTION_PARALLEL_CLEANUP)
            pvs->nindexes_parallel_cleanup++;
        if (vacoptions & VACUUM_OPTION_PARALLEL_COND_CLEANUP)
            pvs->nindexes_parallel_condcleanup++;
    }
    pvs->indstats = indstats;

    // Set up shared coordination state
    shared = (PVShared *) shm_toc_allocate(pcxt->toc, est_shared_len);
    MemSet(shared, 0, est_shared_len);
    shared->relid = RelationGetRelid(rel);
    shared->elevel = elevel;
    shared->dead_items_info.max_bytes = vac_work_mem * 1024L;

    // Create shared dead items storage
    dead_items = TidStoreCreateShared(shared->dead_items_info.max_bytes,
                                     LWTRANCHE_PARALLEL_VACUUM_DSA);
    pvs->dead_items = dead_items;
    shared->dead_items_handle = TidStoreGetHandle(dead_items);
    shared->dead_items_dsa_handle = dsa_get_handle(TidStoreGetDSA(dead_items));

    // Initialize atomic counters for coordination
    pg_atomic_init_u32(&(shared->cost_balance), 0);
    pg_atomic_init_u32(&(shared->active_nworkers), 0);
    pg_atomic_init_u32(&(shared->idx), 0);

    pvs->shared = shared;

    // Set up buffer and WAL usage tracking for workers
    BufferUsage *buffer_usage = shm_toc_allocate(pcxt->toc,
                                                mul_size(sizeof(BufferUsage), pcxt->nworkers));
    WalUsage *wal_usage = shm_toc_allocate(pcxt->toc,
                                          mul_size(sizeof(WalUsage), pcxt->nworkers));
    pvs->buffer_usage = buffer_usage;
    pvs->wal_usage = wal_usage;

    return pvs;
}
```