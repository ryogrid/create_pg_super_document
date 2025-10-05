# _brin_begin_parallel

## Location
[src/backend/access/brin/brin.c:2354-2537](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin.c#L2354-L2537)

## Overview
Initializes and launches parallel workers for BRIN index creation, setting up shared memory structures and coordinating parallel heap scanning and sorting operations.

## Definition
```c
static void _brin_begin_parallel(BrinBuildState *buildstate, Relation heap, Relation index, bool isconcurrent, int request)
```

## Detailed Description
This function orchestrates the setup of parallel BRIN index building by:

1. **Creating parallel context**: Establishes a parallel execution environment with the requested number of worker processes
2. **Setting up shared memory**: Allocates and initializes shared data structures including BrinShared state, shared tuplesort state, and usage tracking structures
3. **Configuring parallel table scan**: Initializes the parallel heap scan using appropriate snapshot (SnapshotAny for normal builds, MVCC snapshot for concurrent builds)
4. **Launching worker processes**: Starts the requested number of parallel workers and optionally includes the leader process as a participant
5. **Handling fallback scenarios**: Falls back to serial execution if parallel setup fails

The function creates shared memory segments for coordinating work distribution, collecting statistics, and managing the parallel sorting of BRIN tuples. It handles both regular and concurrent index builds with appropriate snapshot isolation.

## Parameters / Member Variables
- `buildstate`: BRIN build state structure that will be updated with leader information
- `heap`: The heap relation being indexed
- `index`: The BRIN index relation being built
- `isconcurrent`: Boolean flag indicating if this is a CREATE INDEX CONCURRENTLY operation
- `request`: Target number of parallel worker processes to launch

## Dependencies
- Functions called/Symbols referenced:
  - [EnterParallelMode](../E/EnterParallelMode.md) (enter parallel execution mode)
  - [CreateParallelContext](../C/CreateParallelContext.md) (create parallel worker context)
  - [_brin_parallel_estimate_shared](_brin_parallel_estimate_shared.md) (estimate shared memory needs)
  - [tuplesort_estimate_shared](../t/tuplesort_estimate_shared.md) (estimate tuplesort memory needs)
  - [InitializeParallelDSM](../I/InitializeParallelDSM.md) (initialize dynamic shared memory)
  - [table_parallelscan_initialize](../t/table_parallelscan_initialize.md) (setup parallel table scan)
  - [tuplesort_initialize_shared](../t/tuplesort_initialize_shared.md) (initialize shared tuplesort state)
  - [LaunchParallelWorkers](../L/LaunchParallelWorkers.md) (start worker processes)
  - [_brin_leader_participate_as_worker](_brin_leader_participate_as_worker.md) (leader participation)
- Called from (representative examples):
  - [brinbuild](brinbuild.md) (main BRIN index build function)

## Notes and Other Information
- Falls back to serial execution if no workers can be launched or DSM allocation fails
- Supports both leader participation and leader-only coordination modes
- Handles concurrent builds with MVCC snapshots for consistency
- Allocates shared memory for WAL usage and buffer usage tracking
- Sets up condition variables and spinlocks for worker coordination
- The function is part of PostgreSQL's parallel index building infrastructure
- If DISABLE_LEADER_PARTICIPATION is defined, the leader doesn't participate in scanning

## Simplified Source
```c
static void
_brin_begin_parallel(BrinBuildState *buildstate, Relation heap, Relation index,
                     bool isconcurrent, int request)
{
    ParallelContext *pcxt;
    int     scantuplesortstates;
    Snapshot snapshot;
    Size    estbrinshared, estsort;
    BrinShared *brinshared;
    Sharedsort *sharedsort;
    BrinLeader *brinleader = (BrinLeader *) palloc0(sizeof(BrinLeader));
    bool    leaderparticipates = true;

    // Enter parallel mode and create context
    EnterParallelMode();
    pcxt = CreateParallelContext("postgres", "_brin_parallel_build_main", request);

    scantuplesortstates = leaderparticipates ? request + 1 : request;

    // Set up appropriate snapshot for scanning
    if (!isconcurrent)
        snapshot = SnapshotAny;
    else
        snapshot = RegisterSnapshot(GetTransactionSnapshot());

    // Estimate shared memory requirements
    estbrinshared = _brin_parallel_estimate_shared(heap, snapshot);
    shm_toc_estimate_chunk(&pcxt->estimator, estbrinshared);
    estsort = tuplesort_estimate_shared(scantuplesortstates);
    shm_toc_estimate_chunk(&pcxt->estimator, estsort);

    // Estimate space for usage tracking and query text
    shm_toc_estimate_keys(&pcxt->estimator, 2);
    shm_toc_estimate_chunk(&pcxt->estimator, mul_size(sizeof(WalUsage), pcxt->nworkers));
    shm_toc_estimate_chunk(&pcxt->estimator, mul_size(sizeof(BufferUsage), pcxt->nworkers));
    shm_toc_estimate_keys(&pcxt->estimator, 2);

    if (debug_query_string)
    {
        shm_toc_estimate_chunk(&pcxt->estimator, strlen(debug_query_string) + 1);
        shm_toc_estimate_keys(&pcxt->estimator, 1);
    }

    // Initialize DSM - fall back to serial if it fails
    InitializeParallelDSM(pcxt);
    if (pcxt->seg == NULL)
    {
        if (IsMVCCSnapshot(snapshot))
            UnregisterSnapshot(snapshot);
        DestroyParallelContext(pcxt);
        ExitParallelMode();
        return;
    }

    // Set up shared BRIN state
    brinshared = (BrinShared *) shm_toc_allocate(pcxt->toc, estbrinshared);
    brinshared->heaprelid = RelationGetRelid(heap);
    brinshared->indexrelid = RelationGetRelid(index);
    brinshared->isconcurrent = isconcurrent;
    brinshared->scantuplesortstates = scantuplesortstates;
    brinshared->pagesPerRange = buildstate->bs_pagesPerRange;
    ConditionVariableInit(&brinshared->workersdonecv);
    SpinLockInit(&brinshared->mutex);

    // Initialize counters
    brinshared->nparticipantsdone = 0;
    brinshared->reltuples = 0.0;
    brinshared->indtuples = 0.0;

    // Set up parallel table scan and shared tuplesort
    table_parallelscan_initialize(heap, ParallelTableScanFromBrinShared(brinshared), snapshot);
    sharedsort = (Sharedsort *) shm_toc_allocate(pcxt->toc, estsort);
    tuplesort_initialize_shared(sharedsort, scantuplesortstates, pcxt->seg);

    // Insert shared objects into TOC
    shm_toc_insert(pcxt->toc, PARALLEL_KEY_BRIN_SHARED, brinshared);
    shm_toc_insert(pcxt->toc, PARALLEL_KEY_TUPLESORT, sharedsort);

    // Allocate usage tracking space
    WalUsage *walusage = shm_toc_allocate(pcxt->toc, mul_size(sizeof(WalUsage), pcxt->nworkers));
    BufferUsage *bufferusage = shm_toc_allocate(pcxt->toc, mul_size(sizeof(BufferUsage), pcxt->nworkers));
    shm_toc_insert(pcxt->toc, PARALLEL_KEY_WAL_USAGE, walusage);
    shm_toc_insert(pcxt->toc, PARALLEL_KEY_BUFFER_USAGE, bufferusage);

    // Launch workers and set up leader state
    LaunchParallelWorkers(pcxt);
    brinleader->pcxt = pcxt;
    brinleader->nparticipanttuplesorts = pcxt->nworkers_launched + (leaderparticipates ? 1 : 0);
    brinleader->brinshared = brinshared;
    brinleader->sharedsort = sharedsort;
    brinleader->snapshot = snapshot;
    brinleader->walusage = walusage;
    brinleader->bufferusage = bufferusage;

    // Fall back to serial if no workers launched
    if (pcxt->nworkers_launched == 0)
    {
        _brin_end_parallel(brinleader, NULL);
        return;
    }

    // Save leader state and optionally participate in scanning
    buildstate->bs_leader = brinleader;
    if (leaderparticipates)
        _brin_leader_participate_as_worker(buildstate, heap, index);

    WaitForParallelWorkersToAttach(pcxt);
}
```