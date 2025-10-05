# _bt_begin_parallel

## Location
[src/backend/access/nbtree/nbtsort.c:1396-1606](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsort.c#L1396-L1606)

## Overview
Initializes and launches parallel workers for B-tree index construction, setting up shared memory structures and coordinating the parallel build process.

## Definition

```c
static void
_bt_begin_parallel(BTBuildState *buildstate, bool isconcurrent, int request)
```
## Detailed Description
This function is responsible for setting up the entire parallel infrastructure for B-tree index building. It creates a parallel context, estimates and allocates shared memory for various components (tuplesort states, WAL/buffer usage tracking, query strings), and launches worker processes. The function handles both regular and concurrent index builds by selecting appropriate snapshots.

Key responsibilities include:
- Creating parallel context and entering parallel mode
- Estimating shared memory requirements for tuplesort operations
- Setting up shared state structures (BTShared) with index metadata
- Initializing parallel table scan for heap relation
- Allocating space for performance monitoring (WAL/Buffer usage)
- Launching worker processes and coordinating leader participation
- Handling fallback to serial build if parallel setup fails

The function supports both unique and non-unique indexes, with unique indexes requiring an additional spool for duplicate handling.

## Parameters / Member Variables
- `*buildstate`: Main B-tree build state containing spool, heap relation, and other build context
- `isconcurrent`: Boolean indicating if this is a CREATE INDEX CONCURRENTLY operation (affects snapshot selection)
- `request`: Target number of parallel worker processes to launch
## Dependencies
- Functions called/Symbols referenced:
  - [EnterParallelMode](../E/EnterParallelMode.md): Enter PostgreSQL's parallel execution mode
  - [CreateParallelContext](../C/CreateParallelContext.md): Create context for parallel worker coordination
  - [_bt_parallel_estimate_shared](_bt_parallel_estimate_shared.md): Estimate shared memory for B-tree specific state
  - [tuplesort_estimate_shared](../t/tuplesort_estimate_shared.md): Estimate memory for tuplesort operations
  - [InitializeParallelDSM](../I/InitializeParallelDSM.md): Initialize dynamic shared memory segment
  - [table_parallelscan_initialize](../t/table_parallelscan_initialize.md): Set up parallel heap scanning
  - [LaunchParallelWorkers](../L/LaunchParallelWorkers.md): Start the worker processes
  - [_bt_leader_participate_as_worker](_bt_leader_participate_as_worker.md): Have leader process participate as worker
  - [_bt_end_parallel](_bt_end_parallel.md): Cleanup function for parallel mode termination
- Called from (representative examples):
  - [_bt_spools_heapscan](_bt_spools_heapscan.md): Main heap scanning function that decides whether to use parallel processing

## Notes and Other Information
- Falls back to serial build if DSM segment allocation fails or no workers can be launched
- Uses conditional compilation flag DISABLE_LEADER_PARTICIPATION for testing scenarios
- Handles both SnapshotAny (regular builds) and MVCC snapshots (concurrent builds)
- Sets up monitoring infrastructure for WAL and buffer usage tracking
- The caller must eventually call _bt_end_parallel() to properly shutdown parallel mode
- Shared memory layout includes keys for B-tree state, tuplesort data, WAL/buffer usage, and query text

## Simplified Source

```c
static void
_bt_begin_parallel(BTBuildState *buildstate, bool isconcurrent, int request)
{
    ParallelContext *pcxt;
    BTShared *btshared;
    BTLeader *btleader = (BTLeader *) palloc0(sizeof(BTLeader));
    bool leaderparticipates = true;

    // Enter parallel mode and create context
    EnterParallelMode();
    pcxt = CreateParallelContext("postgres", "_bt_parallel_build_main", request);

    // Choose appropriate snapshot for scanning
    Snapshot snapshot = isconcurrent ?
                       RegisterSnapshot(GetTransactionSnapshot()) :
                       SnapshotAny;

    // Estimate shared memory requirements
    Size estbtshared = _bt_parallel_estimate_shared(buildstate->spool->heap, snapshot);
    Size estsort = tuplesort_estimate_shared(leaderparticipates ? request + 1 : request);

    // Add space estimates for B-tree state, tuplesort, WAL/buffer usage, query text
    shm_toc_estimate_chunk(&pcxt->estimator, estbtshared);
    shm_toc_estimate_chunk(&pcxt->estimator, estsort);

    if (buildstate->spool->isunique) {
        shm_toc_estimate_chunk(&pcxt->estimator, estsort); // Second spool
    }

    estimate_instrumentation_space(&pcxt->estimator, pcxt->nworkers);

    // Initialize shared memory
    InitializeParallelDSM(pcxt);
    if (pcxt->seg == NULL) {
        // Fallback to serial build
        cleanup_and_exit(snapshot, pcxt);
        return;
    }

    // Set up shared B-tree state
    btshared = setup_shared_btree_state(pcxt, buildstate, isconcurrent, snapshot);

    // Initialize tuplesort shared states
    Sharedsort *sharedsort = setup_tuplesort_shared(pcxt, estsort);
    Sharedsort *sharedsort2 = buildstate->spool->isunique ?
                             setup_tuplesort_shared(pcxt, estsort) : NULL;

    // Store query string and setup instrumentation
    store_query_string_and_instrumentation(pcxt);

    // Launch workers
    LaunchParallelWorkers(pcxt);
    if (pcxt->nworkers_launched == 0) {
        _bt_end_parallel(btleader);
        return;
    }

    // Initialize leader state
    initialize_btleader(btleader, pcxt, btshared, sharedsort, sharedsort2, snapshot);
    buildstate->btleader = btleader;

    // Leader participates as worker if enabled
    if (leaderparticipates) {
        _bt_leader_participate_as_worker(buildstate);
    }

    WaitForParallelWorkersToAttach(pcxt);
}
```