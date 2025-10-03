# InitializeParallelDSM

## Location
[src/backend/access/transam/parallel.c:207-503](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/parallel.c#L207-L503)

## Overview
Establishes the dynamic shared memory segment for a parallel context and populates it with all state information that parallel workers will need to execute properly.

## Definition

```c
enumslen = 0;
```
## Detailed Description
InitializeParallelDSM is the core function responsible for setting up shared memory communication between the leader process and parallel workers. It creates a dynamic shared memory (DSM) segment and populates it with a comprehensive set of state information including transaction snapshots, GUC settings, library states, user authentication details, and error communication queues.

The function performs extensive space estimation for various types of state data, creates the DSM segment (or falls back to private memory if DSM creation fails), and systematically serializes all necessary state into the shared memory using a table-of-contents (TOC) structure. It handles edge cases such as interrupt safety and DSM segment limits by gracefully degrading to single-process execution when parallel workers cannot be safely launched.

Key responsibilities include setting up error queues for each worker, serializing transaction and snapshot state, preserving security contexts, and ensuring all workers have access to the same runtime environment as the leader process.

## Parameters / Member Variables
- : The parallel context structure that will be populated with DSM information and worker details

## Dependencies
- Functions called/Symbols referenced:
  - [GetTransactionSnapshot](../G/GetTransactionSnapshot.md) (obtains current transaction snapshot)
  - [GetActiveSnapshot](../G/GetActiveSnapshot.md) (obtains current active snapshot)
  - [GetSessionDsmHandle](../G/GetSessionDsmHandle.md) (creates per-session DSM segment)
  - [EstimateLibraryStateSpace](../E/EstimateLibraryStateSpace.md), EstimateGUCStateSpace, EstimateTransactionStateSpace (space estimation functions)
  - [EstimatePendingSyncsSpace](../E/EstimatePendingSyncsSpace.md), EstimateUncommittedEnumsSpace (state size estimation)
  - [SerializeLibraryState](../S/SerializeLibraryState.md), SerializeGUCState, SerializeTransactionState (state serialization functions)
  - [SerializePendingSyncs](../S/SerializePendingSyncs.md), SerializeUncommittedEnums (data serialization)
  - [dsm_create](../d/dsm_create.md), shm_toc_create, shm_toc_allocate (shared memory management)
  - [shm_mq_create](../s/shm_mq_create.md), shm_mq_attach (message queue setup)
  - [GetAuthenticatedUserId](../G/GetAuthenticatedUserId.md), GetSessionUserId, GetCurrentRoleId (user context)

- Called from (representative examples):
  - [_brin_begin_parallel](../b/_brin_begin_parallel.md) (BRIN index parallel operations)
  - [_bt_begin_parallel](../b/_bt_begin_parallel.md) (B-tree index parallel operations) 
  - [parallel_vacuum_init](../p/parallel_vacuum_init.md) (parallel vacuum setup)
  - [ExecInitParallelPlan](../E/ExecInitParallelPlan.md) (parallel query execution setup)

## Notes and Other Information
- Falls back to single-process execution if DSM creation fails or interrupt handling is unsafe
- Creates separate error message queues for each worker process
- Handles both transaction and active snapshots depending on isolation level requirements
- Serializes extensive state including security contexts, GUC parameters, and pending database operations
- Uses a table-of-contents structure to organize shared memory layout efficiently
- Automatically reduces worker count to zero in edge cases rather than failing outright
- Memory allocation is done in TopTransactionContext to ensure proper cleanup
- The function is designed to be robust against various failure modes in shared memory allocation

## Simplified Source

```c
void InitializeParallelDSM(ParallelContext *pcxt)
{
    MemoryContext oldcontext;
    Size segsize = 0;
    FixedParallelState *fps;
    dsm_handle session_dsm_handle = DSM_HANDLE_INVALID;
    Snapshot transaction_snapshot = GetTransactionSnapshot();
    Snapshot active_snapshot = GetActiveSnapshot();

    // Switch to transaction context for proper cleanup
    oldcontext = MemoryContextSwitchTo(TopTransactionContext);

    // Estimate space for fixed parallel state
    shm_toc_estimate_chunk(&pcxt->estimator, sizeof(FixedParallelState));
    shm_toc_estimate_keys(&pcxt->estimator, 1);

    // Safety check: can't launch workers if interrupts are disabled
    if (!INTERRUPTS_CAN_BE_PROCESSED())
        pcxt->nworkers = 0;

    // Setup per-session DSM if workers requested
    if (pcxt->nworkers > 0)
    {
        session_dsm_handle = GetSessionDsmHandle();
        if (session_dsm_handle == DSM_HANDLE_INVALID)
            pcxt->nworkers = 0;  // Can't exchange tuples without session DSM
    }

    // Estimate space for worker state if we have workers
    if (pcxt->nworkers > 0)
    {
        // Estimate space for various state types
        estimate_and_account_for_state_space(pcxt);

        // Account for error queues and entrypoint info
        shm_toc_estimate_chunk(&pcxt->estimator,
                              pcxt->nworkers * PARALLEL_ERROR_QUEUE_SIZE);
        shm_toc_estimate_chunk(&pcxt->estimator,
                              strlen(pcxt->library_name) + strlen(pcxt->function_name) + 2);
        shm_toc_estimate_keys(&pcxt->estimator, 2);
    }

    // Create DSM segment or fall back to private memory
    segsize = shm_toc_estimate(&pcxt->estimator);
    if (pcxt->nworkers > 0)
        pcxt->seg = dsm_create(segsize, DSM_CREATE_NULL_IF_MAXSEGMENTS);

    if (pcxt->seg != NULL)
        pcxt->toc = shm_toc_create(PARALLEL_MAGIC, dsm_segment_address(pcxt->seg), segsize);
    else
    {
        // Fall back to private memory, no workers
        pcxt->nworkers = 0;
        pcxt->private_memory = MemoryContextAlloc(TopMemoryContext, segsize);
        pcxt->toc = shm_toc_create(PARALLEL_MAGIC, pcxt->private_memory, segsize);
    }

    // Initialize fixed parallel state
    fps = (FixedParallelState *) shm_toc_allocate(pcxt->toc, sizeof(FixedParallelState));
    setup_fixed_parallel_state(fps);
    shm_toc_insert(pcxt->toc, PARALLEL_KEY_FIXED, fps);

    // Serialize all worker state if we have workers
    if (pcxt->nworkers > 0)
    {
        serialize_worker_state(pcxt, session_dsm_handle,
                              transaction_snapshot, active_snapshot);
        setup_error_queues(pcxt);
        setup_entrypoint_info(pcxt);
    }

    // Update final worker count
    pcxt->nworkers_to_launch = pcxt->nworkers;

    // Restore memory context
    MemoryContextSwitchTo(oldcontext);
}
```