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
  - GetTransactionSnapshot (obtains current transaction snapshot)
  - GetActiveSnapshot (obtains current active snapshot)
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