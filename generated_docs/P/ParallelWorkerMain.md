# ParallelWorkerMain

## Location
src/backend/access/transam/parallel.c: 1288 - 1572

## Overview
ParallelWorkerMain is the main entrypoint function for parallel worker processes, responsible for initializing and setting up the entire execution environment for a parallel worker before executing the worker-specific code.

## Definition
```c
void ParallelWorkerMain(Datum main_arg)
```

## Detailed Description
ParallelWorkerMain serves as the primary initialization function for PostgreSQL parallel worker processes. It performs comprehensive setup to establish an execution environment that mirrors the state of the parallel leader process. The function handles dynamic shared memory attachment, transaction state restoration, security context setup, and numerous other initialization tasks required for parallel query execution.

The function operates in several key phases:
1. **Signal handling and worker identification** - Sets up signal handlers and determines the worker number
2. **Dynamic shared memory attachment** - Attaches to the DSM segment created by the leader
3. **Error reporting setup** - Establishes message queues for error communication with the leader
4. **Lock group membership** - Joins the parallel lock group to prevent deadlocks
5. **State restoration** - Restores transaction state, GUC values, snapshots, and various backend states
6. **Worker execution** - Calls the application-specific parallel worker function
7. **Cleanup** - Performs shutdown procedures and reports completion

The function ensures that the parallel worker operates in an environment that is functionally equivalent to the leader process, enabling transparent execution of parallel operations.

## Parameters / Member Variables
- `main_arg`: A Datum containing the DSM segment handle (as UInt32) that the worker should attach to for accessing shared state

## Dependencies
- Functions called/Symbols referenced:
  - dsm_attach, dsm_segment_address
  - shm_toc_attach, shm_toc_lookup
  - LookupParallelWorkerFunction
  - SetParallelStartTimestamps
  - StartParallelWorkerTransaction, EndParallelWorkerTransaction
  - AttachSession, DetachSession  
  - RestorePendingSyncs, RestoreUncommittedEnums
  - BecomeLockGroupMember
  - BackgroundWorkerInitializeConnectionByOid
  - EnterParallelMode, ExitParallelMode
  - StartTransactionCommand, CommitTransactionCommand
- Called from (representative examples):
  - BackgroundWorkerHandle (via bgworker registration)
  - IsParallelWorker (helper function)

## Notes and Other Information
- Sets the global flag `InitializingParallelWorker = true` during initialization phase
- Uses the `ParallelWorkerShutdown` function as a before_shmem_exit callback for cleanup
- Redirects error messages to shared message queues for leader process consumption
- Performs extensive state restoration including GUCs, snapshots, security contexts, and various subsystem states
- The worker number is embedded in `MyBgworkerEntry->bgw_extra` and copied to `ParallelWorkerNumber`
- Creates a dedicated memory context "Parallel worker" for cleanliness during execution
- Handles both REPEATABLE READ/SERIALIZABLE transaction snapshots and lower isolation levels appropriately
- Must successfully join the lock group or exits silently to prevent deadlocks