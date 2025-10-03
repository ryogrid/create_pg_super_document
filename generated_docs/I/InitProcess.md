# InitProcess

## Location
[src/backend/storage/lmgr/proc.c:298-492](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/proc.c#L298-L492)

## Overview
Initializes a per-process PGPROC entry for the current backend, obtaining a process slot from the appropriate free list and setting up all process-local state needed for participation in PostgreSQL's multi-process system.

## Definition
```c
void InitProcess(void)
```

## Detailed Description
InitProcess performs critical per-process initialization for PostgreSQL backends. The function:

1. **Selects appropriate free list**: Determines which process category (normal backend, autovacuum, background worker, walsender) to use based on process type
2. **Acquires PGPROC structure**: Gets a free PGPROC from the chosen free list while holding ProcStructLock
3. **Handles resource exhaustion**: Reports appropriate error messages when all process slots are in use
4. **Initializes process state**: Sets up all PGPROC fields including transaction IDs, wait state, locking state, and synchronization fields
5. **Takes latch ownership**: Associates the shared process latch with the current process for inter-process signaling
6. **Sets up cleanup**: Registers ProcKill to clean up process resources at exit
7. **Initializes subsystems**: Calls InitLWLockAccess and InitDeadLockChecking to set up locking infrastructure

The function essentially transforms a raw backend process into a fully-integrated member of the PostgreSQL process ecosystem.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - Process type detection: AmAutoVacuumWorkerProcess, AmSpecialWorkerProcess, AmBackgroundWorkerProcess, AmWalSenderProcess
  - [List](../L/List.md) operations: dlist_is_empty, dlist_pop_head_node, dlist_node_init
  - Locking: SpinLockAcquire/SpinLockRelease (ProcStructLock)
  - Process management: GetNumberFromPGProc, MarkPostmasterChildActive, set_spins_per_delay
  - [Latch](../L/Latch.md) operations: OwnLatch, SwitchToSharedLatch
  - Semaphore operations: PGSemaphoreReset
  - Cleanup registration: on_shmem_exit (with ProcKill)
  - Subsystem initialization: InitLWLockAccess, InitDeadLockChecking
  - Statistics: pgstat_set_wait_event_storage
- Global variables:
  - ProcGlobal (process management header)
  - MyProc (current process PGPROC)
  - MyProcNumber (current process number)
- Called from:
  - [BootstrapModeMain](../B/BootstrapModeMain.md) (bootstrap process)
  - [AutoVacWorkerMain](../A/AutoVacWorkerMain.md) (autovacuum worker)
  - [BackgroundWorkerMain](../B/BackgroundWorkerMain.md) (background worker)
  - [ReplSlotSyncWorkerMain](../R/ReplSlotSyncWorkerMain.md) (slot sync worker)
  - [BackendMain](../B/BackendMain.md) (regular backend)
  - [PostgresSingleUserMain](../P/PostgresSingleUserMain.md) (single-user mode)

## Notes and Other Information
- Must be called after ProcGlobal has been initialized by InitProcGlobal
- Each process type uses a different free list to ensure proper resource allocation
- Handles "too many connections" errors when process slots are exhausted
- Reuses PGPROC structures from previous processes, reinitializing all necessary fields
- Critical for lock management, transaction processing, and inter-process communication
- The process becomes visible to other backends and can participate in deadlock detection after this call
- Registers automatic cleanup to prevent resource leaks if the process exits unexpectedly

## Simplified Source

```c
// Simplified version of InitProcess
void InitProcess(void) {
    // Step 1: Basic validation - ensure system is properly initialized
    if (ProcGlobal == NULL)
        elog(PANIC, "proc header uninitialized");
    if (MyProc != NULL)
        elog(ERROR, "you already exist");

    // Step 2: Determine which process type we are and select appropriate free list
    dlist_head *procgloballist;
    if (AmAutoVacuumWorkerProcess() || AmSpecialWorkerProcess())
        procgloballist = &ProcGlobal->autovacFreeProcs;
    else if (AmBackgroundWorkerProcess())
        procgloballist = &ProcGlobal->bgworkerFreeProcs;
    else if (AmWalSenderProcess())
        procgloballist = &ProcGlobal->walsenderFreeProcs;
    else
        procgloballist = &ProcGlobal->freeProcs;

    // Step 3: Get a free PGPROC structure from the appropriate list
    SpinLockAcquire(ProcStructLock);
    set_spins_per_delay(ProcGlobal->spins_per_delay);

    if (!dlist_is_empty(procgloballist)) {
        MyProc = (PGPROC *) dlist_pop_head_node(procgloballist);
        SpinLockRelease(ProcStructLock);
    } else {
        // No free processes available - report "too many connections"
        SpinLockRelease(ProcStructLock);
        if (AmWalSenderProcess())
            ereport(FATAL, "max_wal_senders exceeded");
        ereport(FATAL, "too many clients already");
    }

    MyProcNumber = GetNumberFromPGProc(MyProc);

    // Step 4: Register with postmaster for proper cleanup tracking
    if (IsUnderPostmaster && !AmAutoVacuumLauncherProcess() &&
        !AmLogicalSlotSyncWorkerProcess())
        MarkPostmasterChildActive();

    // Step 5: Initialize all PGPROC fields to clean state
    initialize_proc_fields();  // Consolidated initialization

    // Step 6: Set up process-specific state
    MyProc->pid = MyProcPid;
    MyProc->vxid.procNumber = MyProcNumber;
    MyProc->isBackgroundWorker = !AmRegularBackendProcess();

    if (AmAutoVacuumWorkerProcess())
        MyProc->statusFlags |= PROC_IS_AUTOVACUUM;

    // Step 7: Set up inter-process communication via latch
    OwnLatch(&MyProc->procLatch);
    SwitchToSharedLatch();
    pgstat_set_wait_event_storage(&MyProc->wait_event_info);

    // Step 8: Initialize semaphore for clean state
    PGSemaphoreReset(MyProc->sem);

    // Step 9: Register cleanup function for process exit
    on_shmem_exit(ProcKill, 0);

    // Step 10: Initialize subsystems needed for locking and deadlock detection
    InitLWLockAccess();
    InitDeadLockChecking();

    // Platform-specific shared memory attachment (if needed)
    #ifdef EXEC_BACKEND
    if (IsUnderPostmaster)
        AttachSharedMemoryStructs();
    #endif
}

// Helper function to consolidate field initialization
static void initialize_proc_fields(void) {
    // Initialize list links and wait state
    dlist_node_init(&MyProc->links);
    MyProc->waitStatus = PROC_WAIT_STATUS_OK;

    // Initialize transaction-related fields
    MyProc->xid = InvalidTransactionId;
    MyProc->xmin = InvalidTransactionId;
    MyProc->vxid.lxid = InvalidLocalTransactionId;
    MyProc->fpVXIDLock = false;
    MyProc->fpLocalTransactionId = InvalidLocalTransactionId;

    // Initialize database/role fields (filled later)
    MyProc->databaseId = InvalidOid;
    MyProc->roleId = InvalidOid;
    MyProc->tempNamespaceId = InvalidOid;

    // Initialize locking and wait state
    MyProc->lwWaiting = LW_WS_NOT_WAITING;
    MyProc->lwWaitMode = 0;
    MyProc->waitLock = NULL;
    MyProc->waitProcLock = NULL;
    pg_atomic_write_u64(&MyProc->waitStart, 0);

    // Initialize synchronization rep fields
    MyProc->waitLSN = 0;
    MyProc->syncRepState = SYNC_REP_NOT_WAITING;
    dlist_node_init(&MyProc->syncRepLinks);

    // Initialize group processing fields
    MyProc->procArrayGroupMember = false;
    MyProc->clogGroupMember = false;

    // Initialize flags and status
    MyProc->delayChkptFlags = 0;
    MyProc->statusFlags = 0;
    MyProc->recoveryConflictPending = false;
    MyProc->wait_event_info = 0;
}
```

Key simplifications made:
- Consolidated repetitive field initialization into a helper function
- Removed detailed error handling code and complex conditionals for clarity
- Abstracted platform-specific details (#ifdef blocks)
- Simplified complex assertion checking and debug code
- Focused on the main execution path and core functionality
- Combined similar initialization steps into logical groups
- Removed verbose comments while preserving essential logic flow