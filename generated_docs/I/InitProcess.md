# InitProcess

## Location
src/backend/storage/lmgr/proc.c: 298 - 492

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
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - Process type detection: AmAutoVacuumWorkerProcess, AmSpecialWorkerProcess, AmBackgroundWorkerProcess, AmWalSenderProcess
  - List operations: dlist_is_empty, dlist_pop_head_node, dlist_node_init
  - Locking: SpinLockAcquire/SpinLockRelease (ProcStructLock)
  - Process management: GetNumberFromPGProc, MarkPostmasterChildActive, set_spins_per_delay
  - Latch operations: OwnLatch, SwitchToSharedLatch
  - Semaphore operations: PGSemaphoreReset
  - Cleanup registration: on_shmem_exit (with ProcKill)
  - Subsystem initialization: InitLWLockAccess, InitDeadLockChecking
  - Statistics: pgstat_set_wait_event_storage
- Global variables:
  - ProcGlobal (process management header)
  - MyProc (current process PGPROC)
  - MyProcNumber (current process number)
- Called from:
  - BootstrapModeMain (bootstrap process)
  - AutoVacWorkerMain (autovacuum worker)
  - BackgroundWorkerMain (background worker)
  - ReplSlotSyncWorkerMain (slot sync worker)
  - BackendMain (regular backend)
  - PostgresSingleUserMain (single-user mode)

## Notes and Other Information
- Must be called after ProcGlobal has been initialized by InitProcGlobal
- Each process type uses a different free list to ensure proper resource allocation
- Handles "too many connections" errors when process slots are exhausted
- Reuses PGPROC structures from previous processes, reinitializing all necessary fields
- Critical for lock management, transaction processing, and inter-process communication
- The process becomes visible to other backends and can participate in deadlock detection after this call
- Registers automatic cleanup to prevent resource leaks if the process exits unexpectedly