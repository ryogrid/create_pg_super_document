# InitAuxiliaryProcess

## Location
[src/backend/storage/lmgr/proc.c:528-663](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/proc.c#L528-L663)

## Overview
Creates a PGPROC entry for auxiliary processes (like bgwriter and similar processes) so they can wait for LWLocks, providing them with a real MyProc value for process synchronization.

## Definition
void InitAuxiliaryProcess(void)

## Detailed Description
InitAuxiliaryProcess initializes auxiliary processes by allocating them a PGPROC entry from the pre-allocated pool created during InitProcGlobal. Auxiliary processes are background processes that need to perform lightweight locking but don't require full transaction management capabilities.

The function performs several key operations:
- Allocates a free PGPROC slot from the AuxiliaryProcs array
- Initializes the PGPROC structure with appropriate default values
- Sets up latch ownership for process synchronization
- Configures wait event reporting to shared memory
- Initializes lightweight lock access capabilities

Unlike regular backend processes, auxiliary processes are not added to the ProcArray, don't participate in sinval messaging (except startup process as sendOnly), and don't get assigned a VXID unless they're the startup process which needs to show up in pg_locks.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [set_spins_per_delay](../s/set_spins_per_delay.md)
  - GetNumberFromPGProc
  - [dlist_node_init](../d/dlist_node_init.md)
  - [OwnLatch](../O/OwnLatch.md)
  - [SwitchToSharedLatch](../S/SwitchToSharedLatch.md)
  - [pgstat_set_wait_event_storage](../p/pgstat_set_wait_event_storage.md)
  - [PGSemaphoreReset](../P/PGSemaphoreReset.md)
  - [on_shmem_exit](../o/on_shmem_exit.md)
  - [AuxiliaryProcKill](../A/AuxiliaryProcKill.md)
  - [InitLWLockAccess](InitLWLockAccess.md)
  - [AttachSharedMemoryStructs](../A/AttachSharedMemoryStructs.md) (EXEC_BACKEND only)

- Called from (representative examples):
  - [AuxiliaryProcessMainCommon](../A/AuxiliaryProcessMainCommon.md)

## Notes and Other Information
- The function panics if ProcGlobal or AuxiliaryProcs are not initialized
- Errors if MyProc is already set (process already exists)
- Uses ProcStructLock to protect assignment of AuxiliaryProcs entries
- Auxiliary processes are marked as background workers (isBackgroundWorker = true)
- The startup process is a special case that uses locks and participates in sinval messaging
- Under EXEC_BACKEND, shared memory structures are attached after LWLock initialization
- The function sets up cleanup via on_shmem_exit to call AuxiliaryProcKill on process termination