# InitProcGlobal

## Location
[src/backend/storage/lmgr/proc.c:157-297](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/proc.c#L157-L297)

## Overview
Initializes the global process table and associated data structures during postmaster or standalone backend startup, including creation of all per-process semaphores needed to support the configured number of backends.

## Definition
```c
void InitProcGlobal(void)
```

## Detailed Description
InitProcGlobal performs comprehensive initialization of PostgreSQL's process management infrastructure. The function:

1. **Creates the ProcGlobal shared structure**: Allocates and initializes the main PROC_HDR header containing global process state
2. **Initializes process freelists**: Sets up separate freelists for different process types (normal backends, autovacuum workers, background workers, walsenders)
3. **Allocates PGPROC array**: Creates all PGPROC structures for all possible processes upfront
4. **Creates dense arrays**: Allocates mirroring arrays for transaction IDs, subtransaction states, and status flags for performance
5. **Initializes per-process resources**: Sets up semaphores, latches, and locks for each process slot
6. **Organizes processes by type**: Distributes PGPROC structures to appropriate freelists based on process category

The function pre-allocates all semaphores at startup to avoid runtime failures under load when the system runs out of semaphore resources.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - ShmemInitStruct (shared memory structure creation)
  - ShmemAlloc (shared memory allocation)
  - MemSet (memory initialization)
  - dlist_init (doubly-linked list initialization)
  - dlist_push_tail (list insertion)
  - PGSemaphoreCreate (semaphore creation)
  - InitSharedLatch (shared latch initialization)
  - LWLockInitialize (lightweight lock initialization)
  - SpinLockInit (spinlock initialization)
  - pg_atomic_init_u32/u64 (atomic variable initialization)
- Referenced types and constants:
  - PROC_HDR (process management header)
  - PGPROC (individual process structure)
  - MaxBackends, NUM_AUXILIARY_PROCS, max_prepared_xacts
  - MaxConnections, autovacuum_max_workers, max_worker_processes
  - LWTRANCHE_LOCK_FASTPATH, NUM_LOCK_PARTITIONS
- Called from:
  - CreateOrAttachShmemStructs (during shared memory initialization)

## Notes and Other Information
- Only called by the postmaster process, not individual backends
- Pre-allocates semaphores to prevent runtime failures under load
- Creates six categories of processes with separate freelists: normal backends, autovacuum/special workers, background workers, walsenders, auxiliary processes, and prepared transactions
- Sets up dense arrays (xids, subxidStates, statusFlags) for performance optimization of hot code paths
- Auxiliary processes don't use freelists but are found via linear search in InitAuxiliaryProcess()
- Critical for proper multi-process coordination and resource management in PostgreSQL