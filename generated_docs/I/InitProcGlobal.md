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
  - [ShmemInitStruct](../S/ShmemInitStruct.md) (shared memory structure creation)
  - [ShmemAlloc](../S/ShmemAlloc.md) (shared memory allocation)
  - MemSet (memory initialization)
  - [dlist_init](../d/dlist_init.md) (doubly-linked list initialization)
  - [dlist_push_tail](../d/dlist_push_tail.md) (list insertion)
  - [PGSemaphoreCreate](../P/PGSemaphoreCreate.md) (semaphore creation)
  - [InitSharedLatch](InitSharedLatch.md) (shared latch initialization)
  - [LWLockInitialize](../L/LWLockInitialize.md) (lightweight lock initialization)
  - SpinLockInit (spinlock initialization)
  - [pg_atomic_init_u32](../p/pg_atomic_init_u32.md)/u64 (atomic variable initialization)
- Referenced types and constants:
  - [PROC_HDR](../P/PROC_HDR.md) (process management header)
  - [PGPROC](../P/PGPROC.md) (individual process structure)
  - MaxBackends, NUM_AUXILIARY_PROCS, max_prepared_xacts
  - MaxConnections, autovacuum_max_workers, max_worker_processes
  - LWTRANCHE_LOCK_FASTPATH, NUM_LOCK_PARTITIONS
- Called from:
  - [CreateOrAttachShmemStructs](../C/CreateOrAttachShmemStructs.md) (during shared memory initialization)

## Notes and Other Information
- Only called by the postmaster process, not individual backends
- Pre-allocates semaphores to prevent runtime failures under load
- Creates six categories of processes with separate freelists: normal backends, autovacuum/special workers, background workers, walsenders, auxiliary processes, and prepared transactions
- Sets up dense arrays (xids, subxidStates, statusFlags) for performance optimization of hot code paths
- Auxiliary processes don't use freelists but are found via linear search in InitAuxiliaryProcess()
- Critical for proper multi-process coordination and resource management in PostgreSQL

## Simplified Source

```c
// Simplified version of InitProcGlobal
void InitProcGlobal(void) {
    PGPROC *procs;
    int i;
    bool found;
    uint32 TotalProcs = MaxBackends + NUM_AUXILIARY_PROCS + max_prepared_xacts;

    // Create the main ProcGlobal shared structure
    ProcGlobal = (PROC_HDR *) ShmemInitStruct("Proc Header", sizeof(PROC_HDR), &found);
    Assert(!found);

    // Initialize global process state and freelists
    ProcGlobal->spins_per_delay = DEFAULT_SPINS_PER_DELAY;
    dlist_init(&ProcGlobal->freeProcs);                // Normal backends
    dlist_init(&ProcGlobal->autovacFreeProcs);         // Autovacuum workers
    dlist_init(&ProcGlobal->bgworkerFreeProcs);        // Background workers
    dlist_init(&ProcGlobal->walsenderFreeProcs);       // WAL senders

    // Initialize synchronization primitives
    ProcGlobal->startupBufferPinWaitBufId = -1;
    ProcGlobal->walwriterLatch = NULL;
    ProcGlobal->checkpointerLatch = NULL;
    pg_atomic_init_u32(&ProcGlobal->procArrayGroupFirst, INVALID_PROC_NUMBER);
    pg_atomic_init_u32(&ProcGlobal->clogGroupFirst, INVALID_PROC_NUMBER);

    // Allocate all PGPROC structures upfront
    procs = (PGPROC *) ShmemAlloc(TotalProcs * sizeof(PGPROC));
    MemSet(procs, 0, TotalProcs * sizeof(PGPROC));
    ProcGlobal->allProcs = procs;
    ProcGlobal->allProcCount = MaxBackends + NUM_AUXILIARY_PROCS;

    // Create dense arrays for performance-critical data
    ProcGlobal->xids = (TransactionId *) ShmemAlloc(TotalProcs * sizeof(*ProcGlobal->xids));
    ProcGlobal->subxidStates = (XidCacheStatus *) ShmemAlloc(TotalProcs * sizeof(*ProcGlobal->subxidStates));
    ProcGlobal->statusFlags = (uint8 *) ShmemAlloc(TotalProcs * sizeof(*ProcGlobal->statusFlags));

    // Initialize each PGPROC structure
    for (i = 0; i < TotalProcs; i++) {
        PGPROC *proc = &procs[i];

        // Create semaphore, latch, and lock for real processes (not prepared xacts)
        if (i < MaxBackends + NUM_AUXILIARY_PROCS) {
            proc->sem = PGSemaphoreCreate();
            InitSharedLatch(&(proc->procLatch));
            LWLockInitialize(&(proc->fpInfoLock), LWTRANCHE_LOCK_FASTPATH);
        }

        // Assign processes to appropriate freelists based on type
        if (i < MaxConnections) {
            // Normal backend processes
            dlist_push_tail(&ProcGlobal->freeProcs, &proc->links);
            proc->procgloballist = &ProcGlobal->freeProcs;
        }
        else if (i < MaxConnections + autovacuum_max_workers + NUM_SPECIAL_WORKER_PROCS) {
            // Autovacuum and special worker processes
            dlist_push_tail(&ProcGlobal->autovacFreeProcs, &proc->links);
            proc->procgloballist = &ProcGlobal->autovacFreeProcs;
        }
        else if (i < MaxConnections + autovacuum_max_workers + NUM_SPECIAL_WORKER_PROCS + max_worker_processes) {
            // Background worker processes
            dlist_push_tail(&ProcGlobal->bgworkerFreeProcs, &proc->links);
            proc->procgloballist = &ProcGlobal->bgworkerFreeProcs;
        }
        else if (i < MaxBackends) {
            // WAL sender processes
            dlist_push_tail(&ProcGlobal->walsenderFreeProcs, &proc->links);
            proc->procgloballist = &ProcGlobal->walsenderFreeProcs;
        }

        // Initialize per-process lock structures and atomic variables
        for (int j = 0; j < NUM_LOCK_PARTITIONS; j++) {
            dlist_init(&(proc->myProcLocks[j]));
        }
        dlist_init(&proc->lockGroupMembers);

        pg_atomic_init_u32(&(proc->procArrayGroupNext), INVALID_PROC_NUMBER);
        pg_atomic_init_u32(&(proc->clogGroupNext), INVALID_PROC_NUMBER);
        pg_atomic_init_u64(&(proc->waitStart), 0);
    }

    // Set up pointers to special process blocks
    AuxiliaryProcs = &procs[MaxBackends];
    PreparedXactProcs = &procs[MaxBackends + NUM_AUXILIARY_PROCS];

    // Create the process structure spinlock
    ProcStructLock = (slock_t *) ShmemAlloc(sizeof(slock_t));
    SpinLockInit(ProcStructLock);
}
```

Key simplifications made:
- Consolidated memory initialization operations
- Simplified the complex conditional logic for process type assignment
- Added clear comments explaining each major step
- Abstracted detailed error handling and edge cases
- Focused on the main execution path and core functionality
- Maintained the essential algorithm for process organization and resource allocation