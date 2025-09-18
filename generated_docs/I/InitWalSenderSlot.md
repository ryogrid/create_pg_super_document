# InitWalSenderSlot

## Location
src/backend/replication/walsender.c: 2927 - 3003

## Overview
Initializes and reserves a WAL sender slot in shared memory for the current WAL sender process.

## Definition
```c
static void InitWalSenderSlot(void)
```

## Detailed Description
This function sets up per-walsender data structures by finding and reserving an available slot in the shared memory WAL sender control structure. It initializes all fields of the WAL sender slot with appropriate default values and determines the replication kind (physical or logical) based on the database context. The function ensures proper resource cleanup by registering an exit handler.

The slot reservation process is protected by spinlocks to prevent race conditions among multiple WAL sender processes. Once a slot is reserved, the process sets up lag tracking, position tracking, and state management fields.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - WalSnd (structure type)
  - SpinLockAcquire/SpinLockRelease
  - WALSNDSTATE_STARTUP
  - InvalidXLogRecPtr
  - REPLICATION_KIND_PHYSICAL
  - REPLICATION_KIND_LOGICAL  
  - on_shmem_exit
  - WalSndKill
- Called from:
  - LagTracker (src/backend/replication/walsender.c:242)

## Notes and Other Information
- Requires WalSndCtl to be already initialized (inherited from postmaster via fork/EXEC_BACKEND)
- Asserts that MyWalSnd is initially NULL to ensure proper initialization order
- Searches through max_wal_senders slots to find an available one (pid == 0)
- Initializes all tracking pointers (sentPtr, write, flush, apply) to InvalidXLogRecPtr
- Sets all lag measurements (writeLag, flushLag, applyLag) to -1 (uninitialized)
- Determines replication kind based on MyDatabaseId: InvalidOid indicates physical replication
- Logical replication determination allows reading WAL records during slot creation
- Registers WalSndKill as cleanup handler for process exit
- Critical that this succeeds due to prior free slot validation in InitProcess()
- Thread-safe slot reservation using spinlock protection around each slot check