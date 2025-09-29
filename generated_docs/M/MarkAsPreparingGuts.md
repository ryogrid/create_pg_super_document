# MarkAsPreparingGuts

## Location
[src/backend/access/transam/twophase.c:433-503](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L433-L503)

## Overview
Initializes a GlobalTransaction structure and its associated PGPROC entry, setting up the internal state required for a two-phase commit transaction in the preparing phase.

## Definition

```c
static void
MarkAsPreparingGuts(GlobalTransaction gxact, TransactionId xid, const char *gid,
					TimestampTz prepared_at, Oid owner, Oid databaseid)
```
## Detailed Description
MarkAsPreparingGuts is an internal helper function that performs the low-level initialization of a GlobalTransaction and its corresponding PGPROC entry. It's designed to work both during normal transaction preparation and during crash recovery when reloading prepared transactions. The function initializes the PGPROC structure with appropriate values for a prepared transaction, sets up virtual transaction IDs for lock conflict resolution, and populates the GlobalTransaction structure with metadata. It assumes appropriate locks are already held and operates on pre-allocated structures.

## Parameters / Member Variables
- : The GlobalTransaction structure to initialize
- : The transaction ID being prepared
- : The Global Identifier string for the transaction
- : Timestamp when the transaction was prepared
- : Object ID of the user who owns this prepared transaction
- : Object ID of the database where this transaction is being prepared

## Dependencies
- Functions called/Symbols referenced:
  - [GlobalTransaction](../G/GlobalTransaction.md)
  - [PGPROC](../P/PGPROC.md)
  - [LWLockHeldByMeInMode](../L/LWLockHeldByMeInMode.md)
  - GetPGProcByNumber
  - MemSet
  - [dlist_node_init](../d/dlist_node_init.md)
  - PROC_WAIT_STATUS_OK
  - LocalTransactionIdIsValid
  - AmStartupProcess
  - INVALID_PROC_NUMBER
  - LW_WS_NOT_WAITING
  - [pg_atomic_init_u64](../p/pg_atomic_init_u64.md)
  - NUM_LOCK_PARTITIONS
  - [dlist_init](../d/dlist_init.md)
- Called from (representative examples):
  - [MarkAsPreparing](MarkAsPreparing.md)
  - [RecoverPreparedTransactions](../R/RecoverPreparedTransactions.md)

## Notes and Other Information
- This is a static function used internally within the two-phase commit system
- Must be called with TwoPhaseStateLock held in exclusive mode
- Handles both normal operation and crash recovery scenarios
- Initializes PGPROC as a background worker to avoid interference with normal backends
- Sets MyLockedGxact to track ownership of the GlobalTransaction entry
- The subxid data is not populated here and must be filled later by GXactLoadSubxactData
- Clones the virtual transaction ID from the current process when available

## Simplified Source

```c
// Simplified version of MarkAsPreparingGuts
static void
MarkAsPreparingGuts(GlobalTransaction gxact, TransactionId xid, const char *gid,
                    TimestampTz prepared_at, Oid owner, Oid databaseid)
{
    PGPROC *proc;
    int i;

    Assert(LWLockHeldByMeInMode(TwoPhaseStateLock, LW_EXCLUSIVE));
    Assert(gxact != NULL);

    // Get the PGPROC entry associated with this global transaction
    proc = GetPGProcByNumber(gxact->pgprocno);

    // Initialize the PGPROC entry to a clean state
    MemSet(proc, 0, sizeof(PGPROC));
    dlist_node_init(&proc->links);
    proc->waitStatus = PROC_WAIT_STATUS_OK;

    // Set up virtual transaction ID for lock conflict detection
    if (LocalTransactionIdIsValid(MyProc->vxid.lxid)) {
        // Normal case: clone current process's VXID
        proc->vxid.lxid = MyProc->vxid.lxid;
        proc->vxid.procNumber = MyProcNumber;
    } else {
        // Recovery case: use transaction ID directly
        proc->vxid.lxid = xid;
        proc->vxid.procNumber = INVALID_PROC_NUMBER;
    }

    // Set core transaction and process identification
    proc->xid = xid;
    proc->databaseId = databaseid;
    proc->roleId = owner;
    proc->pid = 0;  // No real process for prepared transactions
    proc->isBackgroundWorker = true;

    // Initialize lock-related fields
    proc->lwWaiting = LW_WS_NOT_WAITING;
    proc->waitLock = NULL;
    proc->waitProcLock = NULL;
    pg_atomic_init_u64(&proc->waitStart, 0);

    // Initialize per-partition lock lists
    for (i = 0; i < NUM_LOCK_PARTITIONS; i++) {
        dlist_init(&proc->myProcLocks[i]);
    }

    // Initialize subtransaction tracking (filled later)
    proc->subxidStatus.overflowed = false;
    proc->subxidStatus.count = 0;

    // Initialize the GlobalTransaction structure
    gxact->prepared_at = prepared_at;
    gxact->xid = xid;
    gxact->owner = owner;
    gxact->locking_backend = MyProcNumber;
    gxact->valid = false;
    gxact->inredo = false;
    strcpy(gxact->gid, gid);

    // Remember we own this GlobalTransaction entry
    MyLockedGxact = gxact;
}
```

Key simplifications made:
- Removed detailed field-by-field initialization comments for brevity
- Consolidated related field assignments into logical groups
- Added high-level comments explaining the purpose of each section
- Simplified variable initialization patterns
- Removed platform-specific assertions for clarity
- Grouped related operations (VXID setup, lock initialization, etc.)
- Maintained all essential logic while improving readability