# SetNewSxactGlobalXmin

## Location
[src/backend/storage/lmgr/predicate.c:3241-3301](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L3241-L3301)

## Overview
Updates the global minimum transaction ID (SxactGlobalXmin) by scanning all active serializable transactions to find the earliest xmin value.

## Definition
```c
static void SetNewSxactGlobalXmin(void)
```

## Detailed Description
SetNewSxactGlobalXmin is a critical internal function in PostgreSQL's serializable snapshot isolation system that maintains the global minimum transaction ID across all active serializable transactions. This function walks through the list of active serializable transactions to determine the earliest xmin value, which is essential for MVCC (Multi-Version Concurrency Control) and garbage collection decisions.

The function performs several key operations:
1. Resets the global xmin to invalid and count to zero
2. Iterates through all active serializable transactions
3. Skips rolled back, committed, or old committed transactions
4. Finds the earliest (minimum) xmin value among active transactions
5. Counts how many transactions share this minimum xmin value
6. Updates the serial scheduling system with the new global xmin

This global xmin value is crucial for determining which tuple versions can be safely removed during vacuum operations and for maintaining proper snapshot isolation semantics across the system.

## Parameters / Member Variables
This function takes no parameters but operates on global predicate locking state:
- Updates `PredXact->SxactGlobalXmin`: The earliest xmin among active serializable transactions
- Updates `PredXact->SxactGlobalXminCount`: Count of transactions sharing this minimum xmin value

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockHeldByMe](../L/LWLockHeldByMe.md) (assertion check for SerializableXactHashLock)
  - dlist_foreach (iteration over active transaction list)
  - dlist_container (container access for serializable transactions)
  - SxactIsRolledBack (transaction state check)
  - SxactIsCommitted (transaction state check)
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md) (transaction ID comparison using modular arithmetic)
  - TransactionIdEquals (transaction ID equality check)
  - [SerialSetActiveSerXmin](SerialSetActiveSerXmin.md) (updates serial scheduling xmin)
- Called from:
  - [SerialControl](SerialControl.md) (during serializable transaction management)
  - [ReleasePredicateLocks](../R/ReleasePredicateLocks.md) (when transactions complete)

## Notes and Other Information
- This is a static (internal) function called only within the predicate locking subsystem
- Must be called while holding SerializableXactHashLock in exclusive mode
- Uses PostgreSQL's doubly-linked list (dlist) for efficient iteration over active transactions
- The function handles PostgreSQL's wrap-around transaction ID arithmetic correctly through TransactionIdPrecedes
- Essential for maintaining consistency in MVCC by ensuring proper visibility rules for concurrent serializable transactions
- The count of transactions with the same xmin helps optimize certain serialization conflict detection algorithms

## Simplified Source

```c
// Simplified version of SetNewSxactGlobalXmin
static void SetNewSxactGlobalXmin(void) {
    dlist_iter iter;

    // Ensure we have the required lock
    Assert(LWLockHeldByMe(SerializableXactHashLock));

    // Reset global xmin tracking
    PredXact->SxactGlobalXmin = InvalidTransactionId;
    PredXact->SxactGlobalXminCount = 0;

    // Walk through all active serializable transactions
    dlist_foreach(iter, &PredXact->activeList) {
        SERIALIZABLEXACT *sxact = dlist_container(SERIALIZABLEXACT, xactLink, iter.cur);

        // Skip transactions that are no longer active
        if (SxactIsRolledBack(sxact) || SxactIsCommitted(sxact) || sxact == OldCommittedSxact) {
            continue;
        }

        // Process active transaction's xmin
        Assert(sxact->xmin != InvalidTransactionId);

        if (!TransactionIdIsValid(PredXact->SxactGlobalXmin) ||
            TransactionIdPrecedes(sxact->xmin, PredXact->SxactGlobalXmin)) {
            // Found a new minimum xmin
            PredXact->SxactGlobalXmin = sxact->xmin;
            PredXact->SxactGlobalXminCount = 1;
        } else if (TransactionIdEquals(sxact->xmin, PredXact->SxactGlobalXmin)) {
            // Another transaction has the same minimum xmin
            PredXact->SxactGlobalXminCount++;
        }
    }

    // Update the serial scheduling system with new global xmin
    SerialSetActiveSerXmin(PredXact->SxactGlobalXmin);
}
```

Key simplifications made:
- Added explanatory comments for each major section
- Restructured the main loop logic with clearer early continue for inactive transactions
- Made the xmin comparison logic more readable with better variable spacing
- Preserved all essential functionality including proper transaction state checks
- Maintained the exact same algorithm flow and correctness