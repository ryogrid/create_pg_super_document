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
  - LWLockHeldByMe (assertion check for SerializableXactHashLock)
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