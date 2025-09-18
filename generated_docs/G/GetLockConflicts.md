# GetLockConflicts

## Location
src/backend/storage/lmgr/lock.c: 2904 - 3111

## Overview
GetLockConflicts returns an array of VirtualTransactionIds of transactions currently holding locks that would conflict with a specified lock mode, checking both the shared lock table and fast-path locks.

## Definition
```c
VirtualTransactionId *GetLockConflicts(const LOCKTAG *locktag, LOCKMODE lockmode, int *countp)
```

## Detailed Description
This function provides comprehensive conflict detection for PostgreSQL's locking system by examining both the standard shared lock table and per-backend fast-path lock arrays. It identifies all transactions that currently hold locks that would conflict with the requested lock mode on the specified lock tag. The function is critical for lock waiting logic, recovery conflict resolution, and deadlock detection.

The implementation first checks for potential fast-path conflicts by examining each backend's fast-path array if the requested lock could conflict with relation locks held via fast-path. Then it searches the shared lock hash table for the specific lock object and examines all current lock holders. The function carefully avoids reporting the current transaction as a conflicting holder and filters out transactions that have already committed or aborted.

## Parameters / Member Variables
- `locktag`: Pointer to the LOCKTAG structure identifying the specific lock resource
- `lockmode`: The lock mode for which conflicts are being checked
- `countp`: Optional pointer to receive the count of conflicting transactions (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - LockTagHashCode: Computes hash code for the lock tag
  - LockHashPartitionLock: Determines the appropriate partition lock
  - ConflictsWithRelationFastPath: Checks if the lock could conflict with fast-path locks
  - FAST_PATH_GET_BITS: Macro to extract lock bits from fast-path slots
  - hash_search_with_hash_value: Searches for lock objects in shared hash table
  - GET_VXID_FROM_PGPROC: Macro to extract virtual transaction ID from PGPROC
  - VirtualTransactionIdIsValid/VirtualTransactionIdEquals: VXID utility functions
  - LWLockAcquire/LWLockRelease: Low-level locking primitives
  - dlist_foreach/dlist_container: Doubly-linked list iteration macros
- Called from (representative examples):
  - ProcSleep: During lock waiting to identify blocking transactions
  - ResolveRecoveryConflictWithLock: For resolving conflicts during hot standby recovery
  - WaitForLockersMultiple: When waiting for multiple lock holders to complete

## Notes and Other Information
- Returns a palloc'd array terminated with an invalid VXID
- Result may become outdated immediately due to concurrent lock activity
- Excludes the current transaction from the conflict list
- For hot standby mode, uses a static array in TopMemoryContext for efficiency
- Handles both fast-path and standard lock table entries to provide complete coverage
- Includes logic to avoid duplicate entries when a transaction appears in both fast-path and standard tables
- The function performs database-level filtering to optimize fast-path scanning
- Transactions without valid lxid are considered non-conflicting (post-commit state)
- Includes panic-level error checking for impossible conditions like too many conflicts