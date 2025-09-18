# AbortStrongLockAcquire

## Location
[src/backend/storage/lmgr/lock.c:1760-1788](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L1760-L1788)

## Overview
AbortStrongLockAcquire undoes the state changes made by BeginStrongLockAcquire when a strong lock acquisition fails or needs to be aborted.

## Definition
```c
void AbortStrongLockAcquire(void)
```

## Detailed Description
AbortStrongLockAcquire is the error cleanup counterpart to BeginStrongLockAcquire. When a strong lock acquisition fails or needs to be aborted, this function restores the system to its previous state by decrementing the fast-path strong lock count and clearing the associated flags and global state.

The function first checks if there's actually a strong lock acquisition in progress. If so, it computes the hash partition for the lock, decrements the corresponding count in FastPathStrongRelationLocks, resets the local lock's holdsStrongLockCount flag to false, and clears the global StrongLockInProgress pointer. All operations on shared state are protected by spinlocks to ensure atomicity.

## Parameters / Member Variables
- None (void function, operates on global state)

## Dependencies
- Functions called/Symbols referenced:
  - FastPathStrongLockHashPartition
  - SpinLockAcquire
  - SpinLockRelease
- Global variables used:
  - StrongLockInProgress
  - FastPathStrongRelationLocks
- Data structures used:
  - [LOCALLOCK](../L/LOCALLOCK.md)
- Called from (representative examples):
  - [LockAcquireExtended](../L/LockAcquireExtended.md) (on error paths)
  - [LockErrorCleanup](../L/LockErrorCleanup.md)

## Notes and Other Information
- Unlike other strong lock functions, this one is not static and can be called from other modules
- Includes a safety check to return early if no strong lock is in progress
- Uses assertions to verify the system is in the expected state before cleanup
- The function is idempotent - it can be safely called multiple times
- Critical for maintaining consistency of the fast-path lock count array
- Used in error cleanup paths to ensure proper resource management
- Must undo exactly what BeginStrongLockAcquire set up