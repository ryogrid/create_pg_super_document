# FastPathGetRelationLockEntry

## Location
src/backend/storage/lmgr/lock.c: 2800 - 2903

## Overview
FastPathGetRelationLockEntry retrieves a PROCLOCK for a lock originally acquired via the fast-path mechanism, transferring it to the primary lock table if necessary.

## Definition
```c
static PROCLOCK *FastPathGetRelationLockEntry(LOCALLOCK *locallock)
```

## Detailed Description
This function serves as a bridge between the fast-path locking mechanism and the standard shared lock table. When a lock was originally acquired through fast-path optimization but now needs to be accessible through the standard locking infrastructure (for example, during lock conflict checking or prepared transaction processing), this function locates the fast-path lock entry and transfers it to the shared hash table.

The function first searches the current backend's fast-path slots for the specified relation and lock mode. If found, it creates the corresponding entries in the shared lock hash table and removes the lock from the fast-path slot. If the lock has already been transferred by another process, it searches the shared hash table directly to find the existing PROCLOCK entry.

## Parameters / Member Variables
- `locallock`: Pointer to the LOCALLOCK structure containing the lock tag, mode, and hash code for the lock being retrieved

## Dependencies
- Functions called/Symbols referenced:
  - LockHashPartitionLock: Determines the appropriate partition lock for the hash code
  - LWLockAcquire/LWLockRelease: Low-level locking primitives for concurrency control
  - FAST_PATH_GET_BITS: Macro to check if a fast-path slot is in use
  - FAST_PATH_CHECK_LOCKMODE: Macro to verify specific lock modes in fast-path slots
  - FAST_PATH_CLEAR_LOCKMODE: Macro to clear specific lock modes from fast-path slots
  - SetupLockInTable: Creates or finds lock objects in the shared hash table
  - GrantLock: Grants the transferred lock in the shared lock table
  - hash_search_with_hash_value: Hash table search function for finding existing locks
  - ProcLockHashCode: Computes hash code for PROCLOCK entries
  - ereport/elog: Error reporting functions
- Called from (representative examples):
  - AtPrepare_Locks: During prepared transaction processing
  - ConflictsWithRelationFastPath: When checking for lock conflicts

## Notes and Other Information
- Returns a pointer to the PROCLOCK entry, either newly created or found in the shared table
- Handles the case where the lock may have already been transferred by another backend
- Raises an ERROR if shared memory allocation fails, suggesting increase of max_locks_per_transaction
- Uses exclusive locking on both fast-path info lock and partition lock to ensure consistency
- The caller is responsible for updating the corresponding LOCALLOCK object
- Critical for maintaining lock visibility when transitioning from fast-path to standard locking infrastructure
- Handles error cases where expected lock objects cannot be found in the shared hash table