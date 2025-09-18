# FastPathGrantRelationLock

## Location
src/backend/storage/lmgr/lock.c: 2645 - 2681

## Overview
FastPathGrantRelationLock attempts to grant a lock on a relation using PostgreSQL's per-backend fast-path array, providing an optimized mechanism for acquiring relation locks without accessing the shared lock table.

## Definition


## Detailed Description
This function implements PostgreSQL's fast-path locking mechanism for relation locks. It maintains a per-backend array of lock slots that can quickly grant locks without the overhead of accessing the shared lock table. The function first scans the existing fast-path slots to check if the relation already has an entry, and if found, it adds the requested lock mode to that slot. If no existing entry is found, it attempts to use an available empty slot. If neither condition is met (no existing entry and no empty slots), the function returns false, indicating that the standard locking mechanism should be used instead.

The fast-path mechanism is designed to optimize common locking scenarios where backends frequently acquire and release locks on the same relations, reducing contention on the shared lock table.

## Parameters / Member Variables
- : The OID of the relation for which the lock is being requested
- : The type of lock mode being requested (e.g., AccessShareLock, RowExclusiveLock, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - FAST_PATH_GET_BITS: Macro to extract lock bits from fast-path slot
  - FAST_PATH_CHECK_LOCKMODE: Macro to check if a specific lock mode is already held
  - FAST_PATH_SET_LOCKMODE: Macro to set a lock mode in a fast-path slot
  - FP_LOCK_SLOTS_PER_BACKEND: Constant defining the number of fast-path slots per backend
- Called from (representative examples):
  - [LockAcquireExtended](../L/LockAcquireExtended.md): Main lock acquisition function
  - ConflictsWithRelationFastPath: Function checking for lock conflicts

## Notes and Other Information
- Returns true if the lock was successfully granted via fast-path, false if standard locking is needed
- Uses the global MyProc structure to access the current backend's fast-path lock array
- Maintains FastPathLocalUseCount to track the number of fast-path slots in use
- The function includes assertions to ensure lock modes are not duplicated in the same slot
- Fast-path locking is specifically optimized for relation locks and does not handle all lock types