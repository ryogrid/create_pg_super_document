# FastPathUnGrantRelationLock

## Location
src/backend/storage/lmgr/lock.c: 2682 - 2711

## Overview
FastPathUnGrantRelationLock releases a specific lock mode on a relation from the per-backend fast-path array and updates the local usage count for fast-path slots.

## Definition
```c
static bool FastPathUnGrantRelationLock(Oid relid, LOCKMODE lockmode)
```

## Detailed Description
This function is the counterpart to FastPathGrantRelationLock, handling the release of locks that were granted via the fast-path mechanism. It searches through all fast-path slots for the specified relation and lock mode combination, and when found, clears that lock mode from the slot. The function also maintains an accurate count of fast-path slots in use by resetting and recalculating FastPathLocalUseCount during each call.

The function continues iterating through all slots even after finding and clearing the target lock to ensure the usage count is properly updated. This design ensures that the fast-path usage statistics remain accurate for subsequent lock acquisition decisions.

## Parameters / Member Variables
- `relid`: The OID of the relation from which the lock is being released
- `lockmode`: The specific lock mode being released (e.g., AccessShareLock, RowExclusiveLock, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - FAST_PATH_CHECK_LOCKMODE: Macro to verify if a specific lock mode is held in a fast-path slot
  - FAST_PATH_CLEAR_LOCKMODE: Macro to clear a specific lock mode from a fast-path slot
  - FAST_PATH_GET_BITS: Macro to extract lock bits from fast-path slot for usage counting
  - FP_LOCK_SLOTS_PER_BACKEND: Constant defining the number of fast-path slots per backend
- Called from (representative examples):
  - LockRelease: Main lock release function
  - LockReleaseAll: Function to release all locks held by a backend
  - ConflictsWithRelationFastPath: Function checking for lock conflicts

## Notes and Other Information
- Returns true if the specified lock was found and released, false otherwise
- Resets FastPathLocalUseCount to zero and recalculates it during each call
- Includes an assertion to ensure that duplicate lock releases are not attempted
- The function processes all slots to maintain accurate usage statistics, not just the target slot
- Works in conjunction with FastPathGrantRelationLock to provide optimized relation locking
- Does not remove the relation entry from the fast-path slot even when all lock modes are cleared