# ConditionalLockRelationForExtension

## Location
src/backend/storage/lmgr/lmgr.c: 438 - 454

## Overview
Attempts to acquire an extension lock on a relation without blocking, returning immediately if the lock is not available.

## Definition

```c
bool
ConditionalLockRelationForExtension(Relation relation, LOCKMODE lockmode)
```
## Detailed Description
This function provides a non-blocking variant of LockRelationForExtension. It attempts to acquire an extension lock on a relation to prevent race conditions during relation extension operations, but unlike the blocking version, it returns immediately if the lock cannot be obtained without waiting.

The function creates the same type of lock tag used by LockRelationForExtension using SET_LOCKTAG_RELATION_EXTEND macro, then calls LockAcquire with the dontWait parameter set to true. This allows the caller to implement fallback strategies when the lock is not immediately available.

## Parameters / Member Variables
- `relation`: Pointer to the Relation structure representing the relation to be extended
- `lockmode`: The type of lock to acquire (e.g., ExclusiveLock, ShareLock)

## Return Value
- Returns `true` if the lock was successfully acquired
- Returns `false` if the lock could not be obtained without blocking

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_RELATION_EXTEND (macro to set up lock tag for relation extension)
  - LockAcquire (core lock acquisition function with dontWait=true)
  - LOCKACQUIRE_NOT_AVAIL (constant for comparing lock acquisition result)
- Called from (representative examples):
  - XLTW_Oper (transaction lock wait operations)

## Notes and Other Information
- This is the non-blocking counterpart to LockRelationForExtension
- Useful when the caller needs to implement timeout or alternative strategies for lock acquisition
- The function uses the same lock tag type (LOCKTAG_RELATION_EXTEND) as the blocking version
- Commonly used in scenarios where deadlock avoidance or performance optimization requires non-blocking lock attempts
- The caller should handle the case where the function returns false by either retrying or using an alternative approach