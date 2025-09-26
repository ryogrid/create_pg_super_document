# LockRelease

## Location
[src/backend/storage/lmgr/lock.c:1964-2168](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L1964-L2168)

## Overview
LockRelease releases one lock of a specified mode on a given lock tag, handling both session and transaction locks while waking up any processes that can now be granted locks.

## Definition

```c
bool
LockRelease(const LOCKTAG *locktag, LOCKMODE lockmode, bool sessionLock)
```
## Detailed Description
LockRelease is the primary function for releasing locks in PostgreSQL's lock management system. It performs a comprehensive release process that includes:

1. **Validation**: Verifies lock method and mode parameters
2. **Local Lock Lookup**: Finds the corresponding LOCALLOCK entry
3. **Owner Management**: Decrements reference counts for the appropriate resource owner
4. **Fast Path Optimization**: Attempts fast-path release for relation locks when possible
5. **Shared Lock Table Operations**: Falls back to shared lock table manipulation if needed
6. **Lock Cleanup**: Calls UnGrantLock and CleanUpLock to update shared state and wake waiters
7. **Local Cleanup**: Removes the LOCALLOCK entry if no longer held

The function handles both regular transaction locks (tied to CurrentResourceOwner) and session locks (not tied to any resource owner). It includes extensive error checking and supports PostgreSQL's fast-path optimization for relation locks.

## Parameters / Member Variables
- : Pointer to LOCKTAG structure identifying the specific lock to release
- : The lock mode being released (e.g., AccessShareLock, ExclusiveLock)
- : If true, release a session lock; if false, release a transaction lock

## Dependencies
- Functions called/Symbols referenced:
  - [hash_search](../h/hash_search.md)/hash_search_with_hash_value (hash table operations)
  - EligibleForRelationFastPath
  - [FastPathUnGrantRelationLock](../F/FastPathUnGrantRelationLock.md)
  - LockHashPartitionLock
  - [ResourceOwnerForgetLock](../R/ResourceOwnerForgetLock.md)
  - [UnGrantLock](../U/UnGrantLock.md)
  - [CleanUpLock](../C/CleanUpLock.md)
  - [RemoveLocalLock](../R/RemoveLocalLock.md)
  - LOCK_PRINT/PROCLOCK_PRINT (debug macros)
  - LOCKBIT_ON (macro)
  - [LWLockAcquire](LWLockAcquire.md)/LWLockRelease
- Called from (representative examples):
  - [UnlockRelationId](../U/UnlockRelationId.md)
  - [UnlockRelationOid](../U/UnlockRelationOid.md)
  - [UnlockRelation](../U/UnlockRelation.md)
  - [XactLockTableDelete](../X/XactLockTableDelete.md)
  - [SpeculativeInsertionLockRelease](../S/SpeculativeInsertionLockRelease.md)
  - [UnlockDatabaseObject](../U/UnlockDatabaseObject.md)
  - [UnlockSharedObject](../U/UnlockSharedObject.md)
  - pg_advisory_unlock functions

## Notes and Other Information
- Returns true if lock was successfully released, false if not owned by caller
- Supports both fast-path and shared lock table release mechanisms
- Resets lockCleared flag when completely releasing a lock
- May need to re-lookup shared objects if lock was moved from fast-path to shared table
- Includes comprehensive ownership verification to prevent releasing locks owned by other resource owners
- Wakes up waiting processes through CleanUpLock when appropriate
- Located in src/backend/storage/lmgr/lock.c at lines 1964-2168
- Critical for transaction cleanup and lock contention resolution