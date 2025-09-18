# LockRelation

## Location
[src/backend/storage/lmgr/lmgr.c:244-274](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lmgr.c#L244-L274)

## Overview
LockRelation is a convenience function for acquiring an additional lock on an already-open relation, with built-in invalidation message handling.

## Definition
```c
void LockRelation(Relation relation, LOCKMODE lockmode)
```

## Detailed Description
This function acquires a lock on a relation that is already open. It creates a LOCKTAG using the relation's LockRelId information, acquires the lock using LockAcquireExtended, and then handles invalidation messages if necessary. The function is designed as a convenience routine for adding locks to relations that are already open - it should not be used with relations opened with NoLock. After acquiring the lock, it checks for cache invalidation messages and marks the local lock as clear if needed, ensuring cache consistency.

## Parameters / Member Variables
- `relation`: Pointer to an already-open Relation structure containing the relation information and lock details
- `lockmode`: The lock mode to acquire (e.g., AccessShareLock, RowExclusiveLock, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_RELATION (macro to set up lock tag for relation)
  - [LockAcquireExtended](LockAcquireExtended.md) (performs the actual lock acquisition with extended options)
  - [AcceptInvalidationMessages](../A/AcceptInvalidationMessages.md) (processes cache invalidation messages)
  - [MarkLockClear](../M/MarkLockClear.md) (marks the local lock state as clear)
- Types referenced:
  - [Relation](../R/Relation.md) (relation descriptor structure)
  - LOCKTAG (lock tag structure)
  - [LOCALLOCK](LOCALLOCK.md) (local lock information)
  - LockAcquireResult (result of lock acquisition)
  - LOCKMODE (enumeration of lock modes)
- Called from (representative examples):
  - index_create (in index.c:999)

## Notes and Other Information
- This is specifically for adding locks to already-open relations - never use with relation_open(foo, NoLock)
- The function uses reportMemoryError=true, dontWait=false, and needStrongLockCheck=false in LockAcquireExtended
- Includes cache invalidation handling to maintain consistency when the lock was not already held locally
- Part of PostgreSQL's lock manager subsystem located in src/backend/storage/lmgr/lmgr.c
- The invalidation message processing ensures that any cached relation information is updated if other processes modified the relation