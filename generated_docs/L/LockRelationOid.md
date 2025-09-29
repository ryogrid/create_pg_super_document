# LockRelationOid

## Location
[src/backend/storage/lmgr/lmgr.c:108-150](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lmgr.c#L108-L150)

## Overview
LockRelationOid acquires a lock on a relation using only its OID, typically called before opening the relation's relcache entry to ensure consistency.

## Definition
```c
void LockRelationOid(Oid relid, LOCKMODE lockmode)
```

## Detailed Description
This function is a core component of PostgreSQL's locking system that acquires a lock on a relation using only its OID. It is typically used before attempting to open a relation's relcache entry to ensure the relation remains consistent during access.

The function creates a LOCKTAG using SetLocktagRelationOid(), then calls LockAcquireExtended() to actually acquire the lock. After acquiring the lock, it handles cache invalidation messages to ensure that any stale relcache entries are updated or flushed before use.

A key optimization is that if the lock was already held with the same mode (LOCKACQUIRE_ALREADY_CLEAR is not returned), the function processes invalidation messages and marks the lock as clear. This ensures that relcache entries are current and prevents issues with stale cached data.

The function handles recursive locking scenarios where code might act on tables (usually catalogs) recursively and ensures proper invalidation message processing even in these corner cases.

## Parameters / Member Variables
- `relid`: The OID of the relation to lock
- `lockmode`: The type of lock to acquire (e.g., AccessShareLock, RowExclusiveLock, AccessExclusiveLock)

## Dependencies
- Functions called/Symbols referenced:
  - [LOCKTAG](LOCKTAG.md)
  - [LOCALLOCK](LOCALLOCK.md)  
  - LockAcquireResult
  - [SetLocktagRelationOid](../S/SetLocktagRelationOid.md)
  - [LockAcquireExtended](LockAcquireExtended.md)
  - LOCKACQUIRE_ALREADY_CLEAR
  - [AcceptInvalidationMessages](../A/AcceptInvalidationMessages.md)
  - [MarkLockClear](../M/MarkLockClear.md)
- Called from (representative examples):
  - [relation_open](../r/relation_open.md) (src/backend/access/common/relation.c:55)
  - [try_relation_open](../t/try_relation_open.md) (src/backend/access/common/relation.c:96)
  - [RangeVarGetRelidExtended](../R/RangeVarGetRelidExtended.md) (src/backend/catalog/namespace.c:592)
  - [heap_create_with_catalog](../h/heap_create_with_catalog.md) (src/backend/catalog/heap.c:1258)
  - [index_create](../i/index_create.md) (src/backend/catalog/index.c:1061)

## Notes and Other Information
- Should generally be used before attempting to open a relation's relcache entry
- Handles cache invalidation to prevent stale relcache entries
- RangeVarGetRelid() specifically relies on this function for proper cache management  
- Optimized to skip invalidation processing when the same lock mode was already held
- Handles recursive table access scenarios properly
- Part of the lock manager (lmgr) subsystem located in src/backend/storage/lmgr/lmgr.c:108-150
- Critical for maintaining consistency in PostgreSQL's relation access

## Simplified Source

```c
void LockRelationOid(Oid relid, LOCKMODE lockmode)
{
    LOCKTAG     tag;
    LOCALLOCK  *locallock;
    LockAcquireResult res;

    // Set up the lock tag for the relation using its OID
    SetLocktagRelationOid(&tag, relid);

    // Acquire the lock (not session lock, not dontWait, reportMemoryError=true)
    res = LockAcquireExtended(&tag, lockmode, false, false, true, &locallock);

    /*
     * Process invalidation messages if we didn't already have this lock.
     * This ensures any stale relcache entries are updated before use.
     * Skip if lock was already held in same mode (LOCKACQUIRE_ALREADY_CLEAR).
     */
    if (res != LOCKACQUIRE_ALREADY_CLEAR)
    {
        // Process any pending invalidation messages
        AcceptInvalidationMessages();

        // Mark this lock as having processed invalidations
        MarkLockClear(locallock);
    }
}
```