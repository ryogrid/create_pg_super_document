# ConditionalLockDatabaseObject

## Location
[src/backend/storage/lmgr/lmgr.c:1024-1058](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lmgr.c#L1024-L1058)

## Overview
ConditionalLockDatabaseObject attempts to obtain a lock on a database object without blocking, returning immediately with a boolean indicating whether the lock was successfully acquired.

## Definition

```c
bool
ConditionalLockDatabaseObject(Oid classid, Oid objid, uint16 objsubid,
							  LOCKMODE lockmode)
```
## Detailed Description
ConditionalLockDatabaseObject provides a non-blocking variant of LockDatabaseObject. It attempts to acquire a lock on a database object but will not wait if the lock is not immediately available. Instead, it returns false if the lock cannot be acquired without blocking, and true if the lock was successfully obtained.

The function uses LockAcquireExtended with the dontWait parameter set to true to achieve the non-blocking behavior. Like its blocking counterpart, this function should not be used for shared objects (tablespaces) or relations, and locks taken this way will not conflict with relation-specific locks.

When a lock is successfully acquired (and it wasn't already held in clear state), the function processes invalidation messages and marks the local lock as clear to maintain cache consistency.

## Parameters / Member Variables
- `classid`: The OID of the system catalog that contains the object
- `objid`: The OID of the specific object to lock within that catalog
- `objsubid`: A sub-object identifier (typically 0 for whole objects, or column numbers for attributes)
- `lockmode`: The LOCKMODE specifying the type of lock to acquire
## Dependencies
- Functions called/Symbols referenced:
  - [LOCKTAG](../L/LOCKTAG.md) (data structure for lock identification)
  - [LOCALLOCK](../L/LOCALLOCK.md) (backend's local view of a lock)
  - LockAcquireResult (enumeration for lock acquisition results)
  - SET_LOCKTAG_OBJECT (macro to initialize object lock tag)
  - [LockAcquireExtended](../L/LockAcquireExtended.md) (extended lock acquisition function with non-blocking option)
  - LOCKACQUIRE_NOT_AVAIL (result indicating lock not available)
  - LOCKACQUIRE_ALREADY_CLEAR (result indicating lock was already held in clear state)
  - [AcceptInvalidationMessages](../A/AcceptInvalidationMessages.md) (system cache invalidation handling)
  - [MarkLockClear](../M/MarkLockClear.md) (mark local lock as having processed invalidations)
- Called from (representative examples):
  - [do_autovacuum](../d/do_autovacuum.md) (src/backend/postmaster/autovacuum.c:2208)

## Notes and Other Information
- Returns true if lock was acquired, false if lock was not available without blocking
- Uses the non-blocking variant of lock acquisition (LockAcquireExtended with dontWait=true)
- Same restrictions as LockDatabaseObject: should not be used for shared objects or relations
- Processes invalidation messages only when a new lock is acquired (not when already held clear)
- Commonly used by background processes like autovacuum that need to avoid blocking on lock acquisition
- The lock acquisition is scoped to the current database (MyDatabaseId)
- Located in src/backend/storage/lmgr/lmgr.c:1024-1058

## Simplified Source
```c
bool ConditionalLockDatabaseObject(Oid classid, Oid objid, uint16 objsubid,
                                   LOCKMODE lockmode)
{
    LOCKTAG tag;
    LOCALLOCK *locallock;
    LockAcquireResult res;

    // Create lock tag for the database object
    SET_LOCKTAG_OBJECT(tag, MyDatabaseId, classid, objid, objsubid);

    // Try to acquire lock without waiting
    res = LockAcquireExtended(&tag, lockmode, false, true, true, &locallock);

    if (res == LOCKACQUIRE_NOT_AVAIL)
        return false;

    // Process invalidation messages if we got a new lock
    if (res != LOCKACQUIRE_ALREADY_CLEAR) {
        AcceptInvalidationMessages();
        MarkLockClear(locallock);
    }

    return true;
}
```