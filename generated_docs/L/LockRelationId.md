# LockRelationId

## Location
[src/backend/storage/lmgr/lmgr.c:184-211](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lmgr.c#L184-L211)

## Overview
LockRelationId acquires a lock on a relation using a LockRelId structure instead of just the relation OID, providing more direct control over the database and relation identification.

## Definition
```c
void LockRelationId(LockRelId *relid, LOCKMODE lockmode)
```

## Detailed Description
This function is functionally similar to LockRelationOid but takes a LockRelId structure as input instead of just a relation OID. The LockRelId structure contains both the database ID and relation ID, allowing for more explicit control over which database context the relation exists in.

The function directly uses the SET_LOCKTAG_RELATION macro with the provided database ID and relation ID from the LockRelId structure, bypassing the need to determine whether the relation is shared (unlike LockRelationOid which must call IsSharedRelation to determine the appropriate database ID).

Like its sibling functions, LockRelationId performs invalidation message processing after acquiring the lock to ensure relcache consistency. This includes calling AcceptInvalidationMessages() and MarkLockClear() when the lock was not already held in the same mode.

This function is particularly useful when the caller already has both the database ID and relation ID available and wants to avoid the overhead of determining the database ID based on whether the relation is shared.

## Parameters / Member Variables
- `relid`: Pointer to a LockRelId structure containing both database ID (dbId) and relation ID (relId)
- `lockmode`: The type of lock to acquire (e.g., AccessShareLock, RowExclusiveLock, AccessExclusiveLock)

## Dependencies
- Functions called/Symbols referenced:
  - [LockRelId](LockRelId.md)
  - [LOCKTAG](LOCKTAG.md)
  - [LOCALLOCK](LOCALLOCK.md)
  - LockAcquireResult
  - SET_LOCKTAG_RELATION
  - [LockAcquireExtended](LockAcquireExtended.md)
  - LOCKACQUIRE_ALREADY_CLEAR
  - [AcceptInvalidationMessages](../A/AcceptInvalidationMessages.md)
  - [MarkLockClear](../M/MarkLockClear.md)
- Called from (representative examples):
  - [CreateDatabaseUsingWalLog](../C/CreateDatabaseUsingWalLog.md) (src/backend/commands/dbcommands.c:212-213)
  - [ScanSourceDatabasePgClass](../S/ScanSourceDatabasePgClass.md) (src/backend/commands/dbcommands.c:271)

## Notes and Other Information
- Same functionality as LockRelationOid but accepts LockRelId structure instead of just OID
- More efficient when database ID and relation ID are already available
- Bypasses the need to determine if relation is shared (unlike LockRelationOid)
- Uses SET_LOCKTAG_RELATION macro directly with provided IDs
- Performs same invalidation message processing for relcache consistency
- Primarily used in database creation and management operations
- Part of the lock manager (lmgr) subsystem located in src/backend/storage/lmgr/lmgr.c:184-211

## Simplified Source

```c
void
LockRelationId(LockRelId *relid, LOCKMODE lockmode)
{
    LOCKTAG tag;
    LOCALLOCK *locallock;
    LockAcquireResult res;

    // Create lock tag for relation
    SET_LOCKTAG_RELATION(tag, relid->dbId, relid->relId);

    // Acquire the lock
    res = LockAcquireExtended(&tag, lockmode, false, false, true, &locallock);

    // Process invalidation messages if lock was newly acquired
    if (res != LOCKACQUIRE_ALREADY_CLEAR) {
        AcceptInvalidationMessages();
        MarkLockClear(locallock);
    }
}
```