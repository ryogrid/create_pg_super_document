# ConditionalLockRelationOid

## Location
[src/backend/storage/lmgr/lmgr.c:151-183](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lmgr.c#L151-L183)

## Overview
ConditionalLockRelationOid attempts to acquire a lock on a relation using only its OID, but only if the lock can be obtained without blocking, returning success/failure status.

## Definition
```c
bool ConditionalLockRelationOid(Oid relid, LOCKMODE lockmode)
```

## Detailed Description
This function is a non-blocking variant of LockRelationOid that attempts to acquire a lock on a relation but will not wait if the lock is not immediately available. This is particularly useful in scenarios where the caller needs to avoid deadlocks or wants to implement timeout-based locking strategies.

The function follows the same basic pattern as LockRelationOid: it creates a LOCKTAG using SetLocktagRelationOid(), then calls LockAcquireExtended() with the conditional flag set to true. If the lock cannot be acquired immediately (LOCKACQUIRE_NOT_AVAIL is returned), the function returns false.

When the lock is successfully acquired, the function performs the same invalidation message processing as LockRelationOid to ensure relcache consistency, calling AcceptInvalidationMessages() and MarkLockClear() if needed.

The function is designed to be used in situations where non-blocking behavior is essential, such as in autovacuum processes, utility commands, or when implementing lock-ordering protocols to prevent deadlocks.

## Parameters / Member Variables
- `relid`: The OID of the relation to lock
- `lockmode`: The type of lock to acquire (e.g., AccessShareLock, RowExclusiveLock, AccessExclusiveLock)

## Dependencies
- Functions called/Symbols referenced:
  - [LOCKTAG](../L/LOCKTAG.md)
  - [LOCALLOCK](../L/LOCALLOCK.md)
  - LockAcquireResult
  - [SetLocktagRelationOid](../S/SetLocktagRelationOid.md)
  - [LockAcquireExtended](../L/LockAcquireExtended.md)
  - LOCKACQUIRE_NOT_AVAIL
  - LOCKACQUIRE_ALREADY_CLEAR
  - [AcceptInvalidationMessages](../A/AcceptInvalidationMessages.md)
  - [MarkLockClear](../M/MarkLockClear.md)
- Called from (representative examples):
  - [RangeVarGetRelidExtended](../R/RangeVarGetRelidExtended.md) (src/backend/catalog/namespace.c:593)
  - [LockTableRecurse](../L/LockTableRecurse.md) (src/backend/commands/lockcmds.c:134)
  - [vacuum_open_relation](../v/vacuum_open_relation.md) (src/backend/commands/vacuum.c:789)
  - [do_autovacuum](../d/do_autovacuum.md) (src/backend/postmaster/autovacuum.c:2162)
  - [AlterTableMoveAll](../A/AlterTableMoveAll.md) (src/backend/commands/tablecmds.c:15503)

## Notes and Other Information
- Returns true if lock was successfully acquired, false if lock was not available
- Non-blocking version of LockRelationOid - will not wait for conflicting locks
- Performs same invalidation message processing as LockRelationOid when lock is acquired
- Useful for deadlock avoidance and timeout-based locking strategies
- Particularly important for autovacuum and utility commands that should not block indefinitely
- Could easily be extended to other LockXXX routines if needed (as noted in comments)
- Part of the lock manager (lmgr) subsystem located in src/backend/storage/lmgr/lmgr.c:151-183