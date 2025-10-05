# UnlockRelation

## Location
[src/backend/storage/lmgr/lmgr.c:310-329](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lmgr.c#L310-L329)

## Overview
UnlockRelation is a convenience function for unlocking a relation without closing it, using the relation's built-in lock information.

## Definition
```c
void UnlockRelation(Relation relation, LOCKMODE lockmode)
```

## Detailed Description
This function releases a lock on an already-open relation without closing the relation itself. It extracts the LockRelId information from the relation's rd_lockInfo field, creates a LOCKTAG using SET_LOCKTAG_RELATION, and calls LockRelease to perform the actual unlock operation. This is a convenience wrapper that simplifies unlocking when you have a Relation structure available, as it automatically handles the lock tag creation from the relation's stored lock information.

## Parameters / Member Variables
- `relation`: Pointer to an open Relation structure containing the relation information and lock details
- `lockmode`: The lock mode to release (e.g., AccessShareLock, RowExclusiveLock, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_RELATION (macro to set up lock tag for relation)
  - [LockRelease](../L/LockRelease.md) (performs the actual lock release)
- Types referenced:
  - [Relation](../R/Relation.md) (relation descriptor structure)
  - [LOCKTAG](../L/LOCKTAG.md) (lock tag structure)
  - LOCKMODE (enumeration of lock modes)
- Called from (representative examples):
  - [lazy_truncate_heap](../l/lazy_truncate_heap.md) (in vacuumlazy.c:2625, 2641, 2657)

## Notes and Other Information
- This is a convenience function that automatically extracts lock information from the Relation structure
- The function sets sessionLock parameter to false in LockRelease, indicating this is not a session-level lock
- Commonly used in vacuum operations where relations need to be unlocked without closing
- Part of PostgreSQL's lock manager subsystem located in src/backend/storage/lmgr/lmgr.c
- Provides a simpler interface than UnlockRelationId when a Relation structure is available
- The relation remains open after unlocking, allowing continued access with potentially different lock modes

## Simplified Source

```c
void UnlockRelation(Relation relation, LOCKMODE lockmode) {
    LOCKTAG tag;

    // Set up lock tag from relation information
    SET_LOCKTAG_RELATION(tag,
                         relation->rd_lockInfo.lockRelId.dbId,
                         relation->rd_lockInfo.lockRelId.relId);

    // Release the lock
    LockRelease(&tag, lockmode, false);
}
```