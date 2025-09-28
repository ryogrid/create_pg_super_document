# UnlockTuple

## Location
[src/backend/storage/lmgr/lmgr.c:595-615](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lmgr.c#L595-L615)

## Overview
UnlockTuple releases a lock on a specific tuple (row) within a PostgreSQL relation, allowing other transactions to access the tuple according to the lock mode that was previously held.

## Definition

```c
void
UnlockTuple(Relation relation, ItemPointer tid, LOCKMODE lockmode)
```
## Detailed Description
UnlockTuple is a lock management function that releases a previously acquired lock on a specific tuple identified by its ItemPointer (TID - Tuple Identifier). The function constructs a LOCKTAG specifically for tuple-level locking using the relation's database ID, relation ID, block number, and offset number from the tuple's ItemPointer. It then calls LockRelease to perform the actual unlock operation.

This function is part of PostgreSQL's hierarchical locking system and is used extensively during DML operations (UPDATE, DELETE) and DDL operations that need to lock specific tuples while allowing concurrent access to other tuples in the same relation.

## Parameters / Member Variables
- `relation`: Pointer to the Relation structure representing the table/relation containing the tuple
- `tid`: ItemPointer (TID) that uniquely identifies the tuple within the relation (contains block number and offset)
- `lockmode`: The lock mode to release (must match the mode that was previously acquired)

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_TUPLE (macro to construct tuple-specific lock tag)
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md) (extracts block number from ItemPointer)
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md) (extracts offset number from ItemPointer)
  - [LockRelease](../L/LockRelease.md) (performs the actual lock release operation)
- Called from (representative examples):
  - UnlockTupleTuplock (heap access method)
  - [heap_inplace_lock](../h/heap_inplace_lock.md)/heap_inplace_unlock (heap tuple operations)
  - [ExecUpdate](../E/ExecUpdate.md)/ExecMergeMatched (executor for UPDATE/MERGE operations)
  - Various DDL operations (ALTER TABLE, RENAME, etc.)

## Notes and Other Information
- This function must be called with the exact same lockmode that was used to acquire the lock
- The function operates at the tuple level, providing fine-grained concurrency control
- It's commonly used in conjunction with LockTuple for tuple-level locking during row modifications
- The lock tag construction ensures proper identification of the specific tuple across the entire database cluster
- Part of the lock manager subsystem (lmgr) that handles PostgreSQL's sophisticated locking hierarchy

## Simplified Source

```c
// Simplified version of UnlockTuple
void UnlockTuple(Relation relation, ItemPointer tid, LOCKMODE lockmode) {
    LOCKTAG tag;

    // Construct tuple-specific lock tag
    SET_LOCKTAG_TUPLE(tag,
                      relation->rd_lockInfo.lockRelId.dbId,
                      relation->rd_lockInfo.lockRelId.relId,
                      ItemPointerGetBlockNumber(tid),
                      ItemPointerGetOffsetNumber(tid));

    // Release the lock
    LockRelease(&tag, lockmode, false);
}
```

Key simplifications made:
- Preserved the essential lock tag construction logic
- Maintained the tuple identification mechanism
- Added clear comments explaining the unlock process
- Focused on the core tuple-level lock release functionality