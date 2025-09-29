# LockTuple

## Location
[src/backend/storage/lmgr/lmgr.c:558-577](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lmgr.c#L558-L577)

## Overview
Obtain a tuple-level lock for a specific tuple identified by its ItemPointer, used in specialized scenarios where fine-grained tuple locking is required.

## Definition
```c
void LockTuple(Relation relation, ItemPointer tid, LOCKMODE lockmode)
```

## Detailed Description
LockTuple is a PostgreSQL locking function that acquires a tuple-level lock on a specific tuple within a relation. This function is used in a specialized manner because PostgreSQL cannot afford to keep a separate lock in shared memory for every tuple. The function constructs a lock tag using the relations database ID, relation ID, and the tuples block number and offset number extracted from the ItemPointer. It then acquires the lock using the specified lock mode through the lower-level LockAcquire function. The comment specifically mentions to see heap_lock_tuple before using this function, indicating there are important usage considerations.

## Parameters / Member Variables
- `relation`: The relation (table) containing the tuple to be locked
- `tid`: ItemPointer that uniquely identifies the tuple (contains block number and offset number)
- `lockmode`: The type of lock to acquire (e.g., AccessShareLock, ExclusiveLock)

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_TUPLE
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md)
  - [LockAcquire](LockAcquire.md)
  - [LOCKTAG](LOCKTAG.md)
- Called from (representative examples):
  - LockTupleTuplock
  - [heap_inplace_lock](../h/heap_inplace_lock.md)
  - [get_catalog_object_by_oid_extended](../g/get_catalog_object_by_oid_extended.md)
  - [movedb](../m/movedb.md)
  - [AlterDatabase](../A/AlterDatabase.md)
  - [ExecUpdate](../E/ExecUpdate.md)
  - [ExecMergeMatched](../E/ExecMergeMatched.md)

## Notes and Other Information
- This is the most granular level of locking in PostgreSQL (tuple-level)
- The function comment warns to see heap_lock_tuple before using, suggesting complex usage patterns
- Cannot afford separate locks for every tuple, so used in specialized scenarios
- Used extensively in DDL operations (database alterations) and DML operations (updates, merges)
- Extracts block and offset numbers from the ItemPointer to construct the unique tuple identifier
- The function always passes false for both session lock and dontWait parameters to LockAcquire

## Simplified Source

```c
// Simplified version of LockTuple
void LockTuple(Relation relation, ItemPointer tid, LOCKMODE lockmode) {
    LOCKTAG tag;

    // Create a unique lock tag for this specific tuple
    // Combines database ID, relation ID, block number, and offset number
    SET_LOCKTAG_TUPLE(tag,
                      relation->rd_lockInfo.lockRelId.dbId,
                      relation->rd_lockInfo.lockRelId.relId,
                      ItemPointerGetBlockNumber(tid),
                      ItemPointerGetOffsetNumber(tid));

    // Acquire the lock with specified mode
    // Uses session=false, dontWait=false for standard blocking behavior
    LockAcquire(&tag, lockmode, false, false);
}
```

Key simplifications made:
- Added descriptive comments explaining the lock tag construction
- Clarified the purpose of combining database, relation, block, and offset identifiers
- Documented the LockAcquire parameters (session=false, dontWait=false)
- Maintained the essential tuple locking algorithm without modification
- Preserved the original function signature and core logic flow