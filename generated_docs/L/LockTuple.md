# LockTuple

## Location
src/backend/storage/lmgr/lmgr.c: 558 - 577

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
  - ItemPointerGetBlockNumber
  - ItemPointerGetOffsetNumber
  - LockAcquire
  - LOCKTAG
- Called from (representative examples):
  - LockTupleTuplock
  - heap_inplace_lock
  - get_catalog_object_by_oid_extended
  - movedb
  - AlterDatabase
  - ExecUpdate
  - ExecMergeMatched

## Notes and Other Information
- This is the most granular level of locking in PostgreSQL (tuple-level)
- The function comment warns to see heap_lock_tuple before using, suggesting complex usage patterns
- Cannot afford separate locks for every tuple, so used in specialized scenarios
- Used extensively in DDL operations (database alterations) and DML operations (updates, merges)
- Extracts block and offset numbers from the ItemPointer to construct the unique tuple identifier
- The function always passes false for both session lock and dontWait parameters to LockAcquire