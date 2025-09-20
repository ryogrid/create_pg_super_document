# CheckForSerializableConflictIn

## Location
[src/backend/storage/lmgr/predicate.c:4326-4408](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L4326-L4408)

## Overview
CheckForSerializableConflictIn detects serializable conflicts when performing write operations by checking for predicate locks that would create read-write dependencies.

## Definition

```c
void
CheckForSerializableConflictIn(Relation relation, ItemPointer tid, BlockNumber blkno)
```
## Detailed Description
This function is the main entry point for checking serializable conflicts during write operations in PostgreSQL's serializable snapshot isolation. When a transaction performs a tuple update or delete, this function checks if there are existing predicate locks (SIREAD locks) that would create read-write conflicts with other serializable transactions.

The function implements a hierarchical checking strategy, examining locks at three granularity levels:
1. Tuple-level locks (if tid is provided)
2. Page-level locks (if blkno is provided) 
3. Relation-level locks (always checked)

This ordering ensures that granularity promotion doesn't cause missed locks, as coarser locks are acquired before finer ones are released.

The function performs early exit optimizations:
- Returns immediately if serialization is not needed for the relation
- Aborts with serialization failure if the current transaction is already doomed
- Sets MyXactDidWrite flag to remember that this transaction has performed writes

## Parameters / Member Variables
- : The relation being modified in the write operation
- : Item pointer to the specific tuple being written (NULL if not tuple-specific)
- : Block number of the page being written (InvalidBlockNumber if not page-specific)

## Dependencies
- Functions called/Symbols referenced:
  - [SerializationNeededForWrite](../S/SerializationNeededForWrite.md)
  - SxactIsDoomed
  - [CheckTargetForConflictsIn](CheckTargetForConflictsIn.md)
  - SET_PREDICATELOCKTARGETTAG_TUPLE
  - SET_PREDICATELOCKTARGETTAG_PAGE
  - SET_PREDICATELOCKTARGETTAG_RELATION
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md)
- Called from (representative examples):
  - [heap_insert](../h/heap_insert.md)
  - [heap_delete](../h/heap_delete.md)
  - [heap_update](../h/heap_update.md)
  - [index_insert](../i/index_insert.md)
  - [_bt_doinsert](../b/_bt_doinsert.md)
  - [_bt_check_unique](../b/_bt_check_unique.md)

## Notes and Other Information
- This is a public function exported via predicate.h and called throughout the storage layer
- The function must check granularities from finest to coarsest to handle lock promotion correctly
- Cannot hold locks across all granularity checks since targets may be in separate partitions
- Sets MyXactDidWrite=true to track that the current transaction has performed writes
- Will throw ERRCODE_T_R_SERIALIZATION_FAILURE error if transaction is doomed
- Located in src/backend/storage/lmgr/predicate.c:4326-4408