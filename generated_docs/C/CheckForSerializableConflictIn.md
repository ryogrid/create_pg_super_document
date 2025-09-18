# CheckForSerializableConflictIn

## Location
src/backend/storage/lmgr/predicate.c: 4326 - 4408

## Overview
CheckForSerializableConflictIn detects serializable conflicts when performing write operations by checking for predicate locks that would create read-write dependencies.

## Definition


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
  - SerializationNeededForWrite
  - SxactIsDoomed
  - CheckTargetForConflictsIn
  - SET_PREDICATELOCKTARGETTAG_TUPLE
  - SET_PREDICATELOCKTARGETTAG_PAGE
  - SET_PREDICATELOCKTARGETTAG_RELATION
  - ItemPointerGetBlockNumber
  - ItemPointerGetOffsetNumber
- Called from (representative examples):
  - heap_insert
  - heap_delete
  - heap_update
  - index_insert
  - _bt_doinsert
  - _bt_check_unique

## Notes and Other Information
- This is a public function exported via predicate.h and called throughout the storage layer
- The function must check granularities from finest to coarsest to handle lock promotion correctly
- Cannot hold locks across all granularity checks since targets may be in separate partitions
- Sets MyXactDidWrite=true to track that the current transaction has performed writes
- Will throw ERRCODE_T_R_SERIALIZATION_FAILURE error if transaction is doomed
- Located in src/backend/storage/lmgr/predicate.c:4326-4408