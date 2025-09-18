# DropAllPredicateLocksFromTable

## Location
src/backend/storage/lmgr/predicate.c: 2927 - 3112

## Overview
Removes all predicate locks of any granularity from a specified relation (heap or index), optionally transferring them to the corresponding heap relation for DDL operations.

## Definition


## Detailed Description
DropAllPredicateLocksFromTable is a comprehensive cleanup function that handles predicate lock management during DDL operations. The function performs an expensive but necessary operation of scanning the entire lock target table to remove locks associated with a specific relation. Key aspects include:

1. **Early bailout optimizations**: Returns immediately if no serializable transactions are running or if predicate locking is not needed for the relation
2. **Lock acquisition strategy**: Acquires all necessary locks (SerializablePredicateListLock, all partition locks, and SerializableXactHashLock) to ensure exclusive access during the operation  
3. **Comprehensive scanning**: Uses hash table sequential scan to find all lock targets matching the specified relation
4. **Lock transfer mechanism**: When transfer is true, moves all locks from the target relation to the corresponding heap relation, preserving commit sequence numbers
5. **Memory management**: Uses scratch space management to ensure successful completion of transfer operations

The function is designed specifically for DDL operations like DROP TABLE, ALTER TABLE, and similar commands that need to clean up or restructure predicate locks.

## Parameters / Member Variables
- : The relation (heap table or index) from which to drop predicate locks
- : Boolean flag indicating whether to transfer locks to the heap relation (true) or simply drop them (false)

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdIsValid (via PredXact->SxactGlobalXmin check)
  - [PredicateLockingNeededForRelation](../P/PredicateLockingNeededForRelation.md)
  - LWLockAcquire
  - PredicateLockHashPartitionLockByIndex
  - RemoveScratchTarget
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - GET_PREDICATELOCKTARGETTAG_RELATION
  - GET_PREDICATELOCKTARGETTAG_DB
  - GET_PREDICATELOCKTARGETTAG_TYPE
  - SET_PREDICATELOCKTARGETTAG_RELATION
  - PredicateLockTargetTagHashCode
  - [hash_search_with_hash_value](../h/hash_search_with_hash_value.md)
  - [dlist_init](../d/dlist_init.md)
  - dlist_foreach_modify
  - dlist_container
  - [dlist_delete](../d/dlist_delete.md)
  - [hash_search](../h/hash_search.md)
  - [dlist_push_tail](../d/dlist_push_tail.md)
  - PredicateLockHashCodeFromTargetHashCode
  - [RestoreScratchTarget](../R/RestoreScratchTarget.md)
  - LWLockRelease
- Called from (representative examples):
  - [TransferPredicateLocksToHeapRelation](../T/TransferPredicateLocksToHeapRelation.md)
  - [SerialControl](../S/SerialControl.md)

## Notes and Other Information
- Static function - internal to the predicate locking subsystem
- More expensive than most predicate lock functions due to full table scanning, but only called during expensive DDL operations
- Currently only called with transfer=true, but designed to support transfer=false for potential future use (e.g., DROP TABLE cleanup)
- Cannot throw errors as it may be called from non-serializable transactions  
- Requires ACCESS EXCLUSIVE lock on the relation by caller, ensuring no new conflicting locks can be acquired
- Handles both heap relations and index relations, with special logic for index-to-heap transfers
- Uses scratch space mechanism to guarantee successful completion when transferring locks
- Part of the infrastructure supporting safe DDL operations in serializable transactions