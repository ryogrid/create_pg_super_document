# RestoreScratchTarget

## Location
src/backend/storage/lmgr/predicate.c: 2151 - 2172

## Overview
Re-inserts the dummy entry in the predicate lock target hash table, restoring a scratch target that was temporarily removed for safe manipulation.

## Definition


## Detailed Description
RestoreScratchTarget is a static function in PostgreSQL's predicate locking system that re-inserts the dummy scratch target entry into the PredicateLockTargetHash. This function is part of the serializable snapshot isolation implementation and is used to restore the scratch target after it has been temporarily removed from the hash table during lock operations. The scratch target serves as a placeholder entry that allows safe manipulation of the predicate lock target hash table.

The function ensures proper locking semantics by checking if the caller already holds the required partition lock, and acquires it if necessary. It uses hash_search_with_hash_value to insert the scratch target back into the hash table with the HASH_ENTER action, asserting that the entry should not already exist.

## Parameters / Member Variables
- : Boolean flag indicating whether the caller already holds the ScratchPartitionLock. If false, the function will acquire the lock; if true, it assumes the lock is already held.

## Dependencies
- Functions called/Symbols referenced:
  - LWLockHeldByMe
  - LWLockAcquire
  - LWLockRelease
  - [hash_search_with_hash_value](../h/hash_search_with_hash_value.md)
  - HASH_ENTER
- Called from (representative examples):
  - [SerialControl](../S/SerialControl.md)
  - [TransferPredicateLocksToNewTarget](../T/TransferPredicateLocksToNewTarget.md)
  - [DropAllPredicateLocksFromTable](../D/DropAllPredicateLocksFromTable.md)

## Notes and Other Information
- This is a static function only accessible within the predicate.c file
- The function asserts that it is called while holding the SerializablePredicateListLock
- The scratch target mechanism is crucial for safe manipulation of the predicate lock hash table
- The function expects that the scratch target does not already exist in the hash table (asserts !found)
- Part of PostgreSQL's Serializable Snapshot Isolation (SSI) implementation for preventing serialization anomalies