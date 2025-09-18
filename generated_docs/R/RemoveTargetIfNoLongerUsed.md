# RemoveTargetIfNoLongerUsed

## Location
[src/backend/storage/lmgr/predicate.c:2173-2203](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L2173-L2203)

## Overview
Checks whether the list of related predicate locks is empty for a predicate lock target, and removes the target from the hash table if it is no longer used.

## Definition


## Detailed Description
RemoveTargetIfNoLongerUsed is a static function in PostgreSQL's predicate locking system that performs cleanup of unused predicate lock targets. The function checks if a given predicate lock target still has any associated locks by examining its predicateLocks list. If the list is empty, meaning no transactions are holding locks on this target, the function removes the target from the PredicateLockTargetHash to free up memory and maintain hash table efficiency.

This function is essential for memory management in the serializable snapshot isolation system, preventing the accumulation of unused target entries that could lead to memory bloat and degraded performance. The function uses the provided hash value for efficient removal from the hash table.

## Parameters / Member Variables
- : Pointer to the PREDICATELOCKTARGET structure to potentially remove. This represents a lockable object in the predicate locking system.
- : The precomputed hash value for the target's tag, used for efficient hash table operations.

## Dependencies
- Functions called/Symbols referenced:
  - LWLockHeldByMe
  - [dlist_is_empty](../d/dlist_is_empty.md)
  - [hash_search_with_hash_value](../h/hash_search_with_hash_value.md)
  - HASH_REMOVE
  - PREDICATELOCKTARGET (data structure)
  - PG_USED_FOR_ASSERTS_ONLY (macro)
- Called from (representative examples):
  - [SerialControl](../S/SerialControl.md)
  - [DeleteChildTargetLocks](../D/DeleteChildTargetLocks.md)
  - [DeleteLockTarget](../D/DeleteLockTarget.md)
  - [TransferPredicateLocksToNewTarget](../T/TransferPredicateLocksToNewTarget.md)
  - [ClearOldPredicateLocks](../C/ClearOldPredicateLocks.md)
  - [ReleaseOneSerializableXact](ReleaseOneSerializableXact.md)
  - [CheckTargetForConflictsIn](../C/CheckTargetForConflictsIn.md)

## Notes and Other Information
- This is a static function only accessible within the predicate.c file
- The function asserts that it is called while holding the SerializablePredicateListLock
- The function only removes targets that have no remaining predicate locks (empty predicateLocks list)
- Uses PG_USED_FOR_ASSERTS_ONLY macro for the rmtarget variable to avoid unused variable warnings in non-debug builds
- Part of PostgreSQL's memory management strategy for the SSI (Serializable Snapshot Isolation) system
- The hash value parameter optimization avoids recomputing the hash during removal operations