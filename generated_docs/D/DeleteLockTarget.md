# DeleteLockTarget

## Location
[src/backend/storage/lmgr/predicate.c:2659-2719](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L2659-L2719)

## Overview
Removes a predicate lock target along with all associated predicate locks, performing cleanup of the predicate locking data structures when targets are no longer needed.

## Definition

```c
static void
DeleteLockTarget(PREDICATELOCKTARGET *target, uint32 targettaghash)
```
## Detailed Description
DeleteLockTarget is an internal function responsible for cleaning up predicate lock targets and their associated locks within PostgreSQL's serializable snapshot isolation system. The function performs a comprehensive cleanup operation:

1. **Lock validation**: Ensures proper locking protocol by asserting that the caller holds the required SerializablePredicateListLock and the appropriate hash partition lock
2. **Predicate lock removal**: Iterates through all predicate locks associated with the target and removes them from both the target's predicate lock list and the transaction's predicate lock list
3. **Hash table cleanup**: Removes each predicate lock from the PredicateLockHash hash table using the appropriate hash code
4. **Target cleanup**: Calls RemoveTargetIfNoLongerUsed to potentially remove the target itself if it's no longer referenced

This function is critical for maintaining the integrity of the predicate locking system by ensuring that obsolete locks and targets don't accumulate in memory.

## Parameters / Member Variables
- : Pointer to the PREDICATELOCKTARGET structure to be deleted
- : Hash value of the target's tag, used for efficient hash table operations

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockHeldByMeInMode](../L/LWLockHeldByMeInMode.md)
  - [LWLockHeldByMe](../L/LWLockHeldByMe.md)
  - PredicateLockHashPartitionLock
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - dlist_foreach_modify
  - dlist_container
  - [dlist_delete](../d/dlist_delete.md)
  - [hash_search_with_hash_value](../h/hash_search_with_hash_value.md)
  - PredicateLockHashCodeFromTargetHashCode
  - [LWLockRelease](../L/LWLockRelease.md)
  - [RemoveTargetIfNoLongerUsed](../R/RemoveTargetIfNoLongerUsed.md)
- Called from (representative examples):
  - [SerialControl](../S/SerialControl.md)
  - [TransferPredicateLocksToNewTarget](../T/TransferPredicateLocksToNewTarget.md)

## Notes and Other Information
- Static function - internal to the predicate locking subsystem
- Requires exclusive access to SerializablePredicateListLock and appropriate hash partition lock
- Part of the cleanup mechanism for the predicate locking system
- Uses double-linked lists (dlist) for efficient traversal and removal of predicate locks
- Critical for preventing memory leaks in long-running serializable transactions
- The function maintains strict locking order to avoid deadlocks during cleanup operations