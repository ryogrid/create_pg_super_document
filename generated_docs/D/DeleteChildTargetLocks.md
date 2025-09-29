# DeleteChildTargetLocks

## Location
[src/backend/storage/lmgr/predicate.c:2204-2278](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L2204-L2278)

## Overview
Deletes child target locks owned by the current process that are covered by a new target, implementing lock promotion optimization in the predicate locking system.

## Definition

```c
static void
DeleteChildTargetLocks(const PREDICATELOCKTARGETTAG *newtargettag)
```
## Detailed Description
DeleteChildTargetLocks is a static function in PostgreSQL's predicate locking system that removes child (more granular) predicate locks when a parent (less granular) lock is acquired. This implements lock promotion, where acquiring a coarser lock (like a page lock) makes finer locks (like tuple locks) redundant. The function iterates through all predicate locks held by the current serializable transaction and removes any locks whose targets are covered by the new target.

The function handles both normal and parallel execution modes, acquiring appropriate locks to ensure thread safety. In parallel mode, it acquires the per-transaction predicate list lock to coordinate with worker processes. For each lock that should be removed, it properly cleans up all associated data structures including removing entries from hash tables and linked lists.

This optimization is crucial for performance, preventing the accumulation of redundant fine-grained locks when coarser locks are sufficient, thereby reducing memory usage and lock checking overhead.

## Parameters / Member Variables
- : Pointer to the PREDICATELOCKTARGETTAG representing the new (typically coarser) lock target. Child locks whose targets are covered by this new target will be deleted.

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - [IsInParallelMode](../I/IsInParallelMode.md)
  - dlist_foreach_modify
  - dlist_container
  - TargetTagIsCoveredBy
  - PredicateLockTargetTagHashCode
  - PredicateLockHashPartitionLock
  - [dlist_delete](../d/dlist_delete.md)
  - [hash_search_with_hash_value](../h/hash_search_with_hash_value.md)
  - PredicateLockHashCodeFromTargetHashCode
  - [RemoveTargetIfNoLongerUsed](../R/RemoveTargetIfNoLongerUsed.md)
  - [DecrementParentLocks](DecrementParentLocks.md)
  - [SERIALIZABLEXACT](../S/SERIALIZABLEXACT.md), PREDICATELOCK (data structures)
- Called from (representative examples):
  - [SerialControl](../S/SerialControl.md)
  - [PredicateLockAcquire](../P/PredicateLockAcquire.md)

## Notes and Other Information
- This is a static function only accessible within the predicate.c file
- Assumes uniform usage of target tag fields for optimization purposes
- Handles parallel mode execution by acquiring additional locks for thread safety
- The function implements lock promotion optimization to reduce memory overhead
- Properly maintains all linked list and hash table invariants during lock removal
- Uses PG_USED_FOR_ASSERTS_ONLY to avoid unused variable warnings in non-debug builds
- Part of PostgreSQL's Serializable Snapshot Isolation (SSI) implementation
- The function only operates on locks owned by the current transaction (MySerializableXact)
- Careful lock ordering prevents deadlocks during cleanup operations

## Simplified Source

```c
static void
DeleteChildTargetLocks(const PREDICATELOCKTARGETTAG *newtargettag)
{
    SERIALIZABLEXACT *sxact;
    PREDICATELOCK *predlock;
    dlist_mutable_iter iter;

    // Acquire locks for safe access to transaction data
    LWLockAcquire(SerializablePredicateListLock, LW_SHARED);
    sxact = MySerializableXact;
    if (IsInParallelMode())
        LWLockAcquire(&sxact->perXactPredicateListLock, LW_EXCLUSIVE);

    // Iterate through all locks held by this transaction
    dlist_foreach_modify(iter, &sxact->predicateLocks)
    {
        PREDICATELOCKTAG oldlocktag;
        PREDICATELOCKTARGET *oldtarget;
        PREDICATELOCKTARGETTAG oldtargettag;

        predlock = dlist_container(PREDICATELOCK, xactLink, iter.cur);

        oldlocktag = predlock->tag;
        oldtarget = oldlocktag.myTarget;
        oldtargettag = oldtarget->tag;

        // Check if this old lock is covered by the new target
        if (TargetTagIsCoveredBy(oldtargettag, *newtargettag))
        {
            uint32 oldtargettaghash;
            LWLock *partitionLock;

            // Calculate hash and get partition lock
            oldtargettaghash = PredicateLockTargetTagHashCode(&oldtargettag);
            partitionLock = PredicateLockHashPartitionLock(oldtargettaghash);

            LWLockAcquire(partitionLock, LW_EXCLUSIVE);

            // Remove lock from all data structures
            dlist_delete(&predlock->xactLink);
            dlist_delete(&predlock->targetLink);

            hash_search_with_hash_value(PredicateLockHash,
                                       &oldlocktag,
                                       PredicateLockHashCodeFromTargetHashCode(&oldlocktag, oldtargettaghash),
                                       HASH_REMOVE, NULL);

            // Clean up target if no longer used
            RemoveTargetIfNoLongerUsed(oldtarget, oldtargettaghash);

            LWLockRelease(partitionLock);

            // Update parent lock counts
            DecrementParentLocks(&oldtargettag);
        }
    }

    // Release locks in reverse order
    if (IsInParallelMode())
        LWLockRelease(&sxact->perXactPredicateListLock);
    LWLockRelease(SerializablePredicateListLock);
}
```