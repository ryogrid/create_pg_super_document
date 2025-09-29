# CheckTargetForConflictsIn

## Location
[src/backend/storage/lmgr/predicate.c:4156-4325](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L4156-L4325)

## Overview
CheckTargetForConflictsIn is a static helper function that checks a specific target for read-write dependency conflicts in PostgreSQL's serializable snapshot isolation implementation.

## Definition

```c
static void
CheckTargetForConflictsIn(PREDICATELOCKTARGETTAG *targettag)
```
## Detailed Description
This function is a subroutine of CheckForSerializableConflictIn() that examines a particular predicate lock target to detect serializable conflicts. It searches for existing predicate locks on the target that could create read-write dependencies with the current serializable transaction.

The function performs several key operations:
1. Locates the target in the predicate lock target hash table using the target tag hash
2. Iterates through all predicate locks held on that target by other transactions
3. For each conflicting transaction, flags a read-write conflict if conditions are met
4. Optimizes by removing redundant SIREAD locks when acquiring write locks on tuples

The conflict detection logic ensures that:
- Only active (non-doomed) transactions are considered for conflicts
- Committed transactions are only considered if they finished after the current transaction's snapshot xmin
- Conflicts are only flagged if they don't already exist

## Parameters / Member Variables
- : A pointer to PREDICATELOCKTARGETTAG structure identifying the specific target (relation, page, or tuple) to check for conflicts

## Dependencies
- Functions called/Symbols referenced:
  - PredicateLockTargetTagHashCode
  - PredicateLockHashPartitionLock
  - [hash_search_with_hash_value](../h/hash_search_with_hash_value.md)
  - [IsSubTransaction](../I/IsSubTransaction.md)
  - SxactIsDoomed
  - SxactIsCommitted
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - [GetTransactionSnapshot](../G/GetTransactionSnapshot.md)
  - [RWConflictExists](../R/RWConflictExists.md)
  - [FlagRWConflict](../F/FlagRWConflict.md)
  - [RemoveTargetIfNoLongerUsed](../R/RemoveTargetIfNoLongerUsed.md)
  - [DecrementParentLocks](../D/DecrementParentLocks.md)
- Called from (representative examples):
  - [CheckForSerializableConflictIn](CheckForSerializableConflictIn.md)

## Notes and Other Information
- This is a static function internal to predicate.c, part of PostgreSQL's serializable snapshot isolation implementation
- The function includes an optimization to remove redundant SIREAD locks when acquiring write locks on tuples, but only outside of subtransactions to avoid rollback issues
- Uses multiple LWLock acquisitions with careful lock ordering to prevent deadlocks
- The function handles parallel mode by acquiring additional per-transaction predicate list locks when necessary
- Located in src/backend/storage/lmgr/predicate.c:4156-4325

## Simplified Source

```c
static void CheckTargetForConflictsIn(PREDICATELOCKTARGETTAG *targettag) {
    // Calculate hash and get partition lock for the target
    uint32 targettaghash = PredicateLockTargetTagHashCode(targettag);
    LWLock *partitionLock = PredicateLockHashPartitionLock(targettaghash);

    // Find the target in the hash table
    LWLockAcquire(partitionLock, LW_SHARED);
    PREDICATELOCKTARGET *target = hash_search_with_hash_value(
        PredicateLockTargetHash, targettag, targettaghash, HASH_FIND, NULL);

    if (!target) {
        // No locks on this target
        LWLockRelease(partitionLock);
        return;
    }

    // Check each predicate lock on this target for conflicts
    PREDICATELOCK *mypredlock = NULL;
    LWLockAcquire(SerializableXactHashLock, LW_SHARED);

    foreach_predicate_lock_on_target(target) {
        SERIALIZABLEXACT *sxact = predlock->tag.myXact;

        if (sxact == MySerializableXact) {
            // Our own lock - mark for potential removal if writing to tuple
            if (!IsSubTransaction() && target_is_tuple(targettag)) {
                mypredlock = predlock;
            }
        }
        else if (transaction_needs_conflict_check(sxact)) {
            // Flag conflict with this transaction
            upgrade_to_exclusive_lock();
            FlagRWConflict(sxact, MySerializableXact);
            downgrade_to_shared_lock();
        }
    }

    LWLockRelease(SerializableXactHashLock);
    LWLockRelease(partitionLock);

    // Remove our own SIREAD lock if we're getting a write lock
    if (mypredlock != NULL) {
        remove_predicate_lock_safely(mypredlock, target, targettaghash);
    }
}
```