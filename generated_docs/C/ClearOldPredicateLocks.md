# ClearOldPredicateLocks

## Location
[src/backend/storage/lmgr/predicate.c:3687-3824](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L3687-L3824)

## Overview
Cleans up old predicate locks belonging to committed transactions that are no longer relevant to any in-progress transactions, managing the lifecycle of serializable transaction state.

## Definition
static void ClearOldPredicateLocks(void)

## Detailed Description
This function performs garbage collection of predicate locks and serializable transaction state in PostgreSQL's serializable snapshot isolation implementation. It operates in two main phases:

1. **Finished Transaction Cleanup**: Iterates through the list of finished serializable transactions in commit order. For each transaction, it determines if the transaction can be completely removed or partially cleaned based on whether any active transactions might still need to reference it. Transactions that committed before any currently active transaction took its snapshot can be completely removed.

2. **Predicate Lock Cleanup**: Cleans up predicate locks stored in the dummy OldCommittedSxact transaction that summarizes locks from old transactions. Locks from transactions old enough (based on commitSeqNo) can be safely removed.

The function uses several global tracking variables like SxactGlobalXmin, HavePartialClearedThrough, and CanPartialClearThrough to determine which transactions and locks can be safely cleaned up without affecting the correctness of conflict detection.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md) 
  - dlist_foreach_modify
  - dlist_container
  - [TransactionIdPrecedesOrEquals](../T/TransactionIdPrecedesOrEquals.md)
  - [dlist_delete_thoroughly](../d/dlist_delete_thoroughly.md)
  - [ReleaseOneSerializableXact](../R/ReleaseOneSerializableXact.md)
  - SxactIsReadOnly
  - PredicateLockTargetTagHashCode
  - PredicateLockHashPartitionLock
  - [hash_search_with_hash_value](../h/hash_search_with_hash_value.md)
  - [RemoveTargetIfNoLongerUsed](../R/RemoveTargetIfNoLongerUsed.md)
- Called from:
  - [SerialControl](../S/SerialControl.md)
  - [ReleasePredicateLocks](../R/ReleasePredicateLocks.md)

## Notes and Other Information
- This function is critical for preventing memory leaks in long-running systems with many serializable transactions
- The cleanup is done in a careful order to ensure no active transactions lose access to conflict information they might need
- Uses multiple lightweight locks (SerializableFinishedListLock, SerializableXactHashLock, SerializablePredicateListLock) to coordinate with concurrent operations
- Read-only transactions can be completely removed while read-write transactions may only be partially cleaned (keeping SERIALIZABLEXACT structure but removing locks and conflicts)
- Located at src/backend/storage/lmgr/predicate.c:3687

## Simplified Source

```c
// Simplified version of ClearOldPredicateLocks
static void ClearOldPredicateLocks(void) {
    dlist_mutable_iter iter;

    // Phase 1: Clean up finished transactions
    LWLockAcquire(SerializableFinishedListLock, LW_EXCLUSIVE);
    LWLockAcquire(SerializableXactHashLock, LW_SHARED);

    dlist_foreach_modify(iter, FinishedSerializableTransactions) {
        SERIALIZABLEXACT *finishedSxact =
            dlist_container(SERIALIZABLEXACT, finishedLink, iter.cur);

        // Check if transaction is no longer interesting to any active transaction
        if (transaction_committed_before_active_snapshots(finishedSxact)) {
            // Complete removal - transaction is totally obsolete
            LWLockRelease(SerializableXactHashLock);
            dlist_delete_thoroughly(&finishedSxact->finishedLink);
            ReleaseOneSerializableXact(finishedSxact, false, false);
            LWLockAcquire(SerializableXactHashLock, LW_SHARED);
        }
        else if (can_do_partial_cleanup(finishedSxact)) {
            // Partial cleanup - keep structure but clear locks/conflicts
            LWLockRelease(SerializableXactHashLock);

            if (SxactIsReadOnly(finishedSxact)) {
                // Read-only transactions can be completely removed
                dlist_delete_thoroughly(&finishedSxact->finishedLink);
                ReleaseOneSerializableXact(finishedSxact, false, false);
            } else {
                // Read-write transactions: keep SERIALIZABLEXACT but clear locks
                ReleaseOneSerializableXact(finishedSxact, true, false);
            }

            update_cleanup_progress(finishedSxact->commitSeqNo);
            LWLockAcquire(SerializableXactHashLock, LW_SHARED);
        }
        else {
            // Still needed by active transactions
            break;
        }
    }
    LWLockRelease(SerializableXactHashLock);

    // Phase 2: Clean up old predicate locks from summarized data
    LWLockAcquire(SerializablePredicateListLock, LW_SHARED);
    dlist_foreach_modify(iter, &OldCommittedSxact->predicateLocks) {
        PREDICATELOCK *predlock =
            dlist_container(PREDICATELOCK, xactLink, iter.cur);

        if (predlock_can_be_cleaned(predlock)) {
            // Remove this old predicate lock
            remove_predicate_lock_completely(predlock);
        }
    }

    LWLockRelease(SerializablePredicateListLock);
    LWLockRelease(SerializableFinishedListLock);
}
```

Key simplifications made:
- Abstracted complex transaction age checks into helper function names
- Simplified the nested condition logic for transaction cleanup
- Consolidated predicate lock removal logic
- Removed detailed hash table manipulation for clarity
- Focused on the two main cleanup phases