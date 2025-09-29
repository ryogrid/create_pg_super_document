# ReleaseOneSerializableXact

## Location
[src/backend/storage/lmgr/predicate.c:3825-3961](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L3825-L3961)

## Overview
Releases and cleans up resources associated with a serializable transaction, including predicate locks, conflicts, and transaction records, with options for partial cleanup or summarization.

## Definition
static void ReleaseOneSerializableXact(SERIALIZABLEXACT *sxact, bool partial, bool summarize)

## Detailed Description
This function is the primary mechanism for cleaning up serializable transaction state in PostgreSQL's serializable snapshot isolation implementation. It operates in several phases:

1. **Predicate Lock Cleanup**: Removes all predicate locks held by the transaction. If summarizing, transfers these locks to the OldCommittedSxact dummy transaction with appropriate commit sequence number handling for duplicate consolidation.

2. **Conflict Cleanup**: Releases all read-write conflicts associated with the transaction. For outConflicts, this is skipped when partial=true. When summarizing, it sets summary conflict flags on related transactions.

3. **Transaction Record Cleanup**: Unless partial=true, removes the transaction ID from SerializableXidHash and releases the SERIALIZABLEXACT structure itself.

The function supports three modes:
- **Full cleanup** (partial=false, summarize=false): Complete removal of transaction and all associated state
- **Partial cleanup** (partial=true): Keeps transaction structure and outConflicts but removes locks and inConflicts  
- **Summarizing cleanup** (summarize=true): Transfers predicate locks to summary transaction and marks conflicts appropriately

## Parameters / Member Variables
- : Pointer to the SERIALIZABLEXACT structure to be released
- : When true, keeps the transaction entry and outConflicts but releases locks and inConflicts
- : When true, transfers predicate locks to OldCommittedSxact for space management

## Dependencies
- Functions called/Symbols referenced:
  - SxactIsRolledBack
  - SxactIsCommitted
  - SxactIsOnFinishedList
  - [LWLockHeldByMe](../L/LWLockHeldByMe.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - [IsInParallelMode](../I/IsInParallelMode.md)
  - dlist_foreach_modify
  - dlist_container
  - PredicateLockTargetTagHashCode
  - PredicateLockHashPartitionLock
  - [hash_search_with_hash_value](../h/hash_search_with_hash_value.md)
  - [dlist_delete](../d/dlist_delete.md)
  - [dlist_push_tail](../d/dlist_push_tail.md)
  - [RemoveTargetIfNoLongerUsed](RemoveTargetIfNoLongerUsed.md)
  - [ReleaseRWConflict](ReleaseRWConflict.md)
  - [ReleasePredXact](ReleasePredXact.md)
- Called from:
  - [SerialControl](../S/SerialControl.md)
  - [SummarizeOldestCommittedSxact](../S/SummarizeOldestCommittedSxact.md)
  - [ReleasePredicateLocks](ReleasePredicateLocks.md)
  - [ClearOldPredicateLocks](../C/ClearOldPredicateLocks.md)

## Notes and Other Information
- Must be called with SerializableFinishedListLock held
- Handles parallel query execution by acquiring per-transaction predicate list locks when needed
- The summarize functionality is crucial for preventing memory exhaustion in systems with many old committed transactions
- When summarizing, duplicate predicate locks on the same target are consolidated by keeping the latest commitSeqNo
- Error handling includes out-of-memory conditions when creating summary predicate locks
- Located at src/backend/storage/lmgr/predicate.c:3825

## Simplified Source

```c
// Simplified version of ReleaseOneSerializableXact
static void
ReleaseOneSerializableXact(SERIALIZABLEXACT *sxact, bool partial, bool summarize)
{
    SERIALIZABLEXIDTAG sxidtag;
    dlist_mutable_iter iter;

    // Validate transaction state and lock requirements
    Assert(sxact != NULL);
    Assert(SxactIsRolledBack(sxact) || SxactIsCommitted(sxact));
    Assert(LWLockHeldByMe(SerializableFinishedListLock));

    // Phase 1: Release all predicate locks held by this transaction
    LWLockAcquire(SerializablePredicateListLock, LW_SHARED);
    if (IsInParallelMode())
        LWLockAcquire(&sxact->perXactPredicateListLock, LW_EXCLUSIVE);

    dlist_foreach_modify(iter, &sxact->predicateLocks)
    {
        PREDICATELOCK *predlock = dlist_container(PREDICATELOCK, xactLink, iter.cur);
        PREDICATELOCKTARGET *target = predlock->tag.myTarget;

        // Get partition lock for this predicate lock target
        uint32 targettaghash = PredicateLockTargetTagHashCode(&target->tag);
        LWLock *partitionLock = PredicateLockHashPartitionLock(targettaghash);

        LWLockAcquire(partitionLock, LW_EXCLUSIVE);

        // Remove from target's lock list and hash table
        dlist_delete(&predlock->targetLink);
        hash_search_with_hash_value(PredicateLockHash, &predlock->tag,
                                   targettaghash, HASH_REMOVE, NULL);

        if (summarize) {
            // Transfer lock to OldCommittedSxact for summarization
            TransferLockToSummaryTransaction(predlock, target, sxact->commitSeqNo);
        } else {
            // Clean up target if no longer referenced
            RemoveTargetIfNoLongerUsed(target, targettaghash);
        }

        LWLockRelease(partitionLock);
    }

    // Clear the predicate locks list
    dlist_init(&sxact->predicateLocks);

    if (IsInParallelMode())
        LWLockRelease(&sxact->perXactPredicateListLock);
    LWLockRelease(SerializablePredicateListLock);

    // Phase 2: Release read-write conflicts
    LWLockAcquire(SerializableXactHashLock, LW_EXCLUSIVE);

    // Release outgoing conflicts (unless partial cleanup)
    if (!partial) {
        dlist_foreach_modify(iter, &sxact->outConflicts) {
            RWConflict conflict = dlist_container(RWConflictData, outLink, iter.cur);
            if (summarize)
                conflict->sxactIn->flags |= SXACT_FLAG_SUMMARY_CONFLICT_IN;
            ReleaseRWConflict(conflict);
        }
    }

    // Release incoming conflicts
    dlist_foreach_modify(iter, &sxact->inConflicts) {
        RWConflict conflict = dlist_container(RWConflictData, inLink, iter.cur);
        if (summarize)
            conflict->sxactOut->flags |= SXACT_FLAG_SUMMARY_CONFLICT_OUT;
        ReleaseRWConflict(conflict);
    }

    // Phase 3: Final cleanup (unless partial)
    if (!partial) {
        // Remove transaction ID from hash table
        sxidtag.xid = sxact->topXid;
        if (sxidtag.xid != InvalidTransactionId)
            hash_search(SerializableXidHash, &sxidtag, HASH_REMOVE, NULL);

        // Release the transaction structure itself
        ReleasePredXact(sxact);
    }

    LWLockRelease(SerializableXactHashLock);
}

// Helper function representing the summarization logic
static void
TransferLockToSummaryTransaction(PREDICATELOCK *predlock, PREDICATELOCKTARGET *target,
                                uint64 commitSeqNo)
{
    // Create or find existing lock in OldCommittedSxact
    predlock->tag.myXact = OldCommittedSxact;
    PREDICATELOCK *summaryLock = hash_search_with_hash_value(PredicateLockHash,
                                                           &predlock->tag,
                                                           targettaghash,
                                                           HASH_ENTER_NULL, &found);

    if (found) {
        // Update with latest commit sequence number
        if (summaryLock->commitSeqNo < commitSeqNo)
            summaryLock->commitSeqNo = commitSeqNo;
    } else {
        // Add new summary lock
        dlist_push_tail(&target->predicateLocks, &summaryLock->targetLink);
        dlist_push_tail(&OldCommittedSxact->predicateLocks, &summaryLock->xactLink);
        summaryLock->commitSeqNo = commitSeqNo;
    }
}
```

Key simplifications made:
- Removed detailed error handling and memory allocation checks for clarity
- Abstracted the complex predicate lock transfer logic into a helper function concept
- Simplified hash table operations by removing detailed hash code calculations
- Consolidated similar conflict handling patterns
- Added high-level comments explaining the three main phases
- Removed platform-specific parallel processing details while preserving the logic flow
- Simplified variable declarations and complex nested operations