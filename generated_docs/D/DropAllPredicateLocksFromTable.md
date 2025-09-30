# DropAllPredicateLocksFromTable

## Location
[src/backend/storage/lmgr/predicate.c:2927-3112](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L2927-L3112)

## Overview
Removes all predicate locks of any granularity from a specified relation (heap or index), optionally transferring them to the corresponding heap relation for DDL operations.

## Definition

```c
structure entries for the hash table.
			 */
			oldCommitSeqNo = oldpredlock->commitSeqNo;
```
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
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - PredicateLockHashPartitionLockByIndex
  - [RemoveScratchTarget](../R/RemoveScratchTarget.md)
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
  - [LWLockRelease](../L/LWLockRelease.md)
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

## Simplified Source

```c
static void
DropAllPredicateLocksFromTable(Relation relation, bool transfer)
{
    HASH_SEQ_STATUS seqstat;
    PREDICATELOCKTARGET *oldtarget;
    PREDICATELOCKTARGET *heaptarget = NULL;
    Oid dbId, relId, heapId;
    bool isIndex;
    uint32 heaptargettaghash = 0;

    // Early exit if no serializable transactions are running
    if (!TransactionIdIsValid(PredXact->SxactGlobalXmin))
        return;

    if (!PredicateLockingNeededForRelation(relation))
        return;

    // Get relation identifiers
    dbId = relation->rd_locator.dbOid;
    relId = relation->rd_id;

    if (relation->rd_index == NULL) {
        isIndex = false;
        heapId = relId;
    } else {
        isIndex = true;
        heapId = relation->rd_index->indrelid;
    }

    // Acquire all necessary locks for exclusive access
    LWLockAcquire(SerializablePredicateListLock, LW_EXCLUSIVE);
    for (int i = 0; i < NUM_PREDICATELOCK_PARTITIONS; i++)
        LWLockAcquire(PredicateLockHashPartitionLockByIndex(i), LW_EXCLUSIVE);
    LWLockAcquire(SerializableXactHashLock, LW_EXCLUSIVE);

    // Remove scratch target if transferring locks
    if (transfer)
        RemoveScratchTarget(true);

    // Scan through all lock targets to find matches
    hash_seq_init(&seqstat, PredicateLockTargetHash);

    while ((oldtarget = (PREDICATELOCKTARGET *) hash_seq_search(&seqstat))) {
        dlist_mutable_iter iter;

        // Check if this target matches our relation
        if (GET_PREDICATELOCKTARGETTAG_RELATION(oldtarget->tag) != relId ||
            GET_PREDICATELOCKTARGETTAG_DB(oldtarget->tag) != dbId)
            continue;

        // Skip if already the right lock type
        if (transfer && !isIndex &&
            GET_PREDICATELOCKTARGETTAG_TYPE(oldtarget->tag) == PREDLOCKTAG_RELATION)
            continue;

        // Create heap relation target if needed (for transfers)
        if (transfer && heaptarget == NULL) {
            PREDICATELOCKTARGETTAG heaptargettag;
            bool found;

            SET_PREDICATELOCKTARGETTAG_RELATION(heaptargettag, dbId, heapId);
            heaptargettaghash = PredicateLockTargetTagHashCode(&heaptargettag);
            heaptarget = hash_search_with_hash_value(PredicateLockTargetHash,
                                                   &heaptargettag,
                                                   heaptargettaghash,
                                                   HASH_ENTER, &found);
            if (!found)
                dlist_init(&heaptarget->predicateLocks);
        }

        // Process all locks on this target
        dlist_foreach_modify(iter, &oldtarget->predicateLocks) {
            PREDICATELOCK *oldpredlock =
                dlist_container(PREDICATELOCK, targetLink, iter.cur);

            // Remove old lock
            SerCommitSeqNo oldCommitSeqNo = oldpredlock->commitSeqNo;
            SERIALIZABLEXACT *oldXact = oldpredlock->tag.myXact;

            dlist_delete(&(oldpredlock->xactLink));
            hash_search(PredicateLockHash, &oldpredlock->tag, HASH_REMOVE, NULL);

            // Transfer to heap relation if requested
            if (transfer) {
                PREDICATELOCKTAG newpredlocktag;
                PREDICATELOCK *newpredlock;
                bool found;

                newpredlocktag.myTarget = heaptarget;
                newpredlocktag.myXact = oldXact;

                newpredlock = (PREDICATELOCK *)
                    hash_search_with_hash_value(PredicateLockHash,
                                               &newpredlocktag,
                                               PredicateLockHashCodeFromTargetHashCode(&newpredlocktag, heaptargettaghash),
                                               HASH_ENTER, &found);

                if (!found) {
                    // Create new lock entry
                    dlist_push_tail(&(heaptarget->predicateLocks), &(newpredlock->targetLink));
                    dlist_push_tail(&(newpredlocktag.myXact->predicateLocks), &(newpredlock->xactLink));
                    newpredlock->commitSeqNo = oldCommitSeqNo;
                } else {
                    // Update existing lock with latest commit sequence
                    if (newpredlock->commitSeqNo < oldCommitSeqNo)
                        newpredlock->commitSeqNo = oldCommitSeqNo;
                }
            }
        }

        // Remove the old target
        hash_search(PredicateLockTargetHash, &oldtarget->tag, HASH_REMOVE, NULL);
    }

    // Restore scratch target and release locks
    if (transfer)
        RestoreScratchTarget(true);

    LWLockRelease(SerializableXactHashLock);
    for (int i = NUM_PREDICATELOCK_PARTITIONS - 1; i >= 0; i--)
        LWLockRelease(PredicateLockHashPartitionLockByIndex(i));
    LWLockRelease(SerializablePredicateListLock);
}
```