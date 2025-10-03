# CheckTableForSerializableConflictIn

## Location
[src/backend/storage/lmgr/predicate.c:4409-4490](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L4409-L4490)

## Overview
CheckTableForSerializableConflictIn handles serializable conflict detection for DDL operations that perform mass deletion like TRUNCATE or DROP TABLE on entire tables.

## Definition

```c
void
CheckTableForSerializableConflictIn(Relation relation)
```
## Detailed Description
This function detects serializable conflicts when performing DDL-style logical mass delete operations such as TRUNCATE or DROP TABLE. These operations can occur within serializable transactions and must be properly serialized under SSI (Serializable Snapshot Isolation).

The function handles the unique characteristics of mass deletion operations:
- These operations don't operate entirely within snapshot isolation bounds but must maintain serializability
- They logically occur after any reads that saw rows destroyed by these operations
- Any predicate lock of any granularity on the heap relation creates a read-write conflict
- Index predicate locks are ignored since mass deletion will also truncate/drop indexes

The function performs a comprehensive scan of all predicate lock targets:
1. Acquires all necessary locks in proper order to prevent deadlocks
2. Scans through all predicate lock targets in the hash table
3. For matching targets (same database and relation), flags conflicts with other serializable transactions
4. Releases locks in reverse acquisition order

An important optimization allows early exit when no serializable transactions are running, since the caller holds ACCESS EXCLUSIVE lock preventing new relevant locks from being acquired.

## Parameters / Member Variables
- `relation`: The heap relation being truncated or dropped (must be a heap relation, not an index)
## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdIsValid
  - [SerializationNeededForWrite](../S/SerializationNeededForWrite.md)
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - GET_PREDICATELOCKTARGETTAG_RELATION
  - GET_PREDICATELOCKTARGETTAG_DB
  - [RWConflictExists](../R/RWConflictExists.md)
  - [FlagRWConflict](../F/FlagRWConflict.md)
  - PredicateLockHashPartitionLockByIndex
- Called from (representative examples):
  - [heap_drop_with_catalog](../h/heap_drop_with_catalog.md)
  - [ExecuteTruncateGuts](../E/ExecuteTruncateGuts.md)

## Notes and Other Information
- This is a public function exported via predicate.h for use by DDL operations
- The function assumes the caller holds ACCESS EXCLUSIVE lock on the relation
- Currently does not actually drop existing predicate locks on truncated/dropped tables (noted as potential source of false positives)
- Uses comprehensive locking strategy: acquires all partition locks and serializable xact hash lock
- Only processes heap relations; index relations are explicitly rejected with assertion
- Sets MyXactDidWrite=true to track that the transaction has performed writes
- Located in src/backend/storage/lmgr/predicate.c:4409-4490

## Simplified Source

```c
void CheckTableForSerializableConflictIn(Relation relation)
{
    // Early exit if no serializable transactions are running
    if (!TransactionIdIsValid(PredXact->SxactGlobalXmin))
        return;

    if (!SerializationNeededForWrite(relation))
        return;

    // Mark that this transaction has performed writes
    MyXactDidWrite = true;

    Oid dbId = relation->rd_locator.dbOid;
    Oid heapId = relation->rd_id;

    // Acquire all necessary locks to prevent deadlocks
    LWLockAcquire(SerializablePredicateListLock, LW_EXCLUSIVE);
    for (int i = 0; i < NUM_PREDICATELOCK_PARTITIONS; i++)
        LWLockAcquire(PredicateLockHashPartitionLockByIndex(i), LW_SHARED);
    LWLockAcquire(SerializableXactHashLock, LW_EXCLUSIVE);

    // Scan through all predicate lock targets
    HASH_SEQ_STATUS seqstat;
    hash_seq_init(&seqstat, PredicateLockTargetHash);

    PREDICATELOCKTARGET *target;
    while ((target = (PREDICATELOCKTARGET *) hash_seq_search(&seqstat)))
    {
        // Check if this target matches our relation
        if (GET_PREDICATELOCKTARGETTAG_RELATION(target->tag) != heapId ||
            GET_PREDICATELOCKTARGETTAG_DB(target->tag) != dbId)
            continue;

        // Flag conflicts for all predicate locks on this target
        dlist_mutable_iter iter;
        dlist_foreach_modify(iter, &target->predicateLocks)
        {
            PREDICATELOCK *predlock =
                dlist_container(PREDICATELOCK, targetLink, iter.cur);

            if (predlock->tag.myXact != MySerializableXact &&
                !RWConflictExists(predlock->tag.myXact, MySerializableXact))
            {
                FlagRWConflict(predlock->tag.myXact, MySerializableXact);
            }
        }
    }

    // Release locks in reverse order
    LWLockRelease(SerializableXactHashLock);
    for (int i = NUM_PREDICATELOCK_PARTITIONS - 1; i >= 0; i--)
        LWLockRelease(PredicateLockHashPartitionLockByIndex(i));
    LWLockRelease(SerializablePredicateListLock);
}
```