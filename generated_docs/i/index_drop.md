# index_drop

## Location
[src/backend/catalog/index.c:2114-2403](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/index.c#L2114-L2403)

## Overview
Performs the complete removal of an index from the database, supporting both regular and concurrent drop operations with appropriate locking and transaction management.

## Definition
void index_drop(Oid indexId, bool concurrent, bool concurrent_lock_mode)

## Detailed Description
This function implements the core logic for dropping database indexes, handling both regular DROP INDEX and DROP INDEX CONCURRENTLY operations. It manages the complex process of safely removing an index while ensuring data consistency and avoiding conflicts with concurrent operations.

For regular drops, the function takes exclusive locks and performs immediate removal. For concurrent drops, it implements a multi-phase process that mirrors the reverse of CREATE INDEX CONCURRENTLY:

1. **Phase 1**: Marks the index as invalid (unsets indisvalid) so new queries won't use it, while keeping it available for existing transactions
2. **Transaction Commit**: Commits to make the invalid state visible to other sessions  
3. **Phase 2**: Waits for all transactions that could have used the index to complete
4. **Phase 3**: Marks the index as dead (unsets indisready and indislive) using index_concurrently_set_dead
5. **Final Commit**: Commits the dead state and waits for all remaining transactions
6. **Physical Removal**: Performs the actual catalog and storage cleanup

The function also handles predicate lock transfers, statistics removal, and cleanup of various system catalogs (pg_index, pg_attribute, pg_class, pg_inherits).

## Parameters / Member Variables
- : Object identifier of the index to be dropped
- : Whether to perform DROP INDEX CONCURRENTLY (true) or regular DROP INDEX (false) 
- : Whether to use concurrent-style locking even for non-concurrent drops (used in REINDEX CONCURRENTLY)

## Dependencies
- Functions called/Symbols referenced:
  - [get_rel_persistence](../g/get_rel_persistence.md)
  - [IndexGetRelation](../I/IndexGetRelation.md)
  - [table_open](../t/table_open.md)
  - [index_open](index_open.md)
  - [CheckTableNotInUse](../C/CheckTableNotInUse.md)
  - [GetTopTransactionIdIfAny](../G/GetTopTransactionIdIfAny.md)
  - [index_set_state_flags](index_set_state_flags.md)
  - [CacheInvalidateRelcache](../C/CacheInvalidateRelcache.md)
  - [LockRelationIdForSession](../L/LockRelationIdForSession.md)
  - [CommitTransactionCommand](../C/CommitTransactionCommand.md)
  - [StartTransactionCommand](../S/StartTransactionCommand.md)
  - [PopActiveSnapshot](../P/PopActiveSnapshot.md)
  - [WaitForLockers](../W/WaitForLockers.md)
  - [index_concurrently_set_dead](index_concurrently_set_dead.md)
  - [TransferPredicateLocksToHeapRelation](../T/TransferPredicateLocksToHeapRelation.md)
  - [RelationDropStorage](../R/RelationDropStorage.md)
  - [pgstat_drop_relation](../p/pgstat_drop_relation.md)
  - [index_close](index_close.md)
  - [RelationForgetRelation](../R/RelationForgetRelation.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [heap_attisnull](../h/heap_attisnull.md)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md)
  - [RemoveStatistics](../R/RemoveStatistics.md)
  - [DeleteAttributeTuples](../D/DeleteAttributeTuples.md)
  - [DeleteRelationTuple](../D/DeleteRelationTuple.md)
  - [DeleteInheritsTuple](../D/DeleteInheritsTuple.md)
  - [UnlockRelationIdForSession](../U/UnlockRelationIdForSession.md)
- Called from (representative examples):
  - [doDeletion](../d/doDeletion.md)

## Notes and Other Information
- Should only be called through performDeletion() to ensure associated dependencies are properly cleaned up
- Temporary relations always use non-concurrent drops since other backends cannot access them
- For concurrent drops, validates that no XID has been assigned to ensure it's the first action in the transaction
- Uses different lock modes: AccessExclusiveLock for regular drops, ShareUpdateExclusiveLock for concurrent drops
- Implements deadlock detection during the waiting phases of concurrent drops
- Transfers predicate locks from the index to the heap relation to maintain serializable isolation guarantees
- Handles expression indexes by removing associated statistics
- Updates multiple system catalogs but deliberately avoids updating relhasindex (left for VACUUM to fix)
- In concurrent mode, uses session-level locks to prevent premature relation drops during the multi-transaction process
- The concurrent algorithm ensures that at no point are there inconsistent states visible to other transactions

## Simplified Source

```c
void
index_drop(Oid indexId, bool concurrent, bool concurrent_lock_mode)
{
    Oid heapId;
    Relation userHeapRelation, userIndexRelation, indexRelation;
    HeapTuple tuple;
    bool hasexprs;
    LockRelId heaprelid, indexrelid;
    LOCKTAG heaplocktag;
    LOCKMODE lockmode;

    // Get parent table and determine lock mode
    heapId = IndexGetRelation(indexId, false);
    lockmode = (concurrent || concurrent_lock_mode) ? ShareUpdateExclusiveLock : AccessExclusiveLock;

    // Open and lock both table and index
    userHeapRelation = table_open(heapId, lockmode);
    userIndexRelation = index_open(indexId, lockmode);

    // Safety check: ensure no active use in our session
    CheckTableNotInUse(userIndexRelation, "DROP INDEX");

    if (concurrent) {
        // CONCURRENT DROP: Multi-phase process

        // Validate this is first action in transaction
        if (GetTopTransactionIdIfAny() != InvalidTransactionId)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("DROP INDEX CONCURRENTLY must be first action in transaction")));

        // Phase 1: Mark index invalid so new queries won't use it
        index_set_state_flags(indexId, INDEX_DROP_CLEAR_VALID);
        CacheInvalidateRelcache(userHeapRelation);

        // Save relation IDs and close relations (keeping locks)
        heaprelid = userHeapRelation->rd_lockInfo.lockRelId;
        SET_LOCKTAG_RELATION(heaplocktag, heaprelid.dbId, heaprelid.relId);
        indexrelid = userIndexRelation->rd_lockInfo.lockRelId;

        table_close(userHeapRelation, NoLock);
        index_close(userIndexRelation, NoLock);

        // Get session locks and commit transaction
        LockRelationIdForSession(&heaprelid, ShareUpdateExclusiveLock);
        LockRelationIdForSession(&indexrelid, ShareUpdateExclusiveLock);

        PopActiveSnapshot();
        CommitTransactionCommand();
        StartTransactionCommand();

        // Phase 2: Wait for transactions using the index to complete
        WaitForLockers(heaplocktag, AccessExclusiveLock, true);

        // Mark index as dead (indisready = false, indislive = false)
        index_concurrently_set_dead(heapId, indexId);

        // Commit and wait again for remaining transactions
        CommitTransactionCommand();
        StartTransactionCommand();
        WaitForLockers(heaplocktag, AccessExclusiveLock, true);

        // Re-open relations for final cleanup
        userHeapRelation = table_open(heapId, ShareUpdateExclusiveLock);
        userIndexRelation = index_open(indexId, AccessExclusiveLock);
    }
    else {
        // NON-CONCURRENT DROP: Transfer predicate locks immediately
        TransferPredicateLocksToHeapRelation(userIndexRelation);
    }

    // Schedule physical storage removal
    if (RELKIND_HAS_STORAGE(userIndexRelation->rd_rel->relkind))
        RelationDropStorage(userIndexRelation);

    // Clean up statistics and close index
    pgstat_drop_relation(userIndexRelation);
    index_close(userIndexRelation, NoLock);
    RelationForgetRelation(indexId);

    // Remove pg_index catalog entry
    indexRelation = table_open(IndexRelationId, RowExclusiveLock);
    tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexId));
    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "cache lookup failed for index %u", indexId);

    // Check if index has expressions (for statistics cleanup)
    hasexprs = !heap_attisnull(tuple, Anum_pg_index_indexprs,
                               RelationGetDescr(indexRelation));

    CatalogTupleDelete(indexRelation, &tuple->t_self);
    ReleaseSysCache(tuple);
    table_close(indexRelation, RowExclusiveLock);

    // Clean up expression statistics if needed
    if (hasexprs)
        RemoveStatistics(indexId, 0);

    // Remove entries from system catalogs
    DeleteAttributeTuples(indexId);
    DeleteRelationTuple(indexId);
    DeleteInheritsTuple(indexId, InvalidOid, false, NULL);

    // Invalidate relation cache and close table
    CacheInvalidateRelcache(userHeapRelation);
    table_close(userHeapRelation, NoLock);

    // Release session locks for concurrent case
    if (concurrent) {
        UnlockRelationIdForSession(&heaprelid, ShareUpdateExclusiveLock);
        UnlockRelationIdForSession(&indexrelid, ShareUpdateExclusiveLock);
    }
}
```