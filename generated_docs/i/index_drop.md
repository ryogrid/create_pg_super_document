# index_drop

## Location
src/backend/catalog/index.c: 2114 - 2403

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
  - get_rel_persistence
  - IndexGetRelation
  - table_open
  - index_open
  - CheckTableNotInUse
  - GetTopTransactionIdIfAny
  - index_set_state_flags
  - CacheInvalidateRelcache
  - LockRelationIdForSession
  - CommitTransactionCommand
  - StartTransactionCommand
  - PopActiveSnapshot
  - WaitForLockers
  - index_concurrently_set_dead
  - TransferPredicateLocksToHeapRelation
  - RelationDropStorage
  - pgstat_drop_relation
  - index_close
  - RelationForgetRelation
  - SearchSysCache1
  - heap_attisnull
  - CatalogTupleDelete
  - RemoveStatistics
  - DeleteAttributeTuples
  - DeleteRelationTuple
  - DeleteInheritsTuple
  - UnlockRelationIdForSession
- Called from (representative examples):
  - doDeletion

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