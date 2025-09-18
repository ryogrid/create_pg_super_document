# index_concurrently_set_dead

## Location
src/backend/catalog/index.c: 1820 - 1880

## Overview
Performs the final invalidation stage of DROP INDEX CONCURRENTLY or REINDEX CONCURRENTLY operations, marking an index as dead to all backends before its actual removal.

## Definition
void index_concurrently_set_dead(Oid heapId, Oid indexId)

## Detailed Description
This function implements the critical final step in concurrent index operations that ensures safe removal of an index without blocking other database operations. It coordinates the transition of an index from being available for queries to being completely dead, handling predicate locks, cache invalidation, and state flag updates.

The function performs several key operations:
1. Transfers any existing predicate locks from the index to its parent heap relation, preventing conflicts during the final stages of concurrent operations
2. Updates the index's state flags (indisready and indislive) to mark it as dead using INDEX_DROP_SET_DEAD
3. Invalidates the relcache for the parent table to ensure all sessions refresh their index lists after the transaction commits
4. Maintains proper locking throughout the process with ShareUpdateExclusiveLock on both relations

This function is specifically designed for concurrent operations where the index must remain accessible for updates while being marked for removal, ensuring no queries will use the index after this point.

## Parameters / Member Variables
- : Object identifier of the parent heap relation that owns the index
- : Object identifier of the index to be marked as dead

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - [index_open](index_open.md)
  - [TransferPredicateLocksToHeapRelation](../T/TransferPredicateLocksToHeapRelation.md)
  - [index_set_state_flags](index_set_state_flags.md)
  - [CacheInvalidateRelcache](../C/CacheInvalidateRelcache.md)
  - table_close
  - [index_close](index_close.md)
  - ShareUpdateExclusiveLock
  - INDEX_DROP_SET_DEAD
  - NoLock
- Called from (representative examples):
  - [index_drop](index_drop.md)

## Notes and Other Information
- This function is part of the concurrent index operations framework and should only be called during DROP INDEX CONCURRENTLY or REINDEX CONCURRENTLY operations
- The ShareUpdateExclusiveLock acquired on both relations is held until the end of the calling transaction
- After this function executes, no new queries will use the index, though it may still be open for updates by existing transactions
- The function ensures proper cleanup of predicate locks, which is crucial for serializable transaction isolation
- Cache invalidation affects the entire table's index list, not just the specific index being dropped