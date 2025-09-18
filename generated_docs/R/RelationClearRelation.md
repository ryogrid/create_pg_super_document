# RelationClearRelation

## Location
src/backend/utils/cache/relcache.c: 2561 - 2753

## Overview
RelationClearRelation either destroys a relation cache entry completely or rebuilds it from scratch depending on whether it's still in use, handling special cases for nailed relations and open indexes.

## Definition
```c
static void RelationClearRelation(Relation relation, bool rebuild)
```

## Detailed Description
RelationClearRelation is the central function for handling relation cache invalidation with two distinct operational modes. It either completely destroys an unused cache entry or performs an in-place rebuild for entries that are still actively referenced.

The function implements sophisticated logic for different relation types:
1. **Nailed relations**: Always preserved using RelationReloadNailed() since they must remain accessible
2. **Open indexes**: Protected during active use by reloading index information instead of full reconstruction  
3. **Unused relations**: Completely removed from cache and destroyed
4. **Active relations**: Rebuilt in-place using a careful swap mechanism to preserve existing references

For rebuilds, the function creates a temporary new entry, swaps its contents with the existing entry, then destroys the temporary entry. This approach ensures that existing pointers remain valid during reconstruction and provides rollback safety if errors occur during the rebuild process.

## Parameters / Member Variables
- `relation`: The Relation structure to clear or rebuild. Reference count determines the operation mode.
- `rebuild`: Boolean indicating whether to rebuild (true) or destroy (false). Must match relation's reference count status.

## Dependencies
- Functions called/Symbols referenced:
  - RelationHasReferenceCountZero
  - RelationCloseSmgr
  - [RelationReloadNailed](RelationReloadNailed.md)
  - [RelationReloadIndexInfo](RelationReloadIndexInfo.md)
  - RelationCacheDelete
  - [RelationDestroyRelation](RelationDestroyRelation.md)
  - [RelationBuildDesc](RelationBuildDesc.md)
  - [IsTransactionState](../I/IsTransactionState.md)
  - HistoricSnapshotActive
  - [equalTupleDescs](../e/equalTupleDescs.md)
  - [equalRuleLocks](../e/equalRuleLocks.md)
  - [equalRSDesc](../e/equalRSDesc.md)
  - RELKIND_INDEX
  - RELKIND_PARTITIONED_INDEX
  - InvalidSubTransactionId
- Called from (representative examples):
  - [RelationIdGetRelation](RelationIdGetRelation.md)
  - [RelationFlushRelation](RelationFlushRelation.md)
  - [RelationCacheInvalidate](RelationCacheInvalidate.md)
  - [AtEOXact_cleanup](../A/AtEOXact_cleanup.md)

## Notes and Other Information
- Central function in PostgreSQL's relation cache invalidation system
- Implements reference count-aware cache management
- Provides transaction-safe rebuild mechanism for active entries
- Special handling for system catalogs (nailed relations) and indexes
- Preserves pointer validity during rebuilds to prevent dangling references
- Includes protection against concurrent catalog changes during reconstruction
- Critical for maintaining cache consistency across DDL operations and invalidations