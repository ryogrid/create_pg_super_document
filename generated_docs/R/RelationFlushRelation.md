# RelationFlushRelation

## Location
src/backend/utils/cache/relcache.c: 2866 - 2912

## Overview
RelationFlushRelation handles cache invalidation events by either rebuilding open relations or removing unused ones, with special handling for relations created in the current transaction.

## Definition
```c
static void RelationFlushRelation(Relation relation)
```

## Detailed Description
RelationFlushRelation is the primary entry point for processing cache invalidation events. It implements sophisticated logic to handle different relation states during invalidation:

**For relations created in current transaction**: These are always rebuilt (never flushed) to preserve their "new" status and avoid losing critical transaction-local state. The function temporarily increments the reference count during rebuild to ensure safety, even for relations with zero refcount.

**For pre-existing relations**: The function uses reference count to determine action - open relations (refcount > 0) are rebuilt in-place while unused relations (refcount = 0) are completely removed from the cache.

**During transaction abort**: When not in a valid transaction state, the function falls back to simple invalidation rather than attempting full rebuild, since catalog access required for rebuilding is not safe during abort processing.

The function serves as a critical component of PostgreSQL's cache coherency system, ensuring that invalidation events properly update or remove stale cache entries while preserving transaction semantics.

## Parameters / Member Variables
- `relation`: The Relation structure to flush or rebuild based on its state and reference count.

## Dependencies
- Functions called/Symbols referenced:
  - [RelationIncrementReferenceCount](RelationIncrementReferenceCount.md)
  - [RelationClearRelation](RelationClearRelation.md)
  - [RelationDecrementReferenceCount](RelationDecrementReferenceCount.md)
  - [RelationInvalidateRelation](RelationInvalidateRelation.md)
  - RelationHasReferenceCountZero
  - [IsTransactionState](../I/IsTransactionState.md)
  - InvalidSubTransactionId
- Called from (representative examples):
  - [RelationCacheInvalidateEntry](RelationCacheInvalidateEntry.md)

## Notes and Other Information
- Central dispatcher for cache invalidation event handling
- Preserves transaction-local relation state during invalidation
- Reference count-aware processing for optimal cache management
- Includes safety mechanisms for handling invalidation during transaction abort
- Critical for maintaining cache consistency in multi-transaction environments
- Part of PostgreSQL's shared invalidation message processing system