# RelationDestroyRelation

## Location
[src/backend/utils/cache/relcache.c:2443-2521](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L2443-L2521)

## Overview
RelationDestroyRelation physically destroys a relation cache entry and deallocates all its subsidiary data structures when the relation is no longer referenced.

## Definition
```c
static void RelationDestroyRelation(Relation relation, bool remember_tupdesc)
```

## Detailed Description
RelationDestroyRelation performs complete cleanup of a relation cache entry that is being removed from the cache. This function systematically deallocates all memory and resources associated with a cached relation, including:

1. Closing storage manager files and breaking statistical links
2. Freeing the pg_class tuple data (rd_rel)
3. Managing tuple descriptor reference counting with transaction safety
4. Deallocating all subsidiary structures (triggers, foreign keys, indexes, statistics)
5. Freeing bitmap sets for various attribute classifications
6. Cleaning up specialized contexts and caches
7. Deleting associated memory contexts

The function includes special handling for tuple descriptors during transactions to prevent dangling pointers when ALTER TABLE operations change relation structure concurrently.

## Parameters / Member Variables
- `relation`: The Relation structure to destroy. Must have zero reference count.
- `remember_tupdesc`: Boolean flag indicating whether to defer TupleDesc deallocation until end of transaction for safety against concurrent schema changes.

## Dependencies
- Functions called/Symbols referenced:
  - RelationHasReferenceCountZero
  - RelationCloseSmgr
  - pgstat_unlink_relation
  - [RememberToFreeTupleDescAtEOX](RememberToFreeTupleDescAtEOX.md)
  - [FreeTupleDesc](../F/FreeTupleDesc.md)
  - [FreeTriggerDesc](../F/FreeTriggerDesc.md)
  - [list_free_deep](../l/list_free_deep.md)
  - [list_free](../l/list_free.md)
  - [bms_free](../b/bms_free.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
- Called from (representative examples):
  - RelationCacheInsert
  - [RelationBuildDesc](RelationBuildDesc.md)
  - [RelationClearRelation](RelationClearRelation.md)

## Notes and Other Information
- Caller must ensure relation has zero reference count before calling
- Caller must already have removed the relation from the hash table
- Includes comprehensive cleanup of all relation cache data structures
- Uses reference counting for tuple descriptor management
- Provides transaction-safe tuple descriptor cleanup to handle concurrent DDL
- Part of PostgreSQL's relation cache memory management infrastructure
- Critical for preventing memory leaks in long-running database sessions