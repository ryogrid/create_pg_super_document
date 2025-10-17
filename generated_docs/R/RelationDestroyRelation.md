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
  - [RelationCloseSmgr](RelationCloseSmgr.md)
  - [pgstat_unlink_relation](../p/pgstat_unlink_relation.md)
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

## Simplified Source

```c
static void RelationDestroyRelation(Relation relation, bool remember_tupdesc) {
    // Ensure relation has zero references
    Assert(RelationHasReferenceCountZero(relation));

    // Close storage files and unlink statistics
    RelationCloseSmgr(relation);
    pgstat_unlink_relation(relation);

    // Free basic relation data
    if (relation->rd_rel)
        pfree(relation->rd_rel);

    // Handle tuple descriptor with reference counting
    Assert(relation->rd_att->tdrefcount > 0);
    if (--relation->rd_att->tdrefcount == 0) {
        if (remember_tupdesc)
            RememberToFreeTupleDescAtEOX(relation->rd_att);
        else
            FreeTupleDesc(relation->rd_att);
    }

    // Free subsidiary data structures
    FreeTriggerDesc(relation->trigdesc);
    list_free_deep(relation->rd_fkeylist);
    list_free(relation->rd_indexlist);
    list_free(relation->rd_statlist);

    // Free attribute bitmaps
    bms_free(relation->rd_keyattr);
    bms_free(relation->rd_pkattr);
    bms_free(relation->rd_idattr);
    bms_free(relation->rd_hotblockingattr);
    bms_free(relation->rd_summarizedattr);

    // Free optional cached data
    if (relation->rd_pubdesc) pfree(relation->rd_pubdesc);
    if (relation->rd_options) pfree(relation->rd_options);
    if (relation->rd_indextuple) pfree(relation->rd_indextuple);
    if (relation->rd_amcache) pfree(relation->rd_amcache);
    if (relation->rd_fdwroutine) pfree(relation->rd_fdwroutine);

    // Delete memory contexts
    if (relation->rd_indexcxt) MemoryContextDelete(relation->rd_indexcxt);
    if (relation->rd_rulescxt) MemoryContextDelete(relation->rd_rulescxt);
    if (relation->rd_rsdesc) MemoryContextDelete(relation->rd_rsdesc->rscxt);
    if (relation->rd_partkeycxt) MemoryContextDelete(relation->rd_partkeycxt);
    if (relation->rd_pdcxt) MemoryContextDelete(relation->rd_pdcxt);
    if (relation->rd_pddcxt) MemoryContextDelete(relation->rd_pddcxt);
    if (relation->rd_partcheckcxt) MemoryContextDelete(relation->rd_partcheckcxt);

    // Finally free the relation structure itself
    pfree(relation);
}
```