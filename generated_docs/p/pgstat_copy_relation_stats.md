# pgstat_copy_relation_stats

## Location
[src/backend/utils/activity/pgstat_relation.c:58-91](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_relation.c#L58-L91)

## Overview
Copies statistics data from one relation to another, primarily used for operations like REINDEX CONCURRENTLY where a new relation needs to inherit the statistics of the original relation.

## Definition
```c
void pgstat_copy_relation_stats(Relation dst, Relation src)
```

## Detailed Description
This function transfers statistical information from a source relation to a destination relation. It is designed to support database operations that create new relation objects that should maintain the statistical history of their predecessors. The function fetches the statistics entry for the source relation and copies the entire statistics structure to the destination relation's shared statistics area.

The function first attempts to retrieve the statistics entry for the source relation using `pgstat_fetch_stat_tabentry_ext()`. If no statistics exist for the source relation, the function returns early without making any changes. Otherwise, it acquires a locked reference to the destination relation's statistics entry and performs a direct structure copy of the statistics data.

## Parameters / Member Variables
- `dst`: The destination Relation object that will receive the copied statistics
- `src`: The source Relation object whose statistics will be copied

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_fetch_stat_tabentry_ext](pgstat_fetch_stat_tabentry_ext.md)
  - [pgstat_get_entry_ref_locked](pgstat_get_entry_ref_locked.md)
  - [pgstat_unlock_entry](pgstat_unlock_entry.md)
  - [PgStat_StatTabEntry](../P/PgStat_StatTabEntry.md) (struct)
  - [PgStatShared_Relation](../P/PgStatShared_Relation.md) (struct)
  - [PgStat_EntryRef](../P/PgStat_EntryRef.md) (struct)
  - PGSTAT_KIND_RELATION (constant)
- Called from (representative examples):
  - [index_concurrently_swap](../i/index_concurrently_swap.md) (in REINDEX CONCURRENTLY operations)

## Notes and Other Information
- This function is specifically designed for REINDEX CONCURRENTLY operations where a new index replaces an existing one
- The function handles both shared and non-shared relations appropriately by checking the relisshared flag
- The statistics copy is performed as an atomic operation with proper locking to ensure consistency
- If the source relation has no statistics, the function gracefully exits without affecting the destination
- The function assumes both relations are valid and properly initialized

## Simplified Source

```c
void
pgstat_copy_relation_stats(Relation dst, Relation src)
{
    PgStat_StatTabEntry *srcstats;
    PgStatShared_Relation *dstshstats;
    PgStat_EntryRef *dst_ref;

    // Get source relation statistics
    srcstats = pgstat_fetch_stat_tabentry_ext(src->rd_rel->relisshared,
                                              RelationGetRelid(src));

    // If no source stats exist, nothing to copy
    if (!srcstats)
        return;

    // Get locked reference to destination relation stats
    dst_ref = pgstat_get_entry_ref_locked(PGSTAT_KIND_RELATION,
                                          dst->rd_rel->relisshared ? InvalidOid : MyDatabaseId,
                                          RelationGetRelid(dst),
                                          false);

    // Copy all statistics from source to destination
    dstshstats = (PgStatShared_Relation *) dst_ref->shared_stats;
    dstshstats->stats = *srcstats;

    // Release the lock
    pgstat_unlock_entry(dst_ref);
}
```