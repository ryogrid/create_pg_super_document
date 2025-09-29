# pgstat_count_heap_insert

## Location
[src/backend/utils/activity/pgstat_relation.c:360-374](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_relation.c#L360-L374)

## Overview
Counts tuple insertions by incrementing the transaction-level tuple insertion counter for statistical tracking.

## Definition
void pgstat_count_heap_insert(Relation rel, PgStat_Counter n)

## Detailed Description
This function tracks tuple insertion operations for statistical purposes by incrementing the transactional tuple insertion counter. It ensures that the relation should be counted for statistics, then ensures the proper transaction-level statistics structure exists, and finally adds the specified count to the insertion counter. The statistics are maintained at the transaction level to support proper rollback behavior and accurate reporting in views like pg_stat_xact_all_tables.

## Parameters / Member Variables
- : The Relation structure for the table where tuples are being inserted
- : The number of tuples being inserted (allows batch counting)

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_should_count_relation
  - [ensure_tabstat_xact_level](../e/ensure_tabstat_xact_level.md)
  - PgStat_Counter
  - [PgStat_TableStatus](../P/PgStat_TableStatus.md)
- Called from (representative examples):
  - [heap_insert](../h/heap_insert.md)
  - [heap_multi_insert](../h/heap_multi_insert.md)
  - [RefreshMatViewByOid](../R/RefreshMatViewByOid.md)
  - pgstat_count_buffer_hit

## Notes and Other Information
- Only increments counters for relations that should be tracked according to pgstat_should_count_relation
- Uses transaction-level accounting to support proper rollback behavior if the transaction aborts
- Supports batch insertion counting by accepting a count parameter rather than assuming single tuple operations
- The counter is used for tracking insert activity for autovacuum scheduling and statistics reporting
- Statistics are later aggregated and reported through the PostgreSQL statistics system

## Simplified Source

```c
void
pgstat_count_heap_insert(Relation rel, PgStat_Counter n)
{
    // Only count statistics for relations that should be tracked
    if (pgstat_should_count_relation(rel))
    {
        PgStat_TableStatus *pgstat_info = rel->pgstat_info;

        // Ensure transaction-level statistics structure exists
        ensure_tabstat_xact_level(pgstat_info);

        // Increment the tuple insertion counter
        pgstat_info->trans->tuples_inserted += n;
    }
}
```