# pgstat_count_heap_update

## Location
[src/backend/utils/activity/pgstat_relation.c:375-400](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_relation.c#L375-L400)

## Overview
Counts tuple update operations by incrementing both transactional and non-transactional counters depending on the update type (regular, HOT, or newpage).

## Definition
void pgstat_count_heap_update(Relation rel, bool hot, bool newpage)

## Detailed Description
This function tracks tuple update operations for statistical purposes by maintaining separate counters for different types of updates. It increments the transactional tuples_updated counter for all updates, while also tracking specialized non-transactional counters for HOT (Heap-Only Tuple) updates and newpage updates. The function ensures mutual exclusivity between HOT and newpage updates through an assertion.

HOT updates are optimizations where the updated tuple fits on the same page without requiring index updates, while newpage updates involve moving the tuple to a different page. These specialized counters provide insight into update patterns that affect performance and maintenance operations.

## Parameters / Member Variables
- : The Relation structure for the table where the tuple is being updated
- : Boolean indicating whether this is a HOT (Heap-Only Tuple) update
- : Boolean indicating whether the update moved the tuple to a new page

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_should_count_relation
  - [ensure_tabstat_xact_level](../e/ensure_tabstat_xact_level.md)
  - [PgStat_TableStatus](../P/PgStat_TableStatus.md)
- Called from (representative examples):
  - [heap_update](../h/heap_update.md)
  - pgstat_count_buffer_hit

## Notes and Other Information
- Enforces mutual exclusivity between hot and newpage parameters through an assertion
- Uses transaction-level accounting for the main tuples_updated counter to support rollback behavior
- HOT and newpage counters are non-transactional and are incremented immediately
- Only processes statistics for relations that should be counted according to pgstat_should_count_relation
- The different update type counters help analyze update performance characteristics and guide optimization decisions
- These statistics are used by the autovacuum system and query planners to make informed decisions about maintenance and query execution

## Simplified Source

```c
void
pgstat_count_heap_update(Relation rel, bool hot, bool newpage)
{
    // Ensure hot and newpage are mutually exclusive
    Assert(!(hot && newpage));

    // Only count statistics for relations that should be tracked
    if (pgstat_should_count_relation(rel)) {
        PgStat_TableStatus *pgstat_info = rel->pgstat_info;

        // Ensure transaction-level statistics structure exists
        ensure_tabstat_xact_level(pgstat_info);

        // Increment transactional update counter (can be rolled back)
        pgstat_info->trans->tuples_updated++;

        // Increment non-transactional counters for specific update types
        if (hot)
            pgstat_info->counts.tuples_hot_updated++;
        else if (newpage)
            pgstat_info->counts.tuples_newpage_updated++;
    }
}
```