# pgstat_count_heap_delete

## Location
[src/backend/utils/activity/pgstat_relation.c:401-415](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_relation.c#L401-L415)

## Overview
Updates PostgreSQL statistics to record a tuple deletion operation for heap tables.

## Definition

```c
void
pgstat_count_heap_delete(Relation rel)
```
## Detailed Description
This function is responsible for tracking tuple deletion statistics in PostgreSQL's statistics collector system. When a tuple is deleted from a heap table, this function increments the deletion counter for the relation. The function first checks whether statistics should be collected for the given relation using , and if so, it ensures the transaction-level statistics structure is properly initialized and increments the  counter.

The statistics are maintained at the transaction level and are later aggregated into the global statistics when the transaction commits. This allows for proper rollback handling - if the transaction is aborted, the statistics changes are also discarded.

## Parameters / Member Variables
- `rel`: A  pointer representing the heap table from which a tuple was deleted
## Dependencies
- Functions called/Symbols referenced:
  -  - Determines if statistics should be collected for this relation
  -  - Structure type for maintaining table-level statistics
  -  - Ensures transaction-level statistics tracking is initialized

- Called from (representative examples):
  -  - Main heap tuple deletion function
  -  - When aborting speculative insertions

## Notes and Other Information
- This function is part of PostgreSQL's statistics collection framework
- Statistics are tracked at the transaction level to support proper rollback behavior
- Only relations that should have statistics collected (as determined by ) will have their deletion counts incremented
- The actual statistics are stored in trans->tuples_deleted

## Simplified Source

```c
void pgstat_count_heap_delete(Relation rel) {
    // Only count statistics for relations that should be tracked
    if (pgstat_should_count_relation(rel)) {
        PgStat_TableStatus *pgstat_info = rel->pgstat_info;

        // Ensure transaction-level statistics structure is initialized
        ensure_tabstat_xact_level(pgstat_info);

        // Increment the deletion counter
        pgstat_info->trans->tuples_deleted++;
    }
}
``` 