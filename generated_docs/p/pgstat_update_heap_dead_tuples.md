# pgstat_update_heap_dead_tuples

## Location
[src/backend/utils/activity/pgstat_relation.c:439-455](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_relation.c#L439-L455)

## Overview
Updates the count of dead tuples for a heap table in PostgreSQL statistics, typically used during tuple recovery operations like heap pruning.

## Definition
```c
void pgstat_update_heap_dead_tuples(Relation rel, int delta)
```

## Detailed Description
This function updates the dead tuple count for a relation in PostgreSQL's statistics system. Unlike other statistics functions that track transactional operations, this function operates on nontransactional state, meaning the changes are applied directly to the per-table counter rather than being buffered in transaction-level state.

The function decreases the `delta_dead_tuples` counter by the specified delta value, which represents the recovery of dead tuples (typically through operations like heap pruning where dead tuples are removed). The semantics are that a positive delta value represents tuples that were dead but are now recovered/removed, so the dead tuple count should decrease.

The changes made by this function are not subject to transaction rollback because they represent physical cleanup operations that have already occurred at the storage level.

## Parameters / Member Variables
- `rel`: A `Relation` pointer representing the heap table for which dead tuple statistics are being updated
- `delta`: An integer representing the number of dead tuples that have been recovered/removed (positive values decrease the dead tuple count)

## Dependencies
- Functions called/Symbols referenced:
  - `pgstat_should_count_relation` - Determines if statistics should be collected for this relation
  - `[PgStat_TableStatus](../P/PgStat_TableStatus.md)` - Structure type for maintaining table-level statistics
  - [PgStat_StatTabEntry](../P/PgStat_StatTabEntry.md) - Structure type referenced in the broader statistics context

- Called from (representative examples):
  - [heap_page_prune_opt](../h/heap_page_prune_opt.md) - During heap page pruning operations when dead tuples are removed

## Notes and Other Information
- This function operates on nontransactional state, unlike most other pgstat functions
- The delta value is subtracted from `delta_dead_tuples`, meaning positive delta values represent a reduction in dead tuple count
- Changes are applied directly to `pgstat_info->counts.delta_dead_tuples` rather than transaction-level counters
- This function is typically called during cleanup operations like heap pruning where physical tuple removal has occurred
- The dead tuple count is important for vacuum and autovacuum decision-making processes