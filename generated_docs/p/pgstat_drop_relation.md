# pgstat_drop_relation

## Location
[src/backend/utils/activity/pgstat_relation.c:180-210](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_relation.c#L180-L210)

## Overview
Ensures statistics for a relation are dropped when the current transaction commits and resets transactional counters to zero.

## Definition
void pgstat_drop_relation(Relation rel)

## Detailed Description
This function performs cleanup operations for relation statistics when a relation is being dropped. It operates in two phases: first, it schedules the relation's statistics to be dropped when the current transaction commits using the transactional statistics system. Second, for relations that should be counted in statistics, it resets the transactional tuple counters (inserts, updates, deletes) to zero if they exist at the current transaction nesting level.

The function ensures that pg_stat_xact_all_tables views show zero counters for the relation within the same transaction, providing consistent statistics behavior during the drop operation.

## Parameters / Member Variables
- : The Relation structure representing the relation being dropped, containing relation metadata and statistics information

## Dependencies
- Functions called/Symbols referenced:
  - GetCurrentTransactionNestLevel
  - pgstat_drop_transactional
  - pgstat_should_count_relation
  - save_truncdrop_counters
  - PGSTAT_KIND_RELATION
  - PgStat_TableStatus
- Called from (representative examples):
  - heap_drop_with_catalog
  - index_drop
  - pgstat_count_conn_txn_idle_time

## Notes and Other Information
- The function handles both shared and non-shared relations by passing the appropriate database OID to pgstat_drop_transactional
- Only processes statistics for relations that should be counted (checked via pgstat_should_count_relation)
- Preserves truncate/drop counters by calling save_truncdrop_counters before resetting tuple counters
- Operates at the current transaction nesting level to ensure proper transactional behavior with savepoints