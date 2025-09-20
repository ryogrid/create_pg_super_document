# PgStatShared_Relation

## Location
[src/include/utils/pgstat_internal.h:392-396](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/pgstat_internal.h#L392-L396)

## Overview
A shared memory structure that holds comprehensive table and index statistics for PostgreSQL relations, implementing the common header pattern for variable-amount statistics.

## Definition

```c
typedef struct PgStatShared_Relation
{
	PgStatShared_Common header;
	PgStat_StatTabEntry stats;
} PgStatShared_Relation;
```
## Detailed Description
PgStatShared_Relation is a shared memory structure that maintains detailed statistics for individual relations (tables and indexes) within PostgreSQL databases. This structure follows the established pattern for variable-amount statistics, beginning with a PgStatShared_Common header for validation and locking, followed by relation-specific statistical data.

The structure tracks comprehensive relation activity including scan operations, tuple-level modifications (INSERT, UPDATE, DELETE), buffer cache performance, maintenance operations (VACUUM, ANALYZE), and tuple lifecycle metrics. These statistics are fundamental for query planning, autovacuum decision-making, and performance monitoring at the table level.

This is a critical component of PostgreSQL's cost-based query optimizer, providing the statistical foundation for join order decisions, index selection, and other query planning optimizations.

## Parameters / Member Variables
- : PgStatShared_Common structure containing magic number validation and LWLock for protecting the statistics data during concurrent access
- : PgStat_StatTabEntry structure containing comprehensive relation statistics including scan counts, tuple operations, buffer metrics, maintenance history, and tuple lifecycle data

## Dependencies
- Functions called/Symbols referenced:
  - PgStatShared_Common
  - [PgStat_StatTabEntry](PgStat_StatTabEntry.md)
- Called from (representative examples):
  - pgstat_copy_relation_stats
  - pgstat_report_vacuum
  - pgstat_report_analyze
  - [pgstat_relation_flush_cb](../p/pgstat_relation_flush_cb.md)
  - SH_DECLARE (hash table declarations)

## Notes and Other Information
- Part of PostgreSQL's variable-amount statistics system, allowing multiple relation statistics to coexist in shared memory
- [Relation](../R/Relation.md) statistics include: scan activity (numscans, lastscan), tuple operations (returned, fetched, inserted, updated, deleted, hot_updated, newpage_updated), tuple state tracking (live_tuples, dead_tuples, mod_since_analyze, ins_since_vacuum), buffer performance (blocks_fetched, blocks_hit), and maintenance history (vacuum/analyze timestamps and counts)
- Statistics are used by the query planner to estimate query costs and choose optimal execution plans
- Autovacuum daemon relies on these statistics to determine when tables need maintenance
- Statistics are accessible through system views like pg_stat_user_tables, pg_stat_sys_tables
- Supports both user-initiated and automatic maintenance operation tracking
- Hot updates and newpage updates are tracked separately to understand update patterns and their impact on table bloat