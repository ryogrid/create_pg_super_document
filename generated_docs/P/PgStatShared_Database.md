# PgStatShared_Database

## Location
[src/include/utils/pgstat_internal.h:386-390](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/pgstat_internal.h#L386-L390)

## Overview
A shared memory structure that holds comprehensive database-level statistics for PostgreSQL, implementing the common header pattern for variable-amount statistics.

## Definition

```c
typedef struct PgStatShared_Database
{
	PgStatShared_Common header;
	PgStat_StatDBEntry stats;
} PgStatShared_Database;
```
## Detailed Description
PgStatShared_Database is a shared memory structure that maintains detailed statistics for individual databases within a PostgreSQL cluster. This structure follows the common pattern for variable-amount statistics, beginning with a PgStatShared_Common header that provides magic number validation and lock protection, followed by database-specific statistics.

The structure tracks comprehensive database activity including transaction statistics (commits/rollbacks), block-level I/O metrics, tuple operations (SELECT, INSERT, UPDATE, DELETE), conflict resolution, temporary file usage, session metrics, and timing information. These statistics are essential for database performance monitoring, capacity planning, and troubleshooting database-level issues.

This structure is part of PostgreSQL's extensible statistics framework that allows for different types of statistics objects to be managed uniformly while maintaining type-specific data.

## Parameters / Member Variables
- `header`: PgStatShared_Common structure containing magic number validation and LWLock for protecting the statistics data during concurrent access
- `stats`: PgStat_StatDBEntry structure containing comprehensive database statistics including transaction counts, block I/O metrics, tuple operation counters, conflict statistics, session data, and timing information
## Dependencies
- Functions called/Symbols referenced:
  - [PgStatShared_Common](PgStatShared_Common.md)
  - [PgStat_StatDBEntry](PgStat_StatDBEntry.md)
- Called from (representative examples):
  - [pgstat_report_autovac](../p/pgstat_report_autovac.md)
  - [pgstat_report_checksum_failures_in_db](../p/pgstat_report_checksum_failures_in_db.md)
  - [pgstat_reset_database_timestamp](../p/pgstat_reset_database_timestamp.md)
  - [pgstat_database_flush_cb](../p/pgstat_database_flush_cb.md)
  - [pgstat_database_reset_timestamp_cb](../p/pgstat_database_reset_timestamp_cb.md)
  - SH_DECLARE (hash table declarations)

## Notes and Other Information
- Part of PostgreSQL's variable-amount statistics system, allowing multiple database statistics to coexist in shared memory
- Database statistics include: transaction metrics (xact_commit, xact_rollback), buffer cache performance (blocks_fetched, blocks_hit), tuple activity (returned, fetched, inserted, updated, deleted), conflict types (tablespace, lock, snapshot, logical slot, buffer pin, startup deadlock), temporary file usage, deadlock counts, checksum failures, I/O timing, and session metrics
- The magic number in the header serves as a validity check to detect memory corruption
- Statistics are accessible through system views like pg_stat_database
- Supports database-specific operations like autovacuum reporting and checksum failure tracking
- [Session](../S/Session.md) statistics include connection counts, time spent in various states, and abnormal termination tracking