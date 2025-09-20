# PgStat_StatDBEntry

## Location
[src/include/pgstat.h:323-357](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/pgstat.h#L323-L357)

## Overview
PgStat_StatDBEntry is a comprehensive structure that tracks per-database statistics in PostgreSQL, including transaction counts, block access patterns, tuple operations, conflicts, session information, and various database-level performance metrics.

## Definition

```c
typedef struct PgStat_StatDBEntry
{
	PgStat_Counter xact_commit;
	PgStat_Counter xact_rollback;
	PgStat_Counter blocks_fetched;
	PgStat_Counter blocks_hit;
	PgStat_Counter tuples_returned;
	PgStat_Counter tuples_fetched;
	PgStat_Counter tuples_inserted;
	PgStat_Counter tuples_updated;
	PgStat_Counter tuples_deleted;
	TimestampTz last_autovac_time;
	PgStat_Counter conflict_tablespace;
	PgStat_Counter conflict_lock;
	PgStat_Counter conflict_snapshot;
	PgStat_Counter conflict_logicalslot;
	PgStat_Counter conflict_bufferpin;
	PgStat_Counter conflict_startup_deadlock;
	PgStat_Counter temp_files;
	PgStat_Counter temp_bytes;
	PgStat_Counter deadlocks;
	PgStat_Counter checksum_failures;
	TimestampTz last_checksum_failure;
	PgStat_Counter blk_read_time;	/* times in microseconds */
	PgStat_Counter blk_write_time;
	PgStat_Counter sessions;
	PgStat_Counter session_time;
	PgStat_Counter active_time;
	PgStat_Counter idle_in_transaction_time;
	PgStat_Counter sessions_abandoned;
	PgStat_Counter sessions_fatal;
	PgStat_Counter sessions_killed;

	TimestampTz stat_reset_timestamp;
} PgStat_StatDBEntry;
```
## Detailed Description
PgStat_StatDBEntry serves as the central repository for database-level statistics in PostgreSQL's statistics system. It provides comprehensive metrics covering transactional activity, buffer pool efficiency, tuple-level operations, conflict resolution, session management, and I/O performance. This structure is crucial for database monitoring, performance tuning, and understanding database workload characteristics. The statistics are maintained per database and are used by various system functions, monitoring tools, and the autovacuum system for decision-making.

## Parameters / Member Variables
- `xact_commit`: Number of transactions committed in this database
- `xact_rollback`: Number of transactions rolled back in this database
- `blocks_fetched`: Total number of disk blocks fetched for this database
- `blocks_hit`: Number of buffer hits (blocks found in shared buffer cache)
- `tuples_returned`: Number of tuples returned by queries in this database
- `tuples_fetched`: Number of tuples fetched by queries in this database
- `tuples_inserted`: Number of tuples inserted in this database
- `tuples_updated`: Number of tuples updated in this database
- `tuples_deleted`: Number of tuples deleted in this database
- `last_autovac_time`: Time of last autovacuum run on any table in this database
- `conflict_tablespace`: Number of queries canceled due to tablespace conflicts
- `conflict_lock`: Number of queries canceled due to lock conflicts
- `conflict_snapshot`: Number of queries canceled due to snapshot conflicts
- `conflict_logicalslot`: Number of queries canceled due to logical slot conflicts
- `conflict_bufferpin`: Number of queries canceled due to buffer pin conflicts
- `conflict_startup_deadlock`: Number of queries canceled due to startup deadlocks
- `temp_files`: Number of temporary files created by queries in this database
- `temp_bytes`: Total size of temporary files created (in bytes)
- `deadlocks`: Number of deadlocks detected in this database
- `checksum_failures`: Number of block checksum failures detected
- `last_checksum_failure`: Time of the last checksum failure
- `blk_read_time`: Time spent reading data file blocks (in microseconds)
- `blk_write_time`: Time spent writing data file blocks (in microseconds)
- `sessions`: Number of sessions connected to this database
- `session_time`: Total time spent in sessions (in milliseconds)
- `active_time`: Time spent executing queries (in milliseconds)
- `idle_in_transaction_time`: Time spent idle in transactions (in milliseconds)
- `sessions_abandoned`: Number of sessions terminated due to inactivity
- `sessions_fatal`: Number of sessions terminated due to fatal errors
- `sessions_killed`: Number of sessions terminated by administrator
- `stat_reset_timestamp`: Timestamp when statistics were last reset
## Dependencies
- Functions called/Symbols referenced:
  - PgStat_Counter (statistics counter type)
  - TimestampTz (timestamp data type)
- Called from (representative examples):
  - [pgstat_report_recovery_conflict](../p/pgstat_report_recovery_conflict.md) (conflict reporting)
  - [pgstat_report_deadlock](../p/pgstat_report_deadlock.md) (deadlock reporting)
  - [pgstat_report_tempfile](../p/pgstat_report_tempfile.md) (temporary file reporting)
  - [pgstat_report_connect](../p/pgstat_report_connect.md) (connection reporting)
  - [pgstat_report_disconnect](../p/pgstat_report_disconnect.md) (disconnection reporting)
  - [pgstat_fetch_stat_dbentry](../p/pgstat_fetch_stat_dbentry.md) (statistics retrieval)
  - [pgstat_update_dbstats](../p/pgstat_update_dbstats.md) (statistics update)
  - pg_stat_get_db_* functions (SQL interface functions)
  - autovacuum worker process (for database selection)

## Notes and Other Information
- This structure is fundamental to PostgreSQL's pg_stat_database system view
- Statistics are maintained in shared memory and periodically written to disk
- The conflict statistics are particularly important for standby servers where recovery conflicts can occur
- [Session](../S/Session.md) timing statistics help track database usage patterns and connection behavior
- Block timing statistics (blk_read_time, blk_write_time) require track_io_timing to be enabled
- Used extensively by monitoring tools and database administrators for performance analysis
- The autovacuum system uses these statistics to prioritize databases for maintenance operations