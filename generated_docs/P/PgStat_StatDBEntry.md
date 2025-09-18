# PgStat_StatDBEntry

## Location
src/include/pgstat.h: 323 - 357

## Overview
PgStat_StatDBEntry is a comprehensive structure that tracks per-database statistics in PostgreSQL, including transaction counts, block access patterns, tuple operations, conflicts, session information, and various database-level performance metrics.

## Definition


## Detailed Description
PgStat_StatDBEntry serves as the central repository for database-level statistics in PostgreSQL's statistics system. It provides comprehensive metrics covering transactional activity, buffer pool efficiency, tuple-level operations, conflict resolution, session management, and I/O performance. This structure is crucial for database monitoring, performance tuning, and understanding database workload characteristics. The statistics are maintained per database and are used by various system functions, monitoring tools, and the autovacuum system for decision-making.

## Parameters / Member Variables
- : Number of transactions committed in this database
- : Number of transactions rolled back in this database
- : Total number of disk blocks fetched for this database
- : Number of buffer hits (blocks found in shared buffer cache)
- : Number of tuples returned by queries in this database
- : Number of tuples fetched by queries in this database
- : Number of tuples inserted in this database
- : Number of tuples updated in this database
- : Number of tuples deleted in this database
- : Time of last autovacuum run on any table in this database
- : Number of queries canceled due to tablespace conflicts
- : Number of queries canceled due to lock conflicts
- : Number of queries canceled due to snapshot conflicts
- : Number of queries canceled due to logical slot conflicts
- : Number of queries canceled due to buffer pin conflicts
- : Number of queries canceled due to startup deadlocks
- : Number of temporary files created by queries in this database
- : Total size of temporary files created (in bytes)
- : Number of deadlocks detected in this database
- : Number of block checksum failures detected
- : Time of the last checksum failure
- : Time spent reading data file blocks (in microseconds)
- : Time spent writing data file blocks (in microseconds)
- : Number of sessions connected to this database
- : Total time spent in sessions (in milliseconds)
- : Time spent executing queries (in milliseconds)
- : Time spent idle in transactions (in milliseconds)
- : Number of sessions terminated due to inactivity
- : Number of sessions terminated due to fatal errors
- : Number of sessions terminated by administrator
- : Timestamp when statistics were last reset

## Dependencies
- Functions called/Symbols referenced:
  - PgStat_Counter (statistics counter type)
  - TimestampTz (timestamp data type)
- Called from (representative examples):
  - pgstat_report_recovery_conflict (conflict reporting)
  - pgstat_report_deadlock (deadlock reporting)
  - pgstat_report_tempfile (temporary file reporting)
  - pgstat_report_connect (connection reporting)
  - pgstat_report_disconnect (disconnection reporting)
  - pgstat_fetch_stat_dbentry (statistics retrieval)
  - pgstat_update_dbstats (statistics update)
  - pg_stat_get_db_* functions (SQL interface functions)
  - autovacuum worker process (for database selection)

## Notes and Other Information
- This structure is fundamental to PostgreSQL's pg_stat_database system view
- Statistics are maintained in shared memory and periodically written to disk
- The conflict statistics are particularly important for standby servers where recovery conflicts can occur
- Session timing statistics help track database usage patterns and connection behavior
- Block timing statistics (blk_read_time, blk_write_time) require track_io_timing to be enabled
- Used extensively by monitoring tools and database administrators for performance analysis
- The autovacuum system uses these statistics to prioritize databases for maintenance operations