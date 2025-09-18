# PgStat_StatTabEntry

## Location
src/include/pgstat.h: 399 - 429

## Overview
PgStat_StatTabEntry is a comprehensive structure that contains detailed statistics for PostgreSQL tables, tracking various metrics including scan operations, tuple operations, buffer usage, and maintenance activities like vacuum and analyze operations.

## Definition
```c
typedef struct PgStat_StatTabEntry
{
    PgStat_Counter numscans;
    TimestampTz lastscan;

    PgStat_Counter tuples_returned;
    PgStat_Counter tuples_fetched;

    PgStat_Counter tuples_inserted;
    PgStat_Counter tuples_updated;
    PgStat_Counter tuples_deleted;
    PgStat_Counter tuples_hot_updated;
    PgStat_Counter tuples_newpage_updated;

    PgStat_Counter live_tuples;
    PgStat_Counter dead_tuples;
    PgStat_Counter mod_since_analyze;
    PgStat_Counter ins_since_vacuum;

    PgStat_Counter blocks_fetched;
    PgStat_Counter blocks_hit;

    TimestampTz last_vacuum_time;        /* user initiated vacuum */
    PgStat_Counter vacuum_count;
    TimestampTz last_autovacuum_time;    /* autovacuum initiated */
    PgStat_Counter autovacuum_count;
    TimestampTz last_analyze_time;       /* user initiated */
    PgStat_Counter analyze_count;
    TimestampTz last_autoanalyze_time;   /* autovacuum initiated */
    PgStat_Counter autoanalyze_count;
} PgStat_StatTabEntry;
```

## Detailed Description
This structure serves as the central repository for table-level statistics in PostgreSQL. It provides comprehensive metrics that are essential for query optimization, autovacuum decisions, and performance monitoring. The statistics cover three main areas: access patterns (scans and tuple operations), data modification tracking (inserts, updates, deletes), and maintenance operations (vacuum and analyze). These statistics are actively used by the PostgreSQL autovacuum daemon to determine when tables need maintenance and by the query planner for cost estimation.

## Parameters / Member Variables
- `numscans`: Number of sequential scans initiated on this table
- `lastscan`: Timestamp of the last scan operation on this table
- `tuples_returned`: Number of tuples returned by scans of this table
- `tuples_fetched`: Number of tuples fetched by scans of this table
- `tuples_inserted`: Number of tuples inserted into this table
- `tuples_updated`: Number of tuples updated in this table
- `tuples_deleted`: Number of tuples deleted from this table
- `tuples_hot_updated`: Number of tuples updated using HOT (Heap-Only Tuples) optimization
- `tuples_newpage_updated`: Number of tuples updated to a new page
- `live_tuples`: Estimated number of live tuples in the table
- `dead_tuples`: Estimated number of dead tuples in the table
- `mod_since_analyze`: Number of modifications since the last ANALYZE operation
- `ins_since_vacuum`: Number of insertions since the last VACUUM operation
- `blocks_fetched`: Number of disk blocks fetched for this table
- `blocks_hit`: Number of buffer hits for this table (blocks found in cache)
- `last_vacuum_time`: Timestamp of the last user-initiated VACUUM operation
- `vacuum_count`: Number of user-initiated VACUUM operations performed
- `last_autovacuum_time`: Timestamp of the last autovacuum operation
- `autovacuum_count`: Number of autovacuum operations performed
- `last_analyze_time`: Timestamp of the last user-initiated ANALYZE operation
- `analyze_count`: Number of user-initiated ANALYZE operations performed
- `last_autoanalyze_time`: Timestamp of the last autoanalyze operation
- `autoanalyze_count`: Number of autoanalyze operations performed

## Dependencies
- Functions called/Symbols referenced:
  - PgStat_Counter (used for all counter fields)
  - TimestampTz (used for timestamp fields)
- Called from (representative examples):
  - do_autovacuum
  - pgstat_copy_relation_stats
  - pgstat_report_vacuum
  - pgstat_report_analyze
  - pgstat_fetch_stat_tabentry
  - PG_STAT_GET_RELENTRY_INT64
  - PG_STAT_GET_RELENTRY_TIMESTAMPTZ

## Notes and Other Information
- This structure is defined in src/include/pgstat.h at lines 399-429
- Essential for autovacuum decision-making and query optimization
- Statistics are exposed through various system views like pg_stat_user_tables
- The HOT update tracking helps monitor the effectiveness of the HOT optimization
- Buffer hit/fetch ratios provide insights into cache effectiveness for specific tables
- Maintenance timestamps help track the frequency and recency of table maintenance operations