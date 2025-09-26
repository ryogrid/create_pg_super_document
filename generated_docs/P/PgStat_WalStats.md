# PgStat_WalStats

## Location
[src/include/pgstat.h:431-442](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/pgstat.h#L431-L442)

## Overview
PgStat_WalStats is a structure that contains comprehensive statistics about PostgreSQL Write-Ahead Log (WAL) operations, tracking metrics such as record counts, buffer usage, write operations, and timing information.

## Definition
```c
typedef struct PgStat_WalStats
{
    PgStat_Counter wal_records;
    PgStat_Counter wal_fpi;
    uint64         wal_bytes;
    PgStat_Counter wal_buffers_full;
    PgStat_Counter wal_write;
    PgStat_Counter wal_sync;
    PgStat_Counter wal_write_time;
    PgStat_Counter wal_sync_time;
    TimestampTz stat_reset_timestamp;
} PgStat_WalStats;
```

## Detailed Description
This structure serves as the central repository for WAL-related statistics in PostgreSQL. The WAL is critical for database durability and recovery, and these statistics provide insights into WAL performance, throughput, and potential bottlenecks. The metrics include both operational counts (records, writes, syncs) and performance measurements (timing information), which are essential for monitoring database performance and diagnosing WAL-related issues. These statistics help database administrators understand WAL behavior patterns and optimize WAL configuration for better performance.

## Parameters / Member Variables
- `wal_records`: Number of WAL records generated
- `wal_fpi`: Number of WAL full page images (FPI) written
- `wal_bytes`: Total number of bytes written to WAL
- `wal_buffers_full`: Number of times WAL buffers became full
- `wal_write`: Number of WAL write operations performed
- `wal_sync`: Number of WAL sync operations performed
- `wal_write_time`: Total time spent writing WAL data (in microseconds)
- `wal_sync_time`: Total time spent syncing WAL data to disk (in microseconds)
- `stat_reset_timestamp`: Timestamp when these statistics were last reset

## Dependencies
- Functions called/Symbols referenced:
  - PgStat_Counter (used for most counter fields)
  - uint64 (used for wal_bytes)
  - TimestampTz (used for timestamp field)
- Called from (representative examples):
  - [pgstat_report_wal](../p/pgstat_report_wal.md)
  - PG_STAT_GET_WAL_COLS
  - [PgStatShared_Wal](PgStatShared_Wal.md)
  - [PgStat_Snapshot](PgStat_Snapshot.md)

## Notes and Other Information
- This structure is defined in src/include/pgstat.h at lines 431-442
- Essential for monitoring WAL performance and identifying potential bottlenecks
- The timing statistics (wal_write_time and wal_sync_time) are particularly important for identifying I/O performance issues
- Full page images (FPI) tracking helps understand WAL volume patterns after checkpoints
- Statistics are exposed through the pg_stat_wal system view
- wal_buffers_full counter indicates if WAL buffer size needs to be increased
- These metrics are crucial for WAL tuning and performance optimization