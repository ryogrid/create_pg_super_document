# PgStatShared_Wal

## Location
[src/include/utils/pgstat_internal.h:369-374](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/pgstat_internal.h#L369-L374)

## Overview
A shared memory structure that holds WAL (Write-Ahead Logging) statistics for PostgreSQL, protected by a lightweight lock for concurrent access.

## Definition

```c
typedef struct PgStatShared_Wal
{
	/* lock protects ->stats */
	LWLock		lock;
	PgStat_WalStats stats;
} PgStatShared_Wal;
```
## Detailed Description
PgStatShared_Wal is a shared memory structure that maintains comprehensive statistics about PostgreSQL's Write-Ahead Logging (WAL) system. WAL is a critical component of PostgreSQL's ACID compliance and crash recovery mechanism, logging all changes before they are written to the main data files.

This structure provides centralized access to WAL performance metrics including record generation, buffer usage, I/O operations, and timing information. The statistics are essential for monitoring database write performance, diagnosing bottlenecks, and understanding WAL system behavior under various workloads.

The structure uses an LWLock to ensure thread-safe access in PostgreSQL's multi-process architecture, protecting the statistics from concurrent modifications during collection and reporting.

## Parameters / Member Variables
- : LWLock that protects concurrent access to the stats structure, ensuring data consistency during statistics updates and reads
- : PgStat_WalStats structure containing detailed WAL performance metrics including record counts, buffer statistics, I/O operations, and timing data

## Dependencies
- Functions called/Symbols referenced:
  - [LWLock](../L/LWLock.md)
  - [PgStat_WalStats](PgStat_WalStats.md)
- Called from (representative examples):
  - pgstat_flush_wal
  - [pgstat_wal_reset_all_cb](../p/pgstat_wal_reset_all_cb.md)
  - [pgstat_wal_snapshot_cb](../p/pgstat_wal_snapshot_cb.md)
  - [PgStat_ShmemControl](PgStat_ShmemControl.md) (as a member)

## Notes and Other Information
- WAL statistics tracked include: wal_records (number of WAL records generated), wal_fpi (full page images), wal_bytes (total bytes written), wal_buffers_full (buffer full events), wal_write/wal_sync (I/O operation counts), wal_write_time/wal_sync_time (timing metrics), and stat_reset_timestamp
- This structure is part of PostgreSQL's comprehensive statistics collection system and is embedded within PgStat_ShmemControl
- WAL statistics are crucial for performance monitoring, helping identify write-heavy workloads, I/O bottlenecks, and WAL configuration optimization opportunities
- The statistics can be reset and are typically accessed through system views like pg_stat_wal