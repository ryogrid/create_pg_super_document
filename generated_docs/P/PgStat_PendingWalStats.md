# PgStat_PendingWalStats

## Location
[src/include/pgstat.h:450-457](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/pgstat.h#L450-L457)

## Overview
PgStat_PendingWalStats is a structure designed for efficiently accumulating WAL-related statistics before they are flushed to the main statistics system, using specialized timing types to avoid expensive type conversions during data collection.

## Definition
```c
/*
 * This struct stores wal-related durations as instr_time, which makes it
 * cheaper and easier to accumulate them, by not requiring type
 * conversions. During stats flush instr_time will be converted into
 * microseconds.
 */
typedef struct PgStat_PendingWalStats
{
    PgStat_Counter wal_buffers_full;
    PgStat_Counter wal_write;
    PgStat_Counter wal_sync;
    instr_time     wal_write_time;
    instr_time     wal_sync_time;
} PgStat_PendingWalStats;
```

## Detailed Description
This structure serves as an intermediate accumulation buffer for WAL statistics before they are flushed to the main PgStat_WalStats structure. The key design feature is the use of instr_time type for timing measurements instead of microsecond counters. This design choice significantly improves performance during statistics collection by avoiding expensive type conversions that would otherwise occur with each measurement. The timing values are converted to microseconds only during the statistics flush operation, which happens less frequently than the individual measurements, thus optimizing the critical path of WAL operations.

## Parameters / Member Variables
- `wal_buffers_full`: Pending count of times WAL buffers became full
- `wal_write`: Pending count of WAL write operations performed
- `wal_sync`: Pending count of WAL sync operations performed
- `wal_write_time`: Accumulated time spent writing WAL data (as instr_time for efficiency)
- `wal_sync_time`: Accumulated time spent syncing WAL data (as instr_time for efficiency)

## Dependencies
- Functions called/Symbols referenced:
  - PgStat_Counter (used for counter fields)
  - [instr_time](../i/instr_time.md) (used for timing fields to optimize accumulation)
- Called from (representative examples):
  - pgstat_count_buffer_hit

## Notes and Other Information
- This structure is defined in src/include/pgstat.h at lines 450-457
- Optimized for performance during statistics collection by using instr_time
- Acts as a staging area before statistics are flushed to PgStat_WalStats
- The instr_time fields are converted to microseconds during statistics flush operations
- This design reduces the overhead of statistics collection on WAL operations
- Essential for maintaining WAL performance while still collecting detailed timing statistics
- The structure contains a subset of WAL statistics that require timing measurements