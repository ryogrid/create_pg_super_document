# PgStatShared_BgWriter

## Location
[src/include/utils/pgstat_internal.h:333-340](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/pgstat_internal.h#L333-L340)

## Overview
PgStatShared_BgWriter is a shared memory structure that maintains background writer statistics using a changecount mechanism for high-performance single-writer updates while ensuring thread-safe access to buffer management statistical data.

## Definition
```c
typedef struct PgStatShared_BgWriter
{
    LWLock      lock;
    uint32      changecount;
    PgStat_BgWriterStats stats;
    PgStat_BgWriterStats reset_offset;
} PgStatShared_BgWriter;
```

## Detailed Description
PgStatShared_BgWriter implements the shared memory storage for PostgreSQL's background writer process statistics. Like other fixed-amount shared stats structures, it uses a sophisticated single-writer, multiple-reader design with a changecount mechanism to achieve high performance for stat updates. The structure maintains current statistics about buffer cleaning operations and a reset offset that allows for statistical resets without directly modifying the shared stats data. This design ensures that the background writer can update statistics frequently without being blocked by readers, which is crucial for performance monitoring.

## Parameters / Member Variables
- `lock`: LWLock that protects the reset_offset field and the stat_reset_timestamp within the stats structure
- `changecount`: Used in the changecount mechanism to detect concurrent updates and ensure consistent reads without blocking the background writer process
- `stats`: Current background writer statistics including buffers written during cleaning, maxwritten_clean events, buffer allocations, and reset timestamp
- `reset_offset`: Snapshot of statistics values at the time of the last reset, used to calculate net statistics since reset

## Dependencies
- Functions called/Symbols referenced:
  - LWLock
  - PgStat_BgWriterStats
  - PgStat_Counter
  - TimestampTz
- Called from (representative examples):
  - pgstat_report_bgwriter
  - pgstat_bgwriter_reset_all_cb
  - pgstat_bgwriter_snapshot_cb

## Notes and Other Information
- Uses the same changecount mechanism as PgStatShared_Archiver for lock-free reads and high-performance writes
- The lock only protects reset_offset and stat_reset_timestamp, while regular stat updates use the changecount mechanism
- Critical for monitoring buffer pool efficiency and background writer performance
- Reset operations don't directly modify the stats field but instead update the reset_offset, making resets safe in concurrent environments
- Readers must subtract reset_offset values from stats values to get effective statistics since the last reset
- Part of PostgreSQL's fixed-amount statistics system with exactly one instance in shared memory
- Essential for database administrators to monitor buffer management efficiency and tune background writer settings