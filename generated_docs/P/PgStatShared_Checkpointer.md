# PgStatShared_Checkpointer

## Location
[src/include/utils/pgstat_internal.h:342-349](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/pgstat_internal.h#L342-L349)

## Overview
PgStatShared_Checkpointer is a shared memory structure that maintains checkpointer process statistics using a changecount mechanism for high-performance single-writer updates while ensuring thread-safe access to checkpoint and restartpoint statistical data.

## Definition
```c
typedef struct PgStatShared_Checkpointer
{
    LWLock      lock;
    uint32      changecount;
    PgStat_CheckpointerStats stats;
    PgStat_CheckpointerStats reset_offset;
} PgStatShared_Checkpointer;
```

## Detailed Description
PgStatShared_Checkpointer implements the shared memory storage for PostgreSQL's checkpointer process statistics. Following the same pattern as other fixed-amount shared stats structures, it uses a sophisticated single-writer, multiple-reader design with a changecount mechanism to achieve high performance for stat updates. The structure maintains current statistics about checkpoints, restartpoints, write/sync times, and buffer operations, along with a reset offset that allows for statistical resets without directly modifying the shared stats data. This design is critical for monitoring database durability operations without impacting the performance of the checkpointer process.

## Parameters / Member Variables
- `lock`: LWLock that protects the reset_offset field and the stat_reset_timestamp within the stats structure
- `changecount`: Used in the changecount mechanism to detect concurrent updates and ensure consistent reads without blocking the checkpointer process
- `stats`: Current checkpointer statistics including counts of timed/requested checkpoints and restartpoints, write/sync times, buffers written, and reset timestamp
- `reset_offset`: Snapshot of statistics values at the time of the last reset, used to calculate net statistics since reset

## Dependencies
- Functions called/Symbols referenced:
  - [LWLock](../L/LWLock.md)
  - [PgStat_CheckpointerStats](PgStat_CheckpointerStats.md)
  - PgStat_Counter
  - TimestampTz
- Called from (representative examples):
  - [pgstat_report_checkpointer](../p/pgstat_report_checkpointer.md)
  - [pgstat_checkpointer_reset_all_cb](../p/pgstat_checkpointer_reset_all_cb.md)
  - [pgstat_checkpointer_snapshot_cb](../p/pgstat_checkpointer_snapshot_cb.md)

## Notes and Other Information
- Uses the same changecount mechanism as other fixed-amount stat structures for lock-free reads and high-performance writes
- The lock only protects reset_offset and stat_reset_timestamp, while regular stat updates use the changecount mechanism
- Essential for monitoring database durability and checkpoint performance tuning
- Tracks both regular checkpoints and restartpoints (used in standby servers during recovery)
- Provides timing information (write_time, sync_time) crucial for performance analysis and I/O subsystem tuning
- Reset operations don't directly modify the stats field but update the reset_offset, ensuring thread safety
- Readers must subtract reset_offset values from stats values to get effective statistics since the last reset
- Part of PostgreSQL's fixed-amount statistics system with exactly one instance in shared memory
- Critical for DBAs to monitor checkpoint frequency, duration, and efficiency