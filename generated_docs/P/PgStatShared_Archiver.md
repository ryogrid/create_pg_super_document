# PgStatShared_Archiver

## Location
[src/include/utils/pgstat_internal.h:324-331](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/pgstat_internal.h#L324-L331)

## Overview
PgStatShared_Archiver is a shared memory structure that maintains WAL archiver statistics using a changecount mechanism for high-performance single-writer updates while ensuring thread-safe access to statistical data.

## Definition
```c
typedef struct PgStatShared_Archiver
{
    LWLock      lock;
    uint32      changecount;
    PgStat_ArchiverStats stats;
    PgStat_ArchiverStats reset_offset;
} PgStatShared_Archiver;
```

## Detailed Description
PgStatShared_Archiver implements the shared memory storage for PostgreSQL's WAL archiver statistics. It uses a sophisticated single-writer, multiple-reader design with a changecount mechanism to achieve high performance for stat updates. The structure maintains current statistics and a reset offset that allows for statistical resets without directly modifying the shared stats data. Instead of overwriting shared stats during resets, the system records current counter values in the reset_offset, and readers subtract this offset to get the effective statistics since the last reset.

## Parameters / Member Variables
- `lock`: LWLock that protects the reset_offset field and the stat_reset_timestamp within the stats structure
- `changecount`: Used in the changecount mechanism to detect concurrent updates and ensure consistent reads without blocking writers
- `stats`: Current archiver statistics including archived/failed counts, last archived/failed WAL files, timestamps, and reset timestamp
- `reset_offset`: Snapshot of statistics values at the time of the last reset, used to calculate net statistics since reset

## Dependencies
- Functions called/Symbols referenced:
  - LWLock
  - PgStat_ArchiverStats
  - TimestampTz
  - PgStat_Counter
- Called from (representative examples):
  - pgstat_report_archiver
  - pgstat_archiver_reset_all_cb
  - pgstat_archiver_snapshot_cb

## Notes and Other Information
- Uses the changecount mechanism from PgBackendStatus for lock-free reads and high-performance writes
- The lock only protects reset_offset and stat_reset_timestamp, while regular stat updates use the changecount mechanism
- This design allows the archiver process to update statistics frequently without being blocked by readers
- Reset operations don't directly modify the stats field but instead update the reset_offset, making resets safe in a concurrent environment
- Readers must subtract reset_offset values from stats values to get the effective statistics since the last reset
- Part of PostgreSQL's fixed-amount statistics system, meaning there's exactly one instance of this structure in shared memory