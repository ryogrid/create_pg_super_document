# WalUsage

## Location
[src/include/executor/instrument.h:51-56](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/instrument.h#L51-L56)

## Overview
WalUsage is a struct that tracks Write-Ahead Log (WAL) activity statistics per query, specifically focusing on WAL records generation that can be measured and displayed by EXPLAIN, pg_stat_statements, and similar tools.

## Definition
```c
typedef struct WalUsage
{
    int64       wal_records;    /* # of WAL records produced */
    int64       wal_fpi;        /* # of WAL full page images produced */
    uint64      wal_bytes;      /* size of WAL records produced */
} WalUsage;
```

## Detailed Description
WalUsage provides tracking of WAL activity that can be meaningfully measured and attributed to individual queries or operations. It focuses on WAL records generation rather than system-wide WAL operations like writes, which are tracked separately by WAL global statistics counters in WalStats.

The struct is designed to measure WAL activity that is directly attributable to query execution, making it valuable for query performance analysis and understanding the WAL impact of specific operations. Like BufferUsage, these counters are never reset to zero, allowing for calculation of incremental WAL usage over arbitrary periods.

## Parameters / Member Variables
- `wal_records`: Count of WAL records produced during the operation
- `wal_fpi`: Count of WAL full page images (FPI) produced, which occur when a page is modified for the first time after a checkpoint
- `wal_bytes`: Total size in bytes of WAL records produced during the operation

## Dependencies
- Functions called/Symbols referenced:
  - None (simple data structure)
- Called from (representative examples):
  - [WalUsageAdd](WalUsageAdd.md)
  - [WalUsageAccumDiff](WalUsageAccumDiff.md)
  - [InstrEndParallelQuery](../I/InstrEndParallelQuery.md)
  - [InstrAccumParallelQuery](../I/InstrAccumParallelQuery.md)
  - [show_wal_usage](../s/show_wal_usage.md) (EXPLAIN output)
  - [pgstat_flush_wal](../p/pgstat_flush_wal.md)
  - parallel vacuum operations
  - parallel index building

## Notes and Other Information
- The struct is defined in src/include/executor/instrument.h:51-56
- Tracks only query-attributable WAL activity, not system-wide WAL operations
- Used extensively in EXPLAIN ANALYZE output to show WAL impact of queries
- Essential for understanding the write amplification effects of different operations
- Full page images (FPI) are a significant component of WAL overhead and are tracked separately
- Complements BufferUsage to provide complete I/O and WAL impact measurement
- Part of PostgreSQL's comprehensive instrumentation framework for performance analysis