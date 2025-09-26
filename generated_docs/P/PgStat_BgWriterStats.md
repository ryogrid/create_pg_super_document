# PgStat_BgWriterStats

## Location
[src/include/pgstat.h:253-259](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/pgstat.h#L253-L259)

## Overview
PgStat_BgWriterStats tracks performance statistics for PostgreSQL's background writer process, which manages buffer pool maintenance and memory allocation counters.

## Definition

```c
typedef struct PgStat_BgWriterStats
{
	PgStat_Counter buf_written_clean;
	PgStat_Counter maxwritten_clean;
	PgStat_Counter buf_alloc;
	TimestampTz stat_reset_timestamp;
} PgStat_BgWriterStats;
```
## Detailed Description
PgStat_BgWriterStats maintains statistics for PostgreSQL's background writer process, which is responsible for writing dirty buffers from the shared buffer pool to disk proactively. The background writer helps reduce the I/O load during checkpoints and improves overall system performance by preventing backends from having to write dirty pages themselves. This structure tracks key metrics including the number of buffers written, instances where the writer was throttled due to excessive writes, and buffer allocation statistics that reflect overall memory management activity.

## Parameters / Member Variables
- : Counter tracking the number of buffers written by the background writer process
- : Counter tracking the number of times the background writer stopped a cleaning scan because it had written too many buffers (indicating I/O throttling)
- : Counter tracking the total number of buffers allocated from the buffer pool
- : Timestamp indicating when these background writer statistics were last reset to zero

## Dependencies
- Functions called/Symbols referenced:
  - PgStat_Counter
  - TimestampTz
- Called from (representative examples):
  - pgstat_report_bgwriter
  - pgstat_bgwriter_snapshot_cb
  - pgstat_count_buffer_hit
  - PgStatShared_BgWriter
  - PgStat_Snapshot

## Notes and Other Information
The background writer statistics are essential for tuning PostgreSQL's memory management and I/O performance. High values in maxwritten_clean may indicate that the background writer is being overly aggressive and should be throttled, while low values in buf_written_clean relative to checkpoint activity might suggest the background writer could be more active. The buf_alloc counter provides insight into overall buffer pool turnover and memory pressure. These statistics are accessible through PostgreSQL's statistics views and are crucial for database performance monitoring and tuning.