# BufferUsage

## Location
[src/include/executor/instrument.h:24-42](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/instrument.h#L24-L42)

## Overview
BufferUsage is a struct that tracks buffer I/O statistics and timing information for shared, local, and temporary buffers in PostgreSQL. It maintains counters that are never reset to allow calculation of incremental usage over arbitrary periods.

## Definition

```c
typedef struct BufferUsage
{
	int64		shared_blks_hit;	/* # of shared buffer hits */
	int64		shared_blks_read;	/* # of shared disk blocks read */
	int64		shared_blks_dirtied;	/* # of shared blocks dirtied */
	int64		shared_blks_written;	/* # of shared disk blocks written */
	int64		local_blks_hit; /* # of local buffer hits */
	int64		local_blks_read;	/* # of local disk blocks read */
	int64		local_blks_dirtied; /* # of local blocks dirtied */
	int64		local_blks_written; /* # of local disk blocks written */
	int64		temp_blks_read; /* # of temp blocks read */
	int64		temp_blks_written;	/* # of temp blocks written */
	instr_time	shared_blk_read_time;	/* time spent reading shared blocks */
	instr_time	shared_blk_write_time;	/* time spent writing shared blocks */
	instr_time	local_blk_read_time;	/* time spent reading local blocks */
	instr_time	local_blk_write_time;	/* time spent writing local blocks */
	instr_time	temp_blk_read_time; /* time spent reading temp blocks */
	instr_time	temp_blk_write_time;	/* time spent writing temp blocks */
} BufferUsage;
```
## Detailed Description
BufferUsage provides comprehensive tracking of buffer I/O operations and timing in PostgreSQL. The struct maintains both operation counts and timing information for three categories of buffers: shared buffers (part of the shared buffer pool), local buffers (used for temporary tables), and temporary buffers (used for sorting and other temporary operations).

The counters are designed to be monotonically increasing and are never reset to zero, allowing for accurate calculation of incremental usage during any time period by taking the difference between two measurements. This design supports performance monitoring, query profiling, and system analysis.

## Parameters / Member Variables
- : Count of buffer hits in the shared buffer pool (data found in memory)
- : Count of shared buffer blocks read from disk storage
- : Count of shared buffer blocks that were modified
- : Count of shared buffer blocks written to disk
- : Count of buffer hits in local buffers for temporary tables
- : Count of local buffer blocks read from disk
- : Count of local buffer blocks that were modified
- : Count of local buffer blocks written to disk
- : Count of temporary buffer blocks read during operations like sorting
- : Count of temporary buffer blocks written during operations
- : Time spent reading shared buffer blocks from disk
- : Time spent writing shared buffer blocks to disk
- : Time spent reading local buffer blocks from disk
- : Time spent writing local buffer blocks to disk
- : Time spent reading temporary buffer blocks
- : Time spent writing temporary buffer blocks

## Dependencies
- Functions called/Symbols referenced:
  - instr_time (timing infrastructure)
- Called from (representative examples):
  - BufferUsageAdd
  - BufferUsageAccumDiff
  - InstrEndParallelQuery
  - InstrAccumParallelQuery
  - show_buffer_usage (EXPLAIN output)
  - parallel vacuum operations
  - parallel index building

## Notes and Other Information
- The struct is defined in src/include/executor/instrument.h:24-42
- All counters must never be reset to zero to maintain the ability to calculate deltas
- Used extensively in parallel query execution and vacuum operations
- Critical for EXPLAIN ANALYZE output and performance monitoring
- The timing fields use instr_time for high-precision measurements
- Essential component of PostgreSQL's instrumentation and monitoring infrastructure