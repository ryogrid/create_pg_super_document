# IOOp

## Location
[src/include/pgstat.h:306-307](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/pgstat.h#L306-L307)

## Overview
IOOp is an enumeration that defines the different types of I/O operations that can be tracked and measured in PostgreSQL's statistics collection system.

## Definition

```c
typedef struct PgStat_BktypeIO
{
	PgStat_Counter counts[IOOBJECT_NUM_TYPES][IOCONTEXT_NUM_TYPES][IOOP_NUM_TYPES];
	PgStat_Counter times[IOOBJECT_NUM_TYPES][IOCONTEXT_NUM_TYPES][IOOP_NUM_TYPES];
} PgStat_BktypeIO;
```
## Detailed Description
IOOp categorizes the fundamental I/O operations that PostgreSQL performs during database operations. This enumeration is essential for detailed performance monitoring, allowing the database to track and analyze different types of I/O activities separately. Each operation type has distinct performance characteristics and resource implications, making this granular tracking valuable for identifying bottlenecks and optimizing database performance.

## Parameters / Member Variables
- : Operations where buffers are evicted from the buffer pool to make room for new data
- : Operations that extend database files by adding new pages or blocks
- : File system synchronization operations that ensure data is written to persistent storage
- : Operations where requested data is found in the buffer cache (cache hits)
- : Physical read operations that fetch data from storage devices into memory
- : Operations where existing buffers are reused without requiring new allocation
- : Physical write operations that store data from memory to storage devices
- : Background operations that write dirty buffers back to storage

## Dependencies
- Functions called/Symbols referenced:
  - Used with IOObject and IOContext for comprehensive I/O statistics
- Called from (representative examples):
  - [pgstat_count_io_op](../p/pgstat_count_io_op.md)
  - [pgstat_count_io_op_n](../p/pgstat_count_io_op_n.md)
  - [pgstat_count_io_op_time](../p/pgstat_count_io_op_time.md)
  - [pgstat_tracks_io_op](../p/pgstat_tracks_io_op.md)
  - [pgstat_get_io_op_index](../p/pgstat_get_io_op_index.md)
  - [pgstat_get_io_time_index](../p/pgstat_get_io_time_index.md)

## Notes and Other Information
- Used with the helper macro IOOP_NUM_TYPES to determine the total number of operation types
- Forms the finest level of granularity in PostgreSQL's I/O statistics framework
- Critical for performance analysis and capacity planning
- Different operation types have varying latency and throughput characteristics
- Cache hits (IOOP_HIT) vs physical reads (IOOP_READ) provide important metrics for buffer pool efficiency
- Writeback operations help distinguish between synchronous and asynchronous write patterns
- Used extensively in pg_stat_io system views for database monitoring and tuning