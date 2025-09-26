# PgStatShared_IO

## Location
[src/include/utils/pgstat_internal.h:352-360](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/pgstat_internal.h#L352-L360)

## Overview
PgStatShared_IO is a shared memory structure that maintains I/O statistics for different backend types, using per-backend-type locks to provide fine-grained concurrency control for I/O performance monitoring across the PostgreSQL system.

## Definition
```c
typedef struct PgStatShared_IO
{
    LWLock      locks[BACKEND_NUM_TYPES];
    PgStat_IO   stats;
} PgStatShared_IO;
```

## Detailed Description
PgStatShared_IO implements shared memory storage for PostgreSQL's comprehensive I/O statistics system. Unlike other fixed-amount stats structures that use a changecount mechanism, this structure uses an array of LWLocks to provide fine-grained concurrency control. Each lock protects I/O statistics for a specific backend type (such as regular backends, background writer, checkpointer, etc.), allowing concurrent updates from different types of processes without contention. The first lock (locks[0]) also protects the global stat_reset_timestamp. This design enables detailed monitoring of I/O operations across different contexts, objects, and operation types throughout the PostgreSQL system.

## Parameters / Member Variables
- `locks`: Array of LWLocks with BACKEND_NUM_TYPES entries, where locks[i] protects stats.stats[i] for backend type i, and locks[0] also protects stats.stat_reset_timestamp
- `stats`: PgStat_IO structure containing a reset timestamp and an array of I/O statistics for each backend type, with detailed counters and timing information for different I/O objects, contexts, and operations

## Dependencies
- Functions called/Symbols referenced:
  - [LWLock](../L/LWLock.md)
  - BACKEND_NUM_TYPES
  - [PgStat_IO](PgStat_IO.md)
  - [PgStat_BktypeIO](PgStat_BktypeIO.md)
  - PgStat_Counter
  - TimestampTz
- Called from (representative examples):
  - Various I/O statistics reporting functions
  - I/O statistics reset and snapshot functions

## Notes and Other Information
- Uses a different concurrency control mechanism compared to other shared stats structures - relies on explicit locking rather than changecount
- The fine-grained locking design allows different backend types to update their I/O statistics concurrently without interference
- Part of PostgreSQL's comprehensive I/O monitoring system that tracks operations across multiple dimensions (backend type, I/O object type, I/O context, I/O operation type)
- Essential for performance analysis and I/O subsystem monitoring, providing detailed breakdowns of read/write operations, timing information, and operation counts
- The multi-dimensional nature of the statistics (backend × object × context × operation) enables sophisticated performance analysis
- Unlike other stat structures, this doesn't use reset offsets but relies on direct protected updates to the shared statistics
- Critical for understanding I/O patterns and performance bottlenecks across different parts of the PostgreSQL system