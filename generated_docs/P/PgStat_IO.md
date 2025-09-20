# PgStat_IO

## Location
[src/include/pgstat.h:316-320](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/pgstat.h#L316-L320)

## Overview
PgStat_IO is a structure that maintains I/O statistics across different backend types in PostgreSQL, providing aggregated I/O operation counts and timing information for the statistics system.

## Definition

```c
typedef struct PgStat_IO
{
	TimestampTz stat_reset_timestamp;
	PgStat_BktypeIO stats[BACKEND_NUM_TYPES];
} PgStat_IO;
```
## Detailed Description
PgStat_IO serves as the top-level container for I/O statistics in PostgreSQL's statistics collection system. It maintains an array of I/O statistics structures (PgStat_BktypeIO) indexed by backend type, allowing the system to track I/O operations separately for different types of PostgreSQL backends (regular backends, background writer, checkpointer, etc.). The structure also tracks when the statistics were last reset, enabling proper interpretation of cumulative statistics.

## Parameters / Member Variables
- `stat_reset_timestamp`: Timestamp indicating when the I/O statistics were last reset, used for calculating statistics since last reset
- `stats[BACKEND_NUM_TYPES]`: Array of PgStat_BktypeIO structures containing detailed I/O statistics for each backend type, indexed by BackendType enumeration values
## Dependencies
- Functions called/Symbols referenced:
  - TimestampTz (timestamp data type)
  - PgStat_BktypeIO (backend-specific I/O statistics structure)
  - BACKEND_NUM_TYPES (macro defining the number of backend types)
- Called from (representative examples):
  - [pgstat_count_io_op_time](../p/pgstat_count_io_op_time.md) (I/O operation timing collection)
  - [pg_stat_get_io](../p/pg_stat_get_io.md) (statistics retrieval function)
  - PgStatShared_IO (shared memory statistics structure)
  - [PgStat_Snapshot](PgStat_Snapshot.md) (statistics snapshot structure)

## Notes and Other Information
- This structure is part of PostgreSQL's comprehensive statistics system for monitoring I/O performance
- The statistics are organized hierarchically: by backend type, then by I/O object type, context type, and operation type within each PgStat_BktypeIO
- The stat_reset_timestamp allows administrators and monitoring tools to understand the time period covered by the statistics
- Used both in shared memory (PgStatShared_IO) and in statistics snapshots for consistent I/O monitoring across the system