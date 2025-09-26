# PGRUsage

## Location
[src/include/utils/pg_rusage.h:22-26](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/pg_rusage.h#L22-L26)

## Overview
PGRUsage is a structure that captures resource usage snapshots for performance measurement and monitoring purposes in PostgreSQL. It serves as the state container for the pg_rusage_init/pg_rusage_show utility functions that provide CPU and elapsed time measurements.

## Definition

```c
typedef struct PGRUsage
{
	struct timeval tv;
	struct rusage ru;
} PGRUsage;
```
## Detailed Description
The PGRUsage structure is a composite data type that combines two POSIX system structures to provide comprehensive resource usage tracking capabilities. It encapsulates both wall-clock time information (via ) and detailed process resource usage statistics (via ). This structure is primarily used in conjunction with  and  functions to measure performance characteristics of various PostgreSQL operations, including vacuum operations, index rebuilds, table analysis, and WAL recovery processes.

The structure enables PostgreSQL to capture resource usage snapshots at specific points in time and later compute differences to determine how much CPU time (both user and system) and wall-clock time was consumed by particular operations. This information is valuable for performance analysis, debugging, and optimization purposes.

## Parameters / Member Variables
- : A  that captures wall-clock time information, containing seconds and microseconds since the Unix epoch. Used to measure elapsed real time between operations.
- : A  that contains detailed resource usage information including user CPU time, system CPU time, memory usage, and various I/O statistics. Provides comprehensive process resource consumption data.

## Dependencies
- Functions called/Symbols referenced:
  -  (system structure)
  -  (system structure)
- Called from (representative examples):
  -  (initializes a PGRUsage snapshot)
  -  (computes and formats usage differences)
  -  (vacuum operations measurement)
  -  (WAL recovery performance tracking)
  -  (index rebuild performance measurement)
  -  (table analysis performance tracking)
  -  (cluster operation performance measurement)
  -  (sorting operation performance tracking)

## Notes and Other Information
- The structure is defined in 
- Requires system includes:  and 
- Used extensively throughout PostgreSQL for performance measurement in operations such as vacuum, analyze, reindex, WAL recovery, and sorting
- The companion functions  and  provide a simple interface for capturing initial state and computing formatted performance reports
- Part of PostgreSQL's internal performance monitoring infrastructure, not exposed to end users directly
- The resource usage data captured depends on the underlying operating system's implementation of  and  system calls