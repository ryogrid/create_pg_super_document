# TimestampDifferenceExceeds

## Location
[src/backend/utils/adt/timestamp.c:1790-1810](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L1790-L1810)

## Overview
TimestampDifferenceExceeds is a utility function that checks whether the difference between two timestamps exceeds a specified threshold expressed in milliseconds.

## Definition

```c
bool
TimestampDifferenceExceeds(TimestampTz start_time,
						   TimestampTz stop_time,
						   int msec)
```
## Detailed Description
This function performs a simple comparison to determine if the time difference between two TimestampTz values is greater than or equal to a specified millisecond threshold. It calculates the difference by subtracting start_time from stop_time and compares the result against the threshold converted to microseconds (PostgreSQL's internal timestamp resolution). The function is designed to work with ordinary finite timestamps and is commonly used with results from GetCurrentTimestamp().

## Parameters / Member Variables
- `start_time`: The earlier timestamp (TimestampTz) used as the starting point for the difference calculation
- `stop_time`: The later timestamp (TimestampTz) used as the ending point for the difference calculation  
- `msec`: The threshold in milliseconds against which the time difference is compared

## Dependencies
- Functions called/Symbols referenced:
  - INT64CONST (macro for 64-bit integer constants)
- Called from (representative examples):
  - [heap_vacuum_rel](../h/heap_vacuum_rel.md) (vacuum operations)
  - [XLogBackgroundFlush](../X/XLogBackgroundFlush.md) (WAL flushing)
  - [WaitForWALToBecomeAvailable](../W/WaitForWALToBecomeAvailable.md) (WAL recovery)
  - [do_analyze_rel](../d/do_analyze_rel.md) (table analysis)
  - [WalSndWaitForWal](../W/WalSndWaitForWal.md) (WAL sender operations)
  - LockBufferForCleanup (buffer management)
  - ProcSleep (process sleeping/waiting)
  - [pgstat_report_stat](../p/pgstat_report_stat.md) (statistics reporting)

## Notes and Other Information
- The function assumes both input timestamps are ordinary finite values
- PostgreSQL stores timestamps with microsecond precision, so the millisecond threshold is converted to microseconds by multiplying by 1000
- Commonly used for timeout checks and performance monitoring throughout the PostgreSQL codebase
- The function performs a simple arithmetic comparison and is very efficient
- Returns true if the time difference meets or exceeds the threshold, false otherwise