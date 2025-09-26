# pgstat_function_flush_cb

## Location
[src/backend/utils/activity/pgstat_function.c:193-222](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_function.c#L193-L222)

## Overview
Callback function that flushes pending function statistics from local backend storage to shared memory statistics, aggregating call counts and timing data.

## Definition
```c
bool pgstat_function_flush_cb(PgStat_EntryRef *entry_ref, bool nowait)
```

## Detailed Description
This function serves as a callback in PostgreSQL's statistics system to transfer accumulated function statistics from a backend's local pending storage to the shared memory statistics area. It aggregates call counts and converts timing information from internal time structures to microseconds before storing in shared statistics. The function uses proper locking to ensure thread-safe access to shared statistics data. If the `nowait` parameter is true and the lock cannot be immediately acquired, the function returns false without performing the flush operation, allowing for non-blocking behavior in time-sensitive contexts.

## Parameters / Member Variables
- `entry_ref`: Reference to the statistics entry containing both local pending data and shared statistics pointers
- `nowait`: Boolean flag indicating whether to attempt a non-blocking lock acquisition; if true and lock cannot be immediately acquired, the function returns false

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_lock_entry](pgstat_lock_entry.md)
  - [pgstat_unlock_entry](pgstat_unlock_entry.md)
  - INSTR_TIME_GET_MICROSEC
  - [PgStat_FunctionCounts](../P/PgStat_FunctionCounts.md) (type)
  - [PgStatShared_Function](../P/PgStatShared_Function.md) (type)
- Called from (representative examples):
  - SH_DECLARE (statistics hash table infrastructure in src/backend/utils/activity/pgstat.c:301)

## Notes and Other Information
- Part of PostgreSQL's statistics flushing infrastructure, registered as a callback for function statistics
- Converts timing data from instr_time format to microseconds for storage in shared memory
- Uses proper locking protocol to ensure atomic updates to shared statistics
- Returns boolean indicating success/failure, allowing callers to handle lock contention scenarios
- Located in src/backend/utils/activity/pgstat_function.c:193-222
- Critical component for moving function performance data from local accumulation to system-wide visibility