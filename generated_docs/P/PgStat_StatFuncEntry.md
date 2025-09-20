# PgStat_StatFuncEntry

## Location
[src/include/pgstat.h:359-365](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/pgstat.h#L359-L365)

## Overview
PgStat_StatFuncEntry is a structure that tracks execution statistics for individual functions in PostgreSQL, including call counts and timing information for performance monitoring and profiling.

## Definition

```c
typedef struct PgStat_StatFuncEntry
{
	PgStat_Counter numcalls;

	PgStat_Counter total_time;	/* times in microseconds */
	PgStat_Counter self_time;
} PgStat_StatFuncEntry;
```
## Detailed Description
PgStat_StatFuncEntry maintains execution statistics for individual user-defined functions in PostgreSQL's statistics system. This structure is essential for function performance monitoring and profiling, allowing database administrators and developers to identify performance bottlenecks in custom functions. The statistics differentiate between total execution time (including time spent in called functions) and self time (excluding time spent in called functions), providing detailed insight into function performance characteristics.

## Parameters / Member Variables
- : Number of times the function has been called since statistics were last reset
- : Total execution time for all calls to this function, including time spent in functions called by this function (measured in microseconds)
- : Total execution time for all calls to this function, excluding time spent in functions called by this function (measured in microseconds)

## Dependencies
- Functions called/Symbols referenced:
  - PgStat_Counter (statistics counter type)
- Called from (representative examples):
  - [find_funcstat_entry](../f/find_funcstat_entry.md) (function statistics lookup)
  - [pgstat_fetch_stat_funcentry](../p/pgstat_fetch_stat_funcentry.md) (statistics retrieval for specific function)
  - PG_STAT_GET_FUNCENTRY_FLOAT8_MS (SQL interface macro for timing statistics)
  - pgstat_count_conn_txn_idle_time (connection timing statistics)
  - [PgStatShared_Function](PgStatShared_Function.md) (shared memory statistics structure)

## Notes and Other Information
- This structure is the foundation for PostgreSQL's pg_stat_user_functions system view
- Function statistics collection must be enabled via track_functions configuration parameter
- The distinction between total_time and self_time is crucial for identifying whether performance issues are within the function itself or in functions it calls
- Statistics are maintained per function OID and are reset when statistics are manually reset or when the function is recreated
- Timing measurements require track_functions to be set to 'all' (not just 'pl' for procedural languages)
- Used by performance monitoring tools and profilers to identify slow-running functions
- The timing information helps developers optimize function performance and identify recursive or deeply nested function calls