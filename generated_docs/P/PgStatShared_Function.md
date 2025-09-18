# PgStatShared_Function

## Location
src/include/utils/pgstat_internal.h: 398 - 402

## Overview
A shared memory structure that holds function execution statistics for PostgreSQL user-defined functions, implementing the common header pattern for variable-amount statistics.

## Definition


## Detailed Description
PgStatShared_Function is a shared memory structure that maintains execution statistics for user-defined functions within PostgreSQL databases. This structure follows the established pattern for variable-amount statistics, beginning with a PgStatShared_Common header for validation and locking, followed by function-specific performance metrics.

The structure tracks essential function performance data including call frequency and execution timing information. These statistics are valuable for identifying performance bottlenecks in user-defined functions, understanding function usage patterns, and optimizing database application logic.

Function statistics are particularly important in applications that rely heavily on stored procedures, user-defined functions, or complex database logic, providing insights into which functions consume the most execution time and are called most frequently.

## Parameters / Member Variables
- : PgStatShared_Common structure containing magic number validation and LWLock for protecting the statistics data during concurrent access
- : PgStat_StatFuncEntry structure containing function execution metrics including call count and timing information (total and self execution time)

## Dependencies
- Functions called/Symbols referenced:
  - PgStatShared_Common
  - [PgStat_StatFuncEntry](PgStat_StatFuncEntry.md)
- Called from (representative examples):
  - [pgstat_function_flush_cb](../p/pgstat_function_flush_cb.md)
  - SH_DECLARE (hash table declarations)

## Notes and Other Information
- Part of PostgreSQL's variable-amount statistics system, allowing multiple function statistics to coexist in shared memory
- Function statistics include: numcalls (number of times the function has been called), total_time (total execution time including nested function calls, in microseconds), and self_time (execution time excluding nested function calls, in microseconds)
- Statistics are accessible through system views like pg_stat_user_functions
- The distinction between total_time and self_time helps identify whether performance issues are in the function itself or in functions it calls
- Function statistics are only collected for user-defined functions, not built-in PostgreSQL functions
- Timing measurements are in microseconds, providing fine-grained performance analysis
- These statistics support function-level performance monitoring and optimization in database applications