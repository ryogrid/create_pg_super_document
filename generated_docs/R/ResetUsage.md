# ResetUsage

## Location
[src/backend/tcop/postgres.c:5080-5086](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L5080-L5086)

## Overview
This function resets the usage statistics baseline by capturing the current resource usage and wall-clock time for subsequent performance measurements.

## Definition
```c
void ResetUsage(void)
```

## Detailed Description
`ResetUsage` establishes a baseline for resource usage measurements by capturing the current process resource usage and wall-clock time into global variables (`Save_r` and `Save_t`). This function is typically called at the beginning of operations that need to be monitored for performance analysis. It works in conjunction with `ShowUsage` to provide before-and-after resource consumption measurements, enabling PostgreSQL to track CPU time, memory usage, and other system resources consumed during specific operations.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - getrusage (system call to get resource usage)
  - gettimeofday (system call to get current time)
  - RUSAGE_SELF (constant for current process resource usage)
  - Save_r (global variable to store resource usage baseline)
  - Save_t (global variable to store time baseline)

- Called from (representative examples):
  - btbuild (B-tree index building)
  - _bt_leafbuild (B-tree leaf page building)
  - _SPI_pquery (SPI query processing)
  - pg_parse_query (query parsing)
  - exec_simple_query (simple query execution)
  - PortalRun (portal execution)

## Notes and Other Information
- Part of PostgreSQL's performance monitoring infrastructure
- Must be called before the operation to be measured
- Stores baseline in global variables that are later used by `ShowUsage`
- Used extensively throughout the codebase for performance tracking
- Critical for query performance analysis and debugging
- The captured baseline includes CPU time, memory usage, and other system resource metrics