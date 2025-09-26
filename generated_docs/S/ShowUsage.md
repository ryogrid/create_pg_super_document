# ShowUsage

## Location
src/backend/tcop/postgres.c: 5087 - 5195

## Overview
This function displays detailed resource usage statistics by comparing current system resource consumption against a previously established baseline set by `ResetUsage`.

## Definition
```c
void ShowUsage(const char *title)
```

## Detailed Description
`ShowUsage` generates comprehensive performance reports by calculating the difference between current resource usage and the baseline established by `ResetUsage`. It captures CPU time (user and system), elapsed wall-clock time, memory usage, I/O statistics, page fault counts, context switches, and other system metrics. The function formats this information into a detailed log message that includes both incremental usage (since the baseline) and total accumulated usage. On Unix-like systems, it provides extensive resource metrics, while on Windows it shows a more limited subset due to platform differences.

## Parameters / Member Variables
- `title`: A descriptive string that identifies the operation being measured, used as the main message in the log output

## Dependencies
- Functions called/Symbols referenced:
  - getrusage (system call to get current resource usage)
  - gettimeofday (system call to get current time)
  - StringInfoData (string buffer structure)
  - initStringInfo (initialize string buffer)
  - appendStringInfo (format and append to string buffer)
  - appendStringInfoString (append string to buffer)
  - ereport (PostgreSQL logging function)
  - errmsg_internal (internal error message formatting)
  - errdetail_internal (internal error detail formatting)
  - pfree (PostgreSQL memory deallocation)
  - RUSAGE_SELF (constant for current process resource usage)
  - Save_r and Save_t (global baseline variables)

- Called from (representative examples):
  - btbuild (B-tree index building)
  - _bt_leafbuild (B-tree leaf page building)
  - _SPI_pquery (SPI query processing)
  - pg_parse_query (query parsing)
  - exec_simple_query (simple query execution)
  - PortalRun (portal execution)

## Notes and Other Information
- Requires a prior call to `ResetUsage` to establish the measurement baseline
- Handles microsecond arithmetic with proper overflow/underflow logic
- Platform-specific behavior: Unix systems show detailed metrics, Windows shows limited subset
- On macOS, converts memory usage from bytes to kilobytes for consistency
- Excludes some rusage fields (ixrss, idrss, isrss) that are not widely supported
- Output format includes both incremental and total usage statistics
- Critical for PostgreSQL performance analysis, debugging, and optimization
- Used extensively throughout the codebase for monitoring expensive operations
- Log level is LOG, making it visible in server logs when appropriate logging levels are set