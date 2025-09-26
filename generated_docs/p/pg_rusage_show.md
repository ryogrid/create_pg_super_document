# pg_rusage_show

## Location
src/backend/utils/misc/pg_rusage.c: 40 - 73

## Overview
Computes and formats the elapsed time and resource usage difference between a baseline snapshot and the current time into a human-readable string.

## Definition

```c
const char *
pg_rusage_show(const PGRUsage *ru0)
```
## Detailed Description
The  function calculates the difference between a previously captured resource usage snapshot (via ) and the current resource usage state. It computes elapsed wall-clock time, user CPU time, and system CPU time, then formats these measurements into a localized string suitable for performance reporting.

The function handles microsecond precision timing calculations, properly managing time borrowing when microseconds underflow during subtraction. It uses a static buffer for the result string, which means the returned pointer is valid until the next call to this function.

The output format shows CPU usage broken down into user time (time spent in user-mode code) and system time (time spent in kernel/system calls), along with total elapsed wall-clock time. This provides insight into whether operations are CPU-bound, I/O-bound, or waiting on external resources.

## Parameters / Member Variables
- : Pointer to a const  structure containing the baseline resource usage snapshot, typically captured earlier using . Contains:
  - : The baseline wall-clock time measurement
  - : The baseline process resource usage statistics including user and system CPU times

## Dependencies
- Functions called/Symbols referenced:
  -  - Called internally to get current resource usage snapshot
  -  - Structure type for resource usage snapshots
  -  - For formatting the result string
  -  - Internationalization macro for localizing the output string
- Called from (representative examples):
  -  - Reports vacuum operation performance
  -  - Reports WAL recovery timing
  -  - Reports index rebuild performance
  -  - Reports table analysis timing
  -  - Reports sort operation performance
  -  - Reports sorting phase timing
  -  - Reports merge operation timing
  -  - Reports tuple dumping performance

## Notes and Other Information
- Returns a pointer to a static buffer, making the function non-reentrant and not thread-safe
- The result string is formatted as: "CPU: user: X.XX s, system: Y.YY s, elapsed: Z.ZZ s"
- Times are displayed with centisecond precision (hundredths of a second)
- Properly handles microsecond arithmetic with borrowing for accurate time calculations
- The function is designed for performance monitoring and debugging rather than high-precision timing
- Commonly used in PostgreSQL's verbose logging and performance analysis features
- The static buffer approach reflects PostgreSQL's single-threaded backend design
- Used extensively throughout PostgreSQL for operation timing in maintenance commands and sort operations