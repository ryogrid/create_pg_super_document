# pg_rusage_show

## Location
[src/backend/utils/misc/pg_rusage.c:40-73](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/pg_rusage.c#L40-L73)

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

## Simplified Source

```c
// Simplified version of pg_rusage_show
const char *
pg_rusage_show(const PGRUsage *ru0)
{
    static char result[100];
    PGRUsage current_usage;

    // Get current resource usage snapshot
    pg_rusage_init(&current_usage);

    // Handle microsecond underflow by borrowing from seconds
    // For wall-clock time
    if (current_usage.tv.tv_usec < ru0->tv.tv_usec) {
        current_usage.tv.tv_sec--;
        current_usage.tv.tv_usec += 1000000;
    }

    // For system CPU time
    if (current_usage.ru.ru_stime.tv_usec < ru0->ru.ru_stime.tv_usec) {
        current_usage.ru.ru_stime.tv_sec--;
        current_usage.ru.ru_stime.tv_usec += 1000000;
    }

    // For user CPU time
    if (current_usage.ru.ru_utime.tv_usec < ru0->ru.ru_utime.tv_usec) {
        current_usage.ru.ru_utime.tv_sec--;
        current_usage.ru.ru_utime.tv_usec += 1000000;
    }

    // Format timing differences into readable string
    snprintf(result, sizeof(result),
             "CPU: user: %d.%02d s, system: %d.%02d s, elapsed: %d.%02d s",
             (int)(current_usage.ru.ru_utime.tv_sec - ru0->ru.ru_utime.tv_sec),
             (int)(current_usage.ru.ru_utime.tv_usec - ru0->ru.ru_utime.tv_usec) / 10000,
             (int)(current_usage.ru.ru_stime.tv_sec - ru0->ru.ru_stime.tv_sec),
             (int)(current_usage.ru.ru_stime.tv_usec - ru0->ru.ru_stime.tv_usec) / 10000,
             (int)(current_usage.tv.tv_sec - ru0->tv.tv_sec),
             (int)(current_usage.tv.tv_usec - ru0->tv.tv_usec) / 10000);

    return result;
}
```

Key simplifications made:
- Renamed `ru1` to `current_usage` for better readability
- Added descriptive comments for each major logic section
- Grouped similar microsecond underflow handling logic
- Removed internationalization macro `_()` for simplicity
- Maintained the exact same algorithmic logic and precision
- Preserved all essential timing calculations and borrowing arithmetic