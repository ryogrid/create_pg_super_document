# enable_startup_progress_timeout

## Location
[src/backend/postmaster/startup.c:323-342](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/startup.c#L323-L342)

## Overview
Sets the start timestamp of the current startup operation and enables the periodic timeout for logging progress reports during PostgreSQL recovery operations.

## Definition
```c
void enable_startup_progress_timeout(void)
```

## Detailed Description
This function initiates the startup progress timeout mechanism for PostgreSQL recovery operations. It performs several key operations: first, it records the current timestamp as the start time of the startup phase using `GetCurrentTimestamp()`. Then, it calculates the first timeout expiration time by adding the configured progress interval to the start time. Finally, it enables a recurring timeout using PostgreSQL's timeout management system.

The function includes a safety check that immediately returns if the startup progress feature is disabled (when `log_startup_progress_interval` is set to 0). When enabled, it sets up a recurring timeout that will trigger `startup_progress_timeout_handler()` at regular intervals defined by `log_startup_progress_interval`.

The timeout is configured as a repeating timeout using `enable_timeout_every()`, which means it will continue to fire periodically until explicitly disabled, allowing for regular progress updates during long-running recovery operations.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called:
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)() (timestamp utility function)
  - `TimestampTzPlusMilliseconds()` (timestamp arithmetic function)
  - `[enable_timeout_every](enable_timeout_every.md)()` (timeout management function)

- Constants referenced:
  - `STARTUP_PROGRESS_TIMEOUT` (timeout identifier)

- [Variables](../V/Variables.md) accessed:
  - `log_startup_progress_interval` (configuration variable for progress interval)
  - `startup_progress_phase_start_time` (static variable to store phase start time)

- Called from:
  - [begin_startup_progress_phase](../b/begin_startup_progress_phase.md) (src/backend/postmaster/startup.c:350)
  - ereport_startup_progress (referenced in src/include/postmaster/startup.h:35)

## Notes and Other Information
- Part of PostgreSQL's startup progress reporting system
- Includes a feature check that returns early if `log_startup_progress_interval` is 0 (feature disabled)
- Uses `enable_timeout_every()` to set up recurring timeouts rather than one-time timeouts
- Records the phase start time in `startup_progress_phase_start_time` for duration calculations
- The timeout will repeatedly trigger `startup_progress_timeout_handler()` at configured intervals
- Default progress interval is 10 seconds (10000ms) as defined by `log_startup_progress_interval`
- Located in startup.c:323-342 with explanatory comment about setting timestamp and enabling timeout