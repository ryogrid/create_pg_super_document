# has_startup_progress_timeout_expired

## Location
[src/backend/postmaster/startup.c:359-378](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/startup.c#L359-L378)

## Overview
Checks whether the startup progress timeout has expired and returns the elapsed time since the current progress phase began.

## Definition
```c
bool has_startup_progress_timeout_expired(long *secs, int *usecs)
```

## Detailed Description
This function is part of PostgreSQL's startup progress monitoring system. It checks if a startup progress timer has expired by examining the `startup_progress_timer_expired` flag. When the timer has expired, the function calculates the elapsed time since the start of the current startup progress phase and resets the timer flag.

The function operates on two static variables:
- `startup_progress_timer_expired`: A volatile sig_atomic_t flag set by the signal handler when the timer expires
- `startup_progress_phase_start_time`: A TimestampTz marking when the current progress phase began

This mechanism allows PostgreSQL to report progress during long-running startup operations (such as recovery) at regular intervals without constantly checking timestamps.

## Parameters / Member Variables
- `secs`: Output parameter that receives the elapsed seconds since the progress phase started
- `usecs`: Output parameter that receives the elapsed microseconds portion

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - [TimestampDifference](../T/TimestampDifference.md)
- Called from (representative examples):
  - ereport_startup_progress (macro in src/include/postmaster/startup.h)

## Notes and Other Information
- Returns `false` immediately if no timeout has occurred (startup_progress_timer_expired is false)
- Resets the `startup_progress_timer_expired` flag to false after processing an expired timeout
- The elapsed time calculation uses PostgreSQL's timestamp utilities for precise timing
- This function is typically used through the `ereport_startup_progress` macro, which automatically logs progress messages when timeouts expire
- The timeout interval is controlled by the `log_startup_progress_interval` configuration parameter
- The function is signal-safe as it only reads/writes sig_atomic_t variables and calls timestamp functions