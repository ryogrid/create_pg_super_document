# disable_startup_progress_timeout

## Location
[src/backend/postmaster/startup.c:309-322](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/startup.c#L309-L322)

## Overview
Disables the startup progress timeout mechanism, stopping progress report timer and clearing any expired timer flag.

## Definition
```c
void disable_startup_progress_timeout(void)
```

## Detailed Description
This function disables the startup progress timeout mechanism used during PostgreSQL recovery operations. It performs two main actions: first, it disables the actual timeout using the PostgreSQL timeout management system, and second, it resets the timer expiration flag to prevent any stale timeout signals from being processed.

The function includes a safety check that immediately returns if the startup progress feature is disabled (when `log_startup_progress_interval` is set to 0). This prevents unnecessary timeout management operations when the progress reporting feature is not in use.

When called, the function uses `disable_timeout()` with the `STARTUP_PROGRESS_TIMEOUT` identifier to stop the timer, and then resets the `startup_progress_timer_expired` flag to false. This ensures a clean state where no progress timeout is active and no stale timeout signals remain.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called:
  - `[disable_timeout](disable_timeout.md)` (timeout management function)

- Constants referenced:
  - `STARTUP_PROGRESS_TIMEOUT` (timeout identifier)

- [Variables](../V/Variables.md) accessed:
  - `log_startup_progress_interval` (configuration variable)
  - `startup_progress_timer_expired` (timer expiration flag)

- Called from:
  - [EnableStandbyMode](../E/EnableStandbyMode.md) (src/backend/access/transam/xlogrecovery.c:488)
  - [begin_startup_progress_phase](../b/begin_startup_progress_phase.md) (src/backend/postmaster/startup.c:349)
  - ereport_startup_progress (referenced in src/include/postmaster/startup.h:36)

## Notes and Other Information
- Part of PostgreSQL's startup progress reporting system
- Includes a feature check that returns early if `log_startup_progress_interval` is 0 (feature disabled)
- Uses PostgreSQL's timeout management system to properly disable timers
- Ensures clean state by resetting both the timeout and the expiration flag
- Typically called when transitioning between startup phases or when disabling progress reporting
- Located in startup.c:309-322