# begin_startup_progress_phase

## Location
[src/backend/postmaster/startup.c:343-358](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/startup.c#L343-L358)

## Overview
A wrapper function that resets and restarts the startup progress timeout mechanism, effectively beginning a new startup progress phase.

## Definition
```c
void begin_startup_progress_phase(void)
```

## Detailed Description
This function serves as a convenient wrapper that orchestrates the transition to a new startup progress phase. It performs a clean reset of the progress timeout mechanism by first disabling any existing timeout and then enabling a fresh timeout with a new start timestamp.

The function is designed to be called at the beginning of major startup operations or when transitioning between different phases of PostgreSQL recovery. By calling `disable_startup_progress_timeout()` followed immediately by `enable_startup_progress_timeout()`, it ensures that any previous timeout state is cleared and a new timing cycle begins from the current moment.

Like other startup progress functions, it includes a safety check that immediately returns if the startup progress feature is disabled (when `log_startup_progress_interval` is set to 0), avoiding unnecessary timeout management operations when progress reporting is not in use.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called:
  - [disable_startup_progress_timeout](../d/disable_startup_progress_timeout.md)() (disables current timeout)
  - [enable_startup_progress_timeout](../e/enable_startup_progress_timeout.md)() (enables new timeout with current timestamp)

- [Variables](../V/Variables.md) accessed:
  - `log_startup_progress_interval` (configuration variable for feature enable/disable check)

- Called from:
  - [PerformWalRecovery](../P/PerformWalRecovery.md) (src/backend/access/transam/xlogrecovery.c:1745)
  - SyncDataDirectory (src/backend/storage/file/fd.c:3582, 3608, 3622)
  - ResetUnloggedRelations (src/backend/storage/file/reinit.c:70)
  - ereport_startup_progress (referenced in src/include/postmaster/startup.h:37)

## Notes and Other Information
- Part of PostgreSQL's startup progress reporting system
- Acts as a thin wrapper combining disable and enable operations
- Includes feature check that returns early if `log_startup_progress_interval` is 0 (feature disabled)
- Used to mark the beginning of new startup phases during recovery operations
- Ensures clean state transition by disabling old timeout before enabling new one
- Called at various points during recovery: WAL recovery, data directory sync, and unlogged relation reset
- The comment in source describes it as "A thin wrapper to first disable and then enable the startup progress timeout"
- Located in startup.c:343-358