# EnableStandbyMode

## Location
src/backend/access/transam/xlogrecovery.c: 478 - 511

## Overview
A wrapper function that enables PostgreSQL standby mode and performs necessary preparatory configurations to optimize standby server behavior.

## Definition
```c
static void EnableStandbyMode(void)
```

## Detailed Description
EnableStandbyMode is a static function that configures PostgreSQL to operate in standby mode. When called, it sets the global StandbyMode variable to true, indicating that the server is running as a standby replica. The function also disables startup progress timeout reporting to prevent unnecessary log bloat, since standby servers are continuously in recovery mode unless promoted to primary. This optimization reduces server log verbosity by avoiding redundant progress reports that would otherwise occur during normal standby operation.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - disable_startup_progress_timeout (disables startup progress reporting)
- Called from (representative examples):
  - InitWalRecovery (during WAL recovery initialization at lines 604 and 760)
  - ReadRecord (during record reading operations at line 3242)

## Notes and Other Information
- Static function, only accessible within xlogrecovery.c
- Part of PostgreSQL's replication and recovery system
- Sets the global StandbyMode variable to true
- Optimizes logging behavior for standby servers to reduce log volume
- Essential for proper standby server configuration during recovery initialization
- Located in src/backend/access/transam/xlogrecovery.c:478-511