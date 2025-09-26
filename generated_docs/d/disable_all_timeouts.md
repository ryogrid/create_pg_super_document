# disable_all_timeouts

## Location
src/backend/utils/misc/timeout.c: 751 - 779

## Overview
Disables the timeout signal handler, removes all active timeouts from the system, and optionally resets timeout indicators.

## Definition


## Detailed Description
This function provides a comprehensive way to shut down the timeout system in PostgreSQL. It disables the alarm signal mechanism and clears all active timeouts from the system. The function operates in two phases: first it disables the alarm signal handler via , then it iterates through all timeout slots to mark them as inactive.

The function is designed with performance considerations in mind - it deliberately leaves the timer interrupt enabled in common usage patterns to avoid the overhead of repeatedly enabling/disabling it when timeouts are likely to be re-established shortly afterward.

## Parameters / Member Variables
- : Boolean flag that controls whether timeout indicator flags should be preserved (true) or reset to false (false)

## Dependencies
- Functions called/Symbols referenced:
  - disable_alarm (disables the SIGALRM signal handler)
  - MAX_TIMEOUTS (constant defining maximum number of timeout slots)
- Called from (representative examples):
  - pgarch_archiveXlog (archiver process cleanup)
  - ResolveRecoveryConflictWithLock (standby conflict resolution)
  - ResolveRecoveryConflictWithBufferPin (standby conflict resolution)
  - PostgresMain (main backend process cleanup)
  - DisableTimeoutParams (macro wrapper)

## Notes and Other Information
- The function sets  to 0 to indicate no timeouts are currently active
- Timer interrupt is intentionally left enabled for performance optimization
- All timeout slots in the  array are marked as inactive
- The  parameter allows callers to preserve timeout indicator states when needed for diagnostic or recovery purposes
- This function is typically called during process shutdown or error recovery scenarios