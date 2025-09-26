# ResolveRecoveryConflictWithLock

## Location
src/backend/storage/ipc/standby.c: 622 - 791

## Overview
Resolves recovery conflicts with other backends holding relation locks by either canceling conflicting backends immediately or waiting with deadlock detection capabilities.

## Definition


## Detailed Description
This function is called from ProcSleep() to resolve conflicts with other backends holding relation locks during hot standby recovery. It implements a sophisticated conflict resolution mechanism that either resolves conflicts immediately when the standby limit time has been exceeded, or sets up timeouts and waits for lock release while monitoring for deadlocks.

The function handles two main scenarios:
1. **Immediate resolution**: When the current time has already exceeded the standby limit time, it immediately cancels all backends holding conflicting locks
2. **Timed waiting**: When there's still time before the limit, it sets up two types of timeouts (standby lock timeout and deadlock timeout) and waits for the lock to be released

The function also implements deadlock detection by sending signals to conflicting backends after the deadlock timeout expires, requesting them to check for deadlocks. It carefully manages the logging_conflict parameter to ensure recovery conflicts are properly logged without duplicate entries.

## Parameters / Member Variables
- : The lock tag identifying the specific lock that is causing the recovery conflict
- : Boolean flag indicating whether the recovery conflict has not been logged yet (true means logging is needed)

## Dependencies
- Functions called/Symbols referenced:
  - GetStandbyLimitTime
  - GetCurrentTimestamp  
  - GetLockConflicts
  - ResolveRecoveryConflictWithVirtualXIDs
  - ProcWaitForSignal
  - SignalVirtualTransaction
  - enable_timeouts
  - disable_all_timeouts
  - pg_atomic_read_u64
  - pg_atomic_write_u64
- Called from (representative examples):
  - ProcSleep (src/backend/storage/lmgr/proc.c:1324)

## Notes and Other Information
- Only operates when InHotStandby is true
- Manages waitStart atomic variable to track when the process started waiting
- Uses two types of timeouts: STANDBY_LOCK_TIMEOUT and STANDBY_DEADLOCK_TIMEOUT
- Implements careful logic to avoid repeatedly sending deadlock check requests
- The logging_conflict parameter enables a two-phase approach where conflicts can be logged before retrying the wait
- Clears all timeouts on exit to avoid interference with other timeout mechanisms
- Updates pg_locks view information through waitStart management