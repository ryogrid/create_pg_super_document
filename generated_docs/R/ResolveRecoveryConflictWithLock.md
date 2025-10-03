# ResolveRecoveryConflictWithLock

## Location
[src/backend/storage/ipc/standby.c:622-791](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/standby.c#L622-L791)

## Overview
Resolves recovery conflicts with other backends holding relation locks by either canceling conflicting backends immediately or waiting with deadlock detection capabilities.

## Definition

```c
void
ResolveRecoveryConflictWithLock(LOCKTAG locktag, bool logging_conflict)
```
## Detailed Description
This function is called from ProcSleep() to resolve conflicts with other backends holding relation locks during hot standby recovery. It implements a sophisticated conflict resolution mechanism that either resolves conflicts immediately when the standby limit time has been exceeded, or sets up timeouts and waits for lock release while monitoring for deadlocks.

The function handles two main scenarios:
1. **Immediate resolution**: When the current time has already exceeded the standby limit time, it immediately cancels all backends holding conflicting locks
2. **Timed waiting**: When there's still time before the limit, it sets up two types of timeouts (standby lock timeout and deadlock timeout) and waits for the lock to be released

The function also implements deadlock detection by sending signals to conflicting backends after the deadlock timeout expires, requesting them to check for deadlocks. It carefully manages the logging_conflict parameter to ensure recovery conflicts are properly logged without duplicate entries.

## Parameters / Member Variables
- `locktag`: The lock tag identifying the specific lock that is causing the recovery conflict
- `logging_conflict`: Boolean flag indicating whether the recovery conflict has not been logged yet (true means logging is needed)
## Dependencies
- Functions called/Symbols referenced:
  - [GetStandbyLimitTime](../G/GetStandbyLimitTime.md)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)  
  - [GetLockConflicts](../G/GetLockConflicts.md)
  - [ResolveRecoveryConflictWithVirtualXIDs](ResolveRecoveryConflictWithVirtualXIDs.md)
  - [ProcWaitForSignal](../P/ProcWaitForSignal.md)
  - [SignalVirtualTransaction](../S/SignalVirtualTransaction.md)
  - [enable_timeouts](../e/enable_timeouts.md)
  - [disable_all_timeouts](../d/disable_all_timeouts.md)
  - [pg_atomic_read_u64](../p/pg_atomic_read_u64.md)
  - [pg_atomic_write_u64](../p/pg_atomic_write_u64.md)
- Called from (representative examples):
  - [ProcSleep](../P/ProcSleep.md) (src/backend/storage/lmgr/proc.c:1324)

## Notes and Other Information
- Only operates when InHotStandby is true
- Manages waitStart atomic variable to track when the process started waiting
- Uses two types of timeouts: STANDBY_LOCK_TIMEOUT and STANDBY_DEADLOCK_TIMEOUT
- Implements careful logic to avoid repeatedly sending deadlock check requests
- The logging_conflict parameter enables a two-phase approach where conflicts can be logged before retrying the wait
- Clears all timeouts on exit to avoid interference with other timeout mechanisms
- Updates pg_locks view information through waitStart management

## Simplified Source

```c
void
ResolveRecoveryConflictWithLock(LOCKTAG locktag, bool logging_conflict)
{
    TimestampTz ltime, now;

    Assert(InHotStandby);

    // Get standby timeout limit and current time
    ltime = GetStandbyLimitTime();
    now = GetCurrentTimestamp();

    // Update waitStart timestamp if this is the first time waiting
    if (pg_atomic_read_u64(&MyProc->waitStart) == 0)
        pg_atomic_write_u64(&MyProc->waitStart, now);

    if (now >= ltime && ltime != 0) {
        // We're past the limit - cancel conflicting backends immediately
        VirtualTransactionId *backends;

        backends = GetLockConflicts(&locktag, AccessExclusiveLock, NULL);
        ResolveRecoveryConflictWithVirtualXIDs(backends,
                                             PROCSIG_RECOVERY_CONFLICT_LOCK,
                                             PG_WAIT_LOCK | locktag.locktag_type,
                                             false);
    } else {
        // Wait with timeouts for standby limit and deadlock detection
        EnableTimeoutParams timeouts[2];
        int cnt = 0;

        // Set standby lock timeout if limit exists
        if (ltime != 0) {
            got_standby_lock_timeout = false;
            timeouts[cnt].id = STANDBY_LOCK_TIMEOUT;
            timeouts[cnt].type = TMPARAM_AT;
            timeouts[cnt].fin_time = ltime;
            cnt++;
        }

        // Set deadlock detection timeout
        got_standby_deadlock_timeout = false;
        timeouts[cnt].id = STANDBY_DEADLOCK_TIMEOUT;
        timeouts[cnt].type = TMPARAM_AFTER;
        timeouts[cnt].delay_ms = DeadlockTimeout;
        cnt++;

        enable_timeouts(timeouts, cnt);
    }

    // Wait for lock release signal
    ProcWaitForSignal(PG_WAIT_LOCK | locktag.locktag_type);

    // Handle timeout cases
    if (got_standby_lock_timeout)
        goto cleanup;

    if (got_standby_deadlock_timeout) {
        VirtualTransactionId *backends;

        backends = GetLockConflicts(&locktag, AccessExclusiveLock, NULL);

        if (VirtualTransactionIdIsValid(*backends)) {
            // Send deadlock check signals to conflicting backends
            while (VirtualTransactionIdIsValid(*backends)) {
                SignalVirtualTransaction(*backends,
                                       PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK,
                                       false);
                backends++;
            }

            // Exit if we need to log the conflict first
            if (logging_conflict)
                goto cleanup;

            // Wait again to avoid repeated deadlock check requests
            got_standby_deadlock_timeout = false;
            ProcWaitForSignal(PG_WAIT_LOCK | locktag.locktag_type);
        }
    }

cleanup:
    // Clean up all timeouts and flags
    disable_all_timeouts(false);
    got_standby_lock_timeout = false;
    got_standby_deadlock_timeout = false;
}
```