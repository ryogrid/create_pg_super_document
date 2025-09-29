# ResolveRecoveryConflictWithVirtualXIDs

## Location
[src/backend/storage/ipc/standby.c:359-466](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/standby.c#L359-L466)

## Overview
This function serves as the main executioner for query backends that conflict with recovery processing by waiting for virtual transaction IDs to complete and terminating them if necessary.

## Definition

```c
static void
ResolveRecoveryConflictWithVirtualXIDs(VirtualTransactionId *waitlist,
									   ProcSignalReason reason, uint32 wait_event_info,
									   bool report_waiting)
```
## Detailed Description
ResolveRecoveryConflictWithVirtualXIDs is a core function in PostgreSQL's standby server conflict resolution mechanism. It processes a list of virtual transaction IDs that are preventing recovery from proceeding, waiting for them to complete naturally or forcibly terminating them if the wait exceeds configured limits.

The function implements a progressive approach: it first attempts to wait for transactions to finish on their own, but if the maximum standby delay is exceeded, it cancels the conflicting virtual transactions using the specified signal reason. Throughout this process, it provides monitoring capabilities by updating the process display and logging recovery conflicts when appropriate.

The function includes sophisticated timing and reporting logic, tracking wait times and providing visibility into recovery conflicts through both process status display and detailed logging when conflicts persist beyond the deadlock timeout threshold.

## Parameters / Member Variables
- : Array of VirtualTransactionId structures representing the transactions that need to be resolved
- : ProcSignalReason indicating the type of signal to send when canceling transactions
- : Wait event information for monitoring and reporting purposes
- : Boolean flag controlling whether this function should report waiting status in PS display and logs

## Dependencies
- Functions called/Symbols referenced:
  - VirtualTransactionIdIsValid
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - [VirtualXactLock](../V/VirtualXactLock.md)
  - [WaitExceedsMaxStandbyDelay](../W/WaitExceedsMaxStandbyDelay.md)
  - [CancelVirtualTransaction](../C/CancelVirtualTransaction.md)
  - [pg_usleep](../p/pg_usleep.md)
  - [TimestampDifferenceExceeds](../T/TimestampDifferenceExceeds.md)
  - [set_ps_display_suffix](../s/set_ps_display_suffix.md)
  - [LogRecoveryConflict](../L/LogRecoveryConflict.md)
  - [set_ps_display_remove_suffix](../s/set_ps_display_remove_suffix.md)
- Called from (representative examples):
  - [ResolveRecoveryConflictWithSnapshot](ResolveRecoveryConflictWithSnapshot.md)
  - [ResolveRecoveryConflictWithTablespace](ResolveRecoveryConflictWithTablespace.md)
  - [ResolveRecoveryConflictWithLock](ResolveRecoveryConflictWithLock.md)

## Notes and Other Information
- This is a static function within the standby.c module, indicating it's an internal implementation detail
- The function implements exponential backoff through standbyWait_us for each transaction waited upon
- Process status is updated to show "waiting" after 500ms to provide user visibility
- Recovery conflicts are logged only when they exceed the deadlock_timeout threshold
- The function handles both successful resolution (transactions complete naturally) and forced resolution (transactions are cancelled)
- Fast exit optimization is implemented for empty waitlists to avoid unnecessary system calls

## Simplified Source

```c
static void
ResolveRecoveryConflictWithVirtualXIDs(VirtualTransactionId *waitlist,
                                       ProcSignalReason reason, uint32 wait_event_info,
                                       bool report_waiting)
{
    TimestampTz waitStart = 0;
    bool waiting = false;
    bool logged_recovery_conflict = false;

    // Quick exit if no transactions to wait for
    if (!VirtualTransactionIdIsValid(*waitlist))
        return;

    // Set up timing for reporting if needed
    if (report_waiting && (log_recovery_conflict_waits || update_process_title))
        waitStart = GetCurrentTimestamp();

    // Process each virtual transaction in the waitlist
    while (VirtualTransactionIdIsValid(*waitlist)) {
        standbyWait_us = STANDBY_INITIAL_WAIT_US;

        // Wait for the virtual transaction to complete
        while (!VirtualXactLock(*waitlist, false)) {
            // Check if maximum wait time exceeded - cancel if needed
            if (WaitExceedsMaxStandbyDelay(wait_event_info)) {
                pid_t pid = CancelVirtualTransaction(*waitlist, reason);

                // Brief pause to avoid flooding unresponsive backends
                if (pid != 0)
                    pg_usleep(5000L);
            }

            // Handle progress reporting
            if (waitStart != 0 && (!logged_recovery_conflict || !waiting)) {
                TimestampTz now = GetCurrentTimestamp();

                // Update process title after 500ms
                if (update_process_title && !waiting &&
                    TimestampDifferenceExceeds(waitStart, now, 500)) {
                    set_ps_display_suffix("waiting");
                    waiting = true;
                }

                // Log conflicts that exceed deadlock timeout
                if (log_recovery_conflict_waits && !logged_recovery_conflict &&
                    TimestampDifferenceExceeds(waitStart, now, DeadlockTimeout)) {
                    LogRecoveryConflict(reason, waitStart, now, waitlist, true);
                    logged_recovery_conflict = true;
                }
            }
        }

        // Move to next transaction
        waitlist++;
    }

    // Final logging and cleanup
    if (logged_recovery_conflict)
        LogRecoveryConflict(reason, waitStart, GetCurrentTimestamp(), NULL, false);

    if (waiting)
        set_ps_display_remove_suffix();
}
```