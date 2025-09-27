# WalSndWaitStopping

## Location
[src/backend/replication/walsender.c:3813-3850](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walsender.c#L3813-L3850)

## Overview
WalSndWaitStopping blocks until all WAL sender processes have either quit or reached the stopping state, ensuring safe timing for shutdown checkpoints.

## Definition
```c
void WalSndWaitStopping(void)
```

## Detailed Description
This function implements a polling loop that waits for all WAL senders to complete their stopping sequence. It is specifically used by the checkpointer process to determine when it's safe to perform the final shutdown checkpoint. The function performs the following operations:

1. **Continuous Monitoring**: Runs an infinite loop until all WAL senders are stopped
2. **Thread-safe State Checking**: Uses spin locks to safely examine each WAL sender's state
3. **State Validation**: Checks that each active WAL sender (pid != 0) has reached WALSNDSTATE_STOPPING
4. **Polling with Sleep**: If any WAL sender is not yet stopped, sleeps for 10ms before rechecking

The function ensures that the shutdown checkpoint can be safely performed without interfering with active replication processes.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire (acquires spin lock for thread-safe access)
  - SpinLockRelease (releases spin lock)
  - [pg_usleep](../p/pg_usleep.md) (sleeps for specified microseconds)
  - WALSNDSTATE_STOPPING (WAL sender stopping state constant)
- Called from (representative examples):
  - [ShutdownXLOG](../S/ShutdownXLOG.md) (during WAL system shutdown, after calling WalSndInitStopping)

## Notes and Other Information
- This function is the second phase of WAL sender shutdown (after WalSndInitStopping)
- Used by the checkpointer to coordinate shutdown checkpoint timing
- Implements a polling approach with 10ms intervals to avoid busy waiting
- Critical for ensuring data consistency during PostgreSQL shutdown
- Only considers WAL senders with non-zero PIDs (active processes)
- The WALSNDSTATE_STOPPING state indicates WAL senders have received the shutdown signal and are winding down
- Blocks indefinitely until all WAL senders reach the required state
- Essential for preventing race conditions between shutdown checkpoint and active replication
- Works with pg_usleep which is optimized for different platforms and remains responsive to signals

## Simplified Source

```c
// Simplified version of WalSndWaitStopping
void WalSndWaitStopping(void) {
    // Poll until all WAL senders reach stopping state
    for (;;) {
        bool all_stopped = true;

        // Check state of all WAL sender slots
        for (int i = 0; i < max_wal_senders; i++) {
            WalSnd *walsnd = &WalSndCtl->walsnds[i];

            // Thread-safe access to WAL sender state
            SpinLockAcquire(&walsnd->mutex);

            // Skip inactive WAL sender slots
            if (walsnd->pid == 0) {
                SpinLockRelease(&walsnd->mutex);
                continue;
            }

            // Check if this WAL sender has reached stopping state
            if (walsnd->state != WALSNDSTATE_STOPPING) {
                all_stopped = false;
                SpinLockRelease(&walsnd->mutex);
                break;
            }
            SpinLockRelease(&walsnd->mutex);
        }

        // Exit if all active WAL senders are in stopping state
        if (all_stopped)
            return;

        // Wait 10ms before checking again
        pg_usleep(10000L);
    }
}
```

Key simplifications made:
- Simplified loop structure and variable declarations
- Added clear comments for each major phase
- Maintained thread-safe state checking pattern
- Preserved the polling mechanism with appropriate sleep interval
- Focused on the core waiting and validation logic