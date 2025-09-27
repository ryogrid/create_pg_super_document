# WalSndInitStopping

## Location
[src/backend/replication/walsender.c:3787-3812](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walsender.c#L3787-L3812)

## Overview
WalSndInitStopping signals all active WAL sender processes to transition to a stopping state, preventing further WAL generation as part of PostgreSQL's graceful shutdown sequence.

## Definition
```c
void WalSndInitStopping(void)
```

## Detailed Description
This function is called during PostgreSQL shutdown to initiate a coordinated stopping of all WAL sender processes. It performs the following operations:

1. **Iterates through all WAL sender slots**: Loops through the entire WalSndCtl->walsnds array up to max_wal_senders
2. **Thread-safe PID retrieval**: Uses spin locks to safely read each WAL sender's process ID
3. **Signal active processes**: Sends PROCSIG_WALSND_INIT_STOPPING signal to each active WAL sender process

The function is part of PostgreSQL's graceful shutdown mechanism, ensuring that WAL senders can complete their current operations and transition to a state where they stop generating new WAL records.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire (acquires spin lock for thread-safe access)
  - SpinLockRelease (releases spin lock)
  - [SendProcSignal](../S/SendProcSignal.md) (sends inter-process signal)
  - PROCSIG_WALSND_INIT_STOPPING (signal type for stopping initialization)
  - INVALID_PROC_NUMBER (used when process number is not applicable)
- Called from (representative examples):
  - [ShutdownXLOG](../S/ShutdownXLOG.md) (during WAL system shutdown)

## Notes and Other Information
- This function is called during PostgreSQL server shutdown procedures
- Only sends signals to active WAL senders (those with pid != 0)
- Uses spin locks for thread-safe access to shared WAL sender state
- The PROCSIG_WALSND_INIT_STOPPING signal triggers WAL senders to enter a stopping state
- Part of a two-phase shutdown: this initiates stopping, then WalSndWaitStopping waits for completion
- Critical for ensuring clean shutdown of streaming replication connections
- The stopping state prevents new WAL generation while allowing current operations to complete
- Works in conjunction with the overall PostgreSQL shutdown sequence to ensure data consistency

## Simplified Source

```c
// Simplified version of WalSndInitStopping
void WalSndInitStopping(void) {
    // Signal all active WAL senders to enter stopping state
    for (int i = 0; i < max_wal_senders; i++) {
        WalSnd *walsnd = &WalSndCtl->walsnds[i];
        pid_t pid;

        // Thread-safe access to WAL sender PID
        SpinLockAcquire(&walsnd->mutex);
        pid = walsnd->pid;
        SpinLockRelease(&walsnd->mutex);

        // Skip inactive WAL sender slots
        if (pid == 0)
            continue;

        // Send stopping signal to active WAL sender
        SendProcSignal(pid, PROCSIG_WALSND_INIT_STOPPING, INVALID_PROC_NUMBER);
    }
}
```

Key simplifications made:
- Consolidated loop variable declaration
- Added clear comments explaining each phase of the operation
- Simplified the conditional logic flow
- Maintained thread-safe PID access pattern
- Preserved the core functionality of signaling all active WAL senders
- Focused on the essential shutdown coordination mechanism