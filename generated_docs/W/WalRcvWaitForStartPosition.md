# WalRcvWaitForStartPosition

## Location
[src/backend/replication/walreceiver.c:665-744](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walreceiver.c#L665-L744)

## Overview
WalRcvWaitForStartPosition waits for the startup process to provide new streaming coordinates when WAL streaming needs to be restarted.

## Definition
static void WalRcvWaitForStartPosition(XLogRecPtr *startpoint, TimeLineID *startpointTLI)

## Detailed Description
This function implements a waiting mechanism for the WAL receiver process when it needs new instructions from the startup process about where to resume WAL streaming. It transitions the WAL receiver from STREAMING state to WAITING state and blocks until the startup process provides new coordinates.

The function operates through a state machine approach:
1. **State transition**: Changes from WALRCV_STREAMING to WALRCV_WAITING state
2. **Reset coordinates**: Clears current receive start position and timeline
3. **Notification**: Wakes up the startup process to indicate readiness for new instructions  
4. **Waiting loop**: Continuously checks for state changes using latch-based waiting
5. **Restart handling**: When WALRCV_RESTARTING state is detected, extracts new coordinates and transitions back to WALRCV_STREAMING

The function handles three possible state transitions during the wait:
- WALRCV_RESTARTING: Normal case where new streaming coordinates are provided
- WALRCV_STOPPING: Termination request, causing process exit
- Other states: Unexpected conditions that maintain the waiting loop

Process title is updated to reflect the current activity state for monitoring purposes.

## Parameters / Member Variables
- : Pointer to receive the new WAL starting position (output parameter)
- : Pointer to receive the new timeline ID for streaming (output parameter)

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire, SpinLockRelease
  - [proc_exit](../p/proc_exit.md), elog
  - [set_ps_display](../s/set_ps_display.md)
  - [WakeupRecovery](WakeupRecovery.md)
  - [ResetLatch](../R/ResetLatch.md), WaitLatch
  - [ProcessWalRcvInterrupts](../P/ProcessWalRcvInterrupts.md)
  - InvalidXLogRecPtr
  - WALRCV_STREAMING, WALRCV_WAITING, WALRCV_RESTARTING, WALRCV_STOPPING

- Called from (representative examples):
  - [WalReceiverMain](WalReceiverMain.md)
  - WalRcvWakeupReason

## Notes and Other Information
- Critical for coordinating WAL receiver restarts with the startup process
- Uses shared memory state machine for inter-process communication
- Implements proper latch-based waiting to avoid busy loops
- Handles graceful shutdown scenarios during waiting periods
- Part of PostgreSQL's streaming replication restart mechanism
- Process title updates help with monitoring and debugging replication issues