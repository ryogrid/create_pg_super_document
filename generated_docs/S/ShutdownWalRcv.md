# ShutdownWalRcv

## Location
[src/backend/replication/walreceiverfuncs.c:178-244](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walreceiverfuncs.c#L178-L244)

## Overview
Gracefully stops the WAL receiver process and waits for it to completely shut down, handling various WAL receiver states appropriately.

## Definition

```c
void
ShutdownWalRcv(void)
```
## Detailed Description
This function implements a coordinated shutdown of the WAL receiver process. It uses a state machine approach to handle different WAL receiver states appropriately - immediately stopping processes in STARTING state, requesting shutdown for active processes (STREAMING, WAITING, RESTARTING), and waiting for processes already in STOPPING state. For active processes, it sends a SIGTERM signal and then waits for acknowledgment via condition variables. The function ensures complete cleanup by waiting until WalRcvRunning() returns false.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [WalRcvData](../W/WalRcvData.md)
  - pid_t
  - WALRCV_STOPPED
  - WALRCV_STARTING
  - WALRCV_STREAMING
  - WALRCV_WAITING
  - WALRCV_RESTARTING
  - WALRCV_STOPPING
  - ConditionVariableBroadcast
  - kill
  - [ConditionVariablePrepareToSleep](../C/ConditionVariablePrepareToSleep.md)
  - [WalRcvRunning](../W/WalRcvRunning.md)
  - [ConditionVariableSleep](../C/ConditionVariableSleep.md)
  - [ConditionVariableCancelSleep](../C/ConditionVariableCancelSleep.md)
- Called from (representative examples):
  - [XLogShutdownWalRcv](../X/XLogShutdownWalRcv.md)

## Notes and Other Information
- Located in src/backend/replication/walreceiverfuncs.c:178-244
- Executed by the Startup process during shutdown sequences
- Uses a state-based approach to handle different shutdown scenarios
- Sends SIGTERM to the WAL receiver process if it's actively running
- Implements proper synchronization using condition variables for clean shutdown
- Waits indefinitely until the WAL receiver acknowledges shutdown by setting state to STOPPED
- Critical for ensuring clean shutdown of streaming replication connections