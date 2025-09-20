# WalRcvRunning

## Location
[src/backend/replication/walreceiverfuncs.c:75-125](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walreceiverfuncs.c#L75-L125)

## Overview
Determines whether the WAL receiver process is currently running or in the process of starting up, with timeout handling for startup failures.

## Definition

```c
bool
WalRcvRunning(void)
```
## Detailed Description
This function checks the current state of the WAL receiver by examining the shared walRcvState variable. It handles the special case where the WAL receiver is in WALRCV_STARTING state for too long, indicating a startup failure. In such cases, it automatically transitions the state to WALRCV_STOPPED and broadcasts a condition variable to notify waiting processes. The function provides a reliable way to determine if WAL receiver operations are active or available.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [WalRcvData](WalRcvData.md)
  - WalRcvState
  - pg_time_t
  - WALRCV_STARTING
  - WALRCV_STARTUP_TIMEOUT
  - WALRCV_STOPPED
  - ConditionVariableBroadcast
- Called from (representative examples):
  - [StartupRequestWalReceiverRestart](../S/StartupRequestWalReceiverRestart.md)
  - [ShutdownWalRcv](../S/ShutdownWalRcv.md)

## Notes and Other Information
- Located in src/backend/replication/walreceiverfuncs.c:75-125
- Uses spin locks to ensure atomic access to shared state variables
- Implements timeout logic with WALRCV_STARTUP_TIMEOUT to handle stuck startup processes
- Returns true if WAL receiver is in any state other than WALRCV_STOPPED
- Critical for coordinating WAL receiver lifecycle management across processes
- Automatically handles cleanup of failed startup attempts by transitioning to stopped state