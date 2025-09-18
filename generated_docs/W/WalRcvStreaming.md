# WalRcvStreaming

## Location
[src/backend/replication/walreceiverfuncs.c:126-177](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walreceiverfuncs.c#L126-L177)

## Overview
Determines whether the WAL receiver is actively streaming WAL data or in a state where streaming is expected (starting, restarting, or streaming).

## Definition


## Detailed Description
This function provides a more specific check than WalRcvRunning() by determining if the WAL receiver is actually engaged in streaming operations or is in a transitional state leading to streaming. It checks for WALRCV_STREAMING, WALRCV_STARTING, and WALRCV_RESTARTING states, making it useful for recovery logic that needs to know if WAL data is being or will be received. Like WalRcvRunning(), it includes timeout handling for startup failures to prevent indefinite waiting on failed WAL receiver processes.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [WalRcvData](WalRcvData.md)
  - WalRcvState
  - pg_time_t
  - WALRCV_STARTING
  - WALRCV_STARTUP_TIMEOUT
  - WALRCV_STOPPED
  - WALRCV_STREAMING
  - WALRCV_RESTARTING
  - ConditionVariableBroadcast
- Called from (representative examples):
  - [FinishWalRecovery](../F/FinishWalRecovery.md)
  - [WaitForWALToBecomeAvailable](WaitForWALToBecomeAvailable.md)

## Notes and Other Information
- Located in src/backend/replication/walreceiverfuncs.c:126-177
- Similar to WalRcvRunning() but with different state logic for streaming-specific operations
- Critical for WAL recovery logic that needs to coordinate with active streaming
- Returns true for streaming, starting, and restarting states
- Includes the same startup timeout logic as WalRcvRunning() to handle failed startups
- Used primarily during recovery operations to determine if WAL data is being received