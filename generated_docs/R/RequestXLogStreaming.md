# RequestXLogStreaming

## Location
src/backend/replication/walreceiverfuncs.c: 245 - 330

## Overview
Requests the postmaster to start or restart a WAL receiver process to stream transaction log data from a primary server at a specified position.

## Definition


## Detailed Description
This function initiates WAL (Write-Ahead Log) streaming by configuring and starting a WAL receiver process. It sets up the connection parameters, replication slot information, and starting position for streaming. The function ensures streaming always begins at segment boundaries to prevent broken segments. It handles both initial startup and restart scenarios, updating the global WalRcv shared memory structure and signaling the postmaster to launch the receiver process when needed.

## Parameters / Member Variables
- `tli`: Timeline ID indicating which timeline to stream from
- `recptr`: WAL position where streaming should begin (adjusted to segment boundary)
- `conninfo`: libpq connection string for connecting to the primary server
- `slotname`: Name of replication slot to acquire (optional, can be NULL)
- `create_temp_slot`: Whether to create a temporary replication slot if no slotname provided

## Dependencies
- Functions called/Symbols referenced:
  - XLogSegmentOffset (to align to segment boundaries)
  - SpinLockAcquire/SpinLockRelease (for mutex protection)
  - strlcpy (for string copying)
  - SendPostmasterSignal (to start walreceiver process)
  - SetLatch (to wake up existing receiver)
- Called from (representative examples):
  - WaitForWALToBecomeAvailable (during recovery)

## Notes and Other Information
- Always adjusts the starting position to the beginning of a WAL segment to prevent corruption
- Uses shared memory structure WalRcvData to communicate with the receiver process
- Handles both initial startup (WALRCV_STOPPED → WALRCV_STARTING) and restart scenarios (WALRCV_WAITING → WALRCV_RESTARTING)
- Thread-safe through spinlock protection of the walrcv mutex
- Supports both persistent and temporary replication slots
- Located in src/backend/replication/walreceiverfuncs.c:245-330