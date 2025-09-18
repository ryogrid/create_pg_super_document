# slotsync_worker_disconnect

## Location
src/backend/replication/logical/slotsync.c: 1177 - 1189

## Overview
slotsync_worker_disconnect is a cleanup function that handles disconnection of the WAL receiver connection when the slot synchronization worker exits.

## Definition
```c
static void slotsync_worker_disconnect(int code, Datum arg)
```

## Detailed Description
This function serves as a cleanup handler that is called when the slot synchronization worker process exits. It takes a Datum argument containing a pointer to a WalReceiverConn structure and properly disconnects the WAL receiver connection using walrcv_disconnect(). This ensures that network connections are properly closed and resources are cleaned up when the worker terminates, whether due to normal shutdown or error conditions.

The function follows PostgreSQL's exit callback pattern, where cleanup functions are registered to be called automatically during process termination.

## Parameters / Member Variables
- `code`: Exit code of the terminating process (not used in current implementation)
- `arg`: Datum containing a pointer to the WalReceiverConn structure that needs to be disconnected

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetPointer](../D/DatumGetPointer.md) (macro)
  - walrcv_disconnect
  - [WalReceiverConn](../W/WalReceiverConn.md) (type)
- Called from (representative examples):
  - [ReplSlotSyncWorkerMain](../R/ReplSlotSyncWorkerMain.md) (registered as exit callback at src/backend/replication/logical/slotsync.c:1475)

## Notes and Other Information
- This is a static function, meaning it's only visible within the slotsync.c compilation unit
- Follows PostgreSQL's exit callback convention with (int code, Datum arg) signature
- The code parameter is provided for consistency with the exit callback interface but is not currently used
- Essential for proper resource cleanup and connection management in the slot synchronization subsystem