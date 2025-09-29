# RequestXLogStreaming

## Location
[src/backend/replication/walreceiverfuncs.c:245-330](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walreceiverfuncs.c#L245-L330)

## Overview
Requests the postmaster to start or restart a WAL receiver process to stream transaction log data from a primary server at a specified position.

## Definition

```c
void
RequestXLogStreaming(TimeLineID tli, XLogRecPtr recptr, const char *conninfo,
					 const char *slotname, bool create_temp_slot)
```
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
  - [strlcpy](../s/strlcpy.md) (for string copying)
  - [SendPostmasterSignal](../S/SendPostmasterSignal.md) (to start walreceiver process)
  - [SetLatch](../S/SetLatch.md) (to wake up existing receiver)
- Called from (representative examples):
  - [WaitForWALToBecomeAvailable](../W/WaitForWALToBecomeAvailable.md) (during recovery)

## Notes and Other Information
- Always adjusts the starting position to the beginning of a WAL segment to prevent corruption
- Uses shared memory structure WalRcvData to communicate with the receiver process
- Handles both initial startup (WALRCV_STOPPED → WALRCV_STARTING) and restart scenarios (WALRCV_WAITING → WALRCV_RESTARTING)
- Thread-safe through spinlock protection of the walrcv mutex
- Supports both persistent and temporary replication slots
- Located in src/backend/replication/walreceiverfuncs.c:245-330

## Simplified Source

```c
void
RequestXLogStreaming(TimeLineID tli, XLogRecPtr recptr, const char *conninfo,
                     const char *slotname, bool create_temp_slot)
{
    WalRcvData *walrcv = WalRcv;
    bool launch = false;
    pg_time_t now = time(NULL);

    // Always start at segment boundary to prevent broken segments
    if (XLogSegmentOffset(recptr, wal_segment_size) != 0) {
        recptr -= XLogSegmentOffset(recptr, wal_segment_size);
    }

    SpinLockAcquire(&walrcv->mutex);

    // Set connection info
    if (conninfo != NULL) {
        strlcpy((char *) walrcv->conninfo, conninfo, MAXCONNINFO);
    } else {
        walrcv->conninfo[0] = '\0';
    }

    // Set replication slot configuration
    if (slotname != NULL && slotname[0] != '\0') {
        strlcpy((char *) walrcv->slotname, slotname, NAMEDATALEN);
        walrcv->is_temp_slot = false;
    } else {
        walrcv->slotname[0] = '\0';
        walrcv->is_temp_slot = create_temp_slot;
    }

    // Update state based on current status
    if (walrcv->walRcvState == WALRCV_STOPPED) {
        launch = true;
        walrcv->walRcvState = WALRCV_STARTING;
    } else {
        walrcv->walRcvState = WALRCV_RESTARTING;
    }
    walrcv->startTime = now;

    // Initialize streaming positions for new timeline
    if (walrcv->receiveStart == 0 || walrcv->receivedTLI != tli) {
        walrcv->flushedUpto = recptr;
        walrcv->receivedTLI = tli;
        walrcv->latestChunkStart = recptr;
    }
    walrcv->receiveStart = recptr;
    walrcv->receiveStartTLI = tli;

    Latch *latch = walrcv->latch;

    SpinLockRelease(&walrcv->mutex);

    // Start new walreceiver or wake existing one
    if (launch) {
        SendPostmasterSignal(PMSIGNAL_START_WALRECEIVER);
    } else if (latch) {
        SetLatch(latch);
    }
}
```