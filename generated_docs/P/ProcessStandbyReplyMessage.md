# ProcessStandbyReplyMessage

## Location
[src/backend/replication/walsender.c:2406-2510](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walsender.c#L2406-L2510)

## Overview
Processes reply messages from standby servers that report their current WAL positions, calculates replication lag metrics, and updates replication slot progress for both physical and logical replication.

## Definition

```c
static void
ProcessStandbyReplyMessage(void)
```
## Detailed Description
This function handles regular status updates from standby servers (walreceivers) that report their progress in receiving, flushing, and applying WAL data. The function parses the incoming message to extract write, flush, and apply positions along with timing information, then updates the shared state to reflect the standby's current status.

The function performs several key operations: it calculates round-trip lag times for write, flush, and apply operations using the lag tracking system; manages lag time clearing when the standby is fully caught up; responds to keepalive requests from the standby; updates the WalSender shared memory structure with current positions and lag metrics; releases any processes waiting for synchronous replication confirmation; and advances replication slot progress based on confirmed flush positions.

A notable optimization clears stale lag metrics when a standby reports being fully caught up in consecutive messages, which typically indicates idle periods driven by wal_receiver_status_interval timeouts.

## Parameters / Member Variables
This function takes no parameters but processes data from the global  buffer containing:
- : LSN up to which WAL has been written to disk on standby
- : LSN up to which WAL has been flushed to disk on standby
- : LSN up to which WAL has been applied on standby
- : Timestamp when the reply was sent by standby
- : Boolean indicating if standby requests a keepalive response

## Dependencies
- Functions called/Symbols referenced:
  - [pq_getmsgint64](../p/pq_getmsgint64.md), pq_getmsgbyte (message parsing)
  - [LagTrackerRead](../L/LagTrackerRead.md) (lag calculation)
  - [WalSndKeepalive](../W/WalSndKeepalive.md) (keepalive responses)
  - [SyncRepReleaseWaiters](../S/SyncRepReleaseWaiters.md) (synchronous replication)
  - [LogicalConfirmReceivedLocation](../L/LogicalConfirmReceivedLocation.md), PhysicalConfirmReceivedLocation (slot advancement)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md), timestamptz_to_str (timing functions)
- Called from (representative examples):
  - [ProcessStandbyMessage](ProcessStandbyMessage.md)

## Notes and Other Information
- Uses spinlocks for thread-safe updates to shared WalSender state
- Implements lag time clearing logic to avoid displaying stale metrics during idle periods
- Handles both logical and physical replication slot advancement based on slot type
- Provides detailed debug logging of received positions and lag metrics
- Critical for synchronous replication coordination and monitoring
- Located in src/backend/replication/walsender.c:2406-2510

## Simplified Source

```c
static void
ProcessStandbyReplyMessage(void)
{
    XLogRecPtr writePtr, flushPtr, applyPtr;
    bool replyRequested;
    TimeOffset writeLag, flushLag, applyLag;
    static bool fullyAppliedLastTime = false;

    // Parse standby reply message
    writePtr = pq_getmsgint64(&reply_message);
    flushPtr = pq_getmsgint64(&reply_message);
    applyPtr = pq_getmsgint64(&reply_message);
    replyTime = pq_getmsgint64(&reply_message);
    replyRequested = pq_getmsgbyte(&reply_message);

    // Calculate replication lag times
    TimestampTz now = GetCurrentTimestamp();
    writeLag = LagTrackerRead(SYNC_REP_WAIT_WRITE, writePtr, now);
    flushLag = LagTrackerRead(SYNC_REP_WAIT_FLUSH, flushPtr, now);
    applyLag = LagTrackerRead(SYNC_REP_WAIT_APPLY, applyPtr, now);

    // Clear stale lag metrics if standby is fully caught up twice in a row
    bool clearLagTimes = false;
    if (applyPtr == sentPtr) {
        if (fullyAppliedLastTime)
            clearLagTimes = true;
        fullyAppliedLastTime = true;
    } else {
        fullyAppliedLastTime = false;
    }

    // Send keepalive response if requested
    if (replyRequested)
        WalSndKeepalive(false, InvalidXLogRecPtr);

    // Update shared WalSender state with current positions and lag metrics
    WalSnd *walsnd = MyWalSnd;
    SpinLockAcquire(&walsnd->mutex);
    walsnd->write = writePtr;
    walsnd->flush = flushPtr;
    walsnd->apply = applyPtr;
    if (writeLag != -1 || clearLagTimes) walsnd->writeLag = writeLag;
    if (flushLag != -1 || clearLagTimes) walsnd->flushLag = flushLag;
    if (applyLag != -1 || clearLagTimes) walsnd->applyLag = applyLag;
    walsnd->replyTime = replyTime;
    SpinLockRelease(&walsnd->mutex);

    // Release waiting sync rep processes
    if (!am_cascading_walsender)
        SyncRepReleaseWaiters();

    // Advance replication slot based on confirmed flush position
    if (MyReplicationSlot && flushPtr != InvalidXLogRecPtr) {
        if (SlotIsLogical(MyReplicationSlot))
            LogicalConfirmReceivedLocation(flushPtr);
        else
            PhysicalConfirmReceivedLocation(flushPtr);
    }
}
```