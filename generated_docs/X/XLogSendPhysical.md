# XLogSendPhysical

## Location
[src/backend/replication/walsender.c:3100-3409](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walsender.c#L3100-L3409)

## Overview
XLogSendPhysical reads and streams physical WAL data to replication clients, handling timeline switches, buffering constraints, and various replication scenarios including primary-standby and cascading replication.

## Definition

```c
static void
XLogSendPhysical(void)
```
## Detailed Description
XLogSendPhysical is the core function responsible for streaming physical WAL data to replication clients. It implements sophisticated logic to handle multiple replication scenarios:

1. **Historic Timeline Streaming**: When streaming from a historic timeline, it ensures streaming only up to the timeline switch point.

2. **Cascading Replication**: On standby servers acting as cascading WAL senders, it streams WAL that has been replayed or received, detecting timeline changes and promotions dynamically.

3. **Primary Streaming**: On primary servers, it streams all WAL that has been flushed to disk for durability guarantees.

The function implements intelligent buffering by reading up to MAX_SEND_SIZE bytes, with careful boundary alignment to ensure WAL records are never split across messages. It attempts to read from WAL buffers first for performance, then falls back to disk-based WAL files when necessary.

The function also handles file reloading scenarios during recovery where WAL files might be replaced from archive, implementing retry logic to ensure data consistency.

## Parameters / Member Variables
This function takes no parameters but operates on several global variables:
- : Signal to transition to stopping state
- : Flag indicating streaming completion
- : Whether streaming a historic timeline
- : Whether this is a cascading WAL sender
- : Last WAL position successfully sent

## Dependencies
- Functions called/Symbols referenced:
  - [WalSndSetState](../W/WalSndSetState.md)
  - [GetStandbyFlushRecPtr](../G/GetStandbyFlushRecPtr.md)
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - [GetWALInsertionTimeLine](../G/GetWALInsertionTimeLine.md)
  - [readTimeLineHistory](../r/readTimeLineHistory.md)
  - [tliSwitchPoint](../t/tliSwitchPoint.md)
  - [GetFlushRecPtr](../G/GetFlushRecPtr.md)
  - [LagTrackerWrite](../L/LagTrackerWrite.md)
  - [WALReadFromBuffers](../W/WALReadFromBuffers.md)
  - [WALRead](../W/WALRead.md)
  - [CheckXLogRemoved](../C/CheckXLogRemoved.md)
  - [wal_segment_close](../w/wal_segment_close.md)
  - pq_putmessage_noblock
- Called from (representative examples):
  - [StartReplication](../S/StartReplication.md)

## Notes and Other Information
- Implements lag tracking by recording timestamps for WAL positions to measure replication lag
- Ensures WAL records are never split across messages by aligning to page boundaries
- Handles the CopyDone protocol message when reaching end of historic timelines
- Updates shared memory status and process title to reflect streaming progress
- Uses spinlocks for safe concurrent access to WAL sender shared state
- Implements retry logic for file reloading scenarios during cascading replication
- Maintains strict durability guarantees by only streaming fsynced WAL on primaries