# GetStandbyFlushRecPtr

## Location
[src/backend/replication/walsender.c:3546-3578](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walsender.c#L3546-L3578)

## Overview
GetStandbyFlushRecPtr returns the latest WAL position that has been safely flushed to disk on a standby server, considering both replayed WAL and received but not yet replayed WAL from the same timeline.

## Definition

```c
XLogRecPtr
GetStandbyFlushRecPtr(TimeLineID *tli)
```
## Detailed Description
GetStandbyFlushRecPtr determines the safe WAL position that can be used for cascading replication or slot synchronization operations on a standby server. The function implements a dual-source strategy to maximize the available WAL range:

1. **Replayed WAL**: Uses GetXLogReplayRecPtr to get the position of WAL that has been applied to the database
2. **Received WAL**: Uses GetWalRcvFlushRecPtr to get the position of WAL that has been received and flushed by walreceiver but not yet replayed

The function returns the furthest safe position by taking the replay pointer as the baseline and extending it to the receive pointer if both are on the same timeline. This optimization allows cascading WAL senders to stream more current data without waiting for WAL replay to catch up.

The function includes safety assertions to ensure it's only called in appropriate contexts: either during cascading WAL sending operations or during replication slot synchronization.

## Parameters / Member Variables
- : Optional pointer to TimeLineID that will be set to the timeline ID of the last replayed WAL record

## Dependencies
- Functions called/Symbols referenced:
  - IsSyncingReplicationSlots
  - [GetWalRcvFlushRecPtr](GetWalRcvFlushRecPtr.md)
  - [GetXLogReplayRecPtr](GetXLogReplayRecPtr.md)
  - Assert
- Called from (representative examples):
  - [synchronize_one_slot](../s/synchronize_one_slot.md)
  - [IdentifySystem](../I/IdentifySystem.md)
  - [StartReplication](../S/StartReplication.md)
  - [XLogSendPhysical](../X/XLogSendPhysical.md)

## Notes and Other Information
- Should only be called when the server is in recovery mode (standby)
- Used by cascading WAL senders to determine how much WAL can be safely streamed to downstream standbys
- Also used by slot synchronization operations to validate remote slot LSNs before local synchronization
- Optimizes cascading replication performance by allowing streaming of received but not yet replayed WAL
- Timeline safety is ensured by only extending to received WAL when it's on the same timeline as replayed WAL
- The function name reflects its role in providing the 'flush' boundary for standby operations
- Returns the more advanced position between replay and receive pointers when timeline conditions are met