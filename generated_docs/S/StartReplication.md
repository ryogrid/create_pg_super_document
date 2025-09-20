# StartReplication

## Location
[src/backend/replication/walsender.c:823-1054](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walsender.c#L823-L1054)

## Overview
Handles the START_REPLICATION command to initiate physical replication streaming from a specified WAL position, setting up the replication infrastructure and entering the main WAL sender loop.

## Definition

```c
static void
StartReplication(StartReplicationCmd *cmd)
```
## Detailed Description
StartReplication implements the core functionality for starting physical replication in PostgreSQL. The function performs several critical tasks:

1. **WAL Reader Setup**: Allocates and configures a WAL reader for processing WAL segments
2. **Replication Slot Management**: Acquires and validates the specified replication slot (if provided)
3. **Timeline Validation**: Determines the appropriate timeline and validates the requested starting point
4. **Protocol Initialization**: Sets up the COPY protocol for streaming WAL data
5. **Main Streaming Loop**: Enters WalSndLoop to continuously stream WAL records
6. **Result Reporting**: Returns timeline information for historic timelines

The function handles both current and historic timeline requests, performing extensive validation to ensure the requested starting point is valid and achievable. For historic timelines, it uses timeline history to validate the request and determine switch points.

## Parameters / Member Variables
- : Pointer to StartReplicationCmd structure containing:
  - : WAL position to begin replication from
  - : Timeline ID to replicate (0 means current timeline)
  - : Name of replication slot to use (optional)

## Dependencies
- Functions called/Symbols referenced:
  - [XLogReaderAllocate](../X/XLogReaderAllocate.md)
  - [WalSndSegmentOpen](../W/WalSndSegmentOpen.md)
  - [ReplicationSlotAcquire](../R/ReplicationSlotAcquire.md)
  - SlotIsLogical
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - [GetStandbyFlushRecPtr](../G/GetStandbyFlushRecPtr.md)
  - [GetFlushRecPtr](../G/GetFlushRecPtr.md)
  - [readTimeLineHistory](../r/readTimeLineHistory.md)
  - [tliSwitchPoint](../t/tliSwitchPoint.md)
  - [WalSndSetState](../W/WalSndSetState.md)
  - pq_beginmessage
  - [pq_sendbyte](../p/pq_sendbyte.md)
  - [pq_sendint16](../p/pq_sendint16.md)
  - [pq_endmessage](../p/pq_endmessage.md)
  - pq_flush
  - [SyncRepInitConfig](SyncRepInitConfig.md)
  - [WalSndLoop](../W/WalSndLoop.md)
  - [XLogSendPhysical](../X/XLogSendPhysical.md)
  - [ReplicationSlotRelease](../R/ReplicationSlotRelease.md)
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md)
  - [begin_tup_output_tupdesc](../b/begin_tup_output_tupdesc.md)
  - [do_tup_output](../d/do_tup_output.md)
  - [end_tup_output](../e/end_tup_output.md)
  - [EndReplicationCommand](../E/EndReplicationCommand.md)
- Called from:
  - [exec_replication_command](../e/exec_replication_command.md)

## Notes and Other Information
- The function never returns normally during active replication; only ereport(ERROR) returns to the main loop
- Supports both standalone and cascading replication scenarios
- Validates that logical replication slots cannot be used for physical replication
- Implements extensive timeline validation to prevent invalid replication requests
- Sets replication state to WALSNDSTATE_CATCHUP initially, then WALSNDSTATE_STARTUP after completion
- For historic timelines, returns a result set with next timeline information
- Uses shared memory to track replication progress (MyWalSnd->sentPtr)
- Integrates with synchronous replication infrastructure via SyncRepInitConfig
- Handles graceful shutdown when got_STOPPING is received
- Performs WAL flush position validation to prevent streaming future WAL positions