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
  - [pq_beginmessage](../p/pq_beginmessage.md)
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

## Simplified Source

```c
// Simplified version of StartReplication
static void StartReplication(StartReplicationCmd *cmd) {
    StringInfoData buf;
    XLogRecPtr FlushPtr;
    TimeLineID FlushTLI;

    // Phase 1: Set up WAL reader for physical replication
    xlogreader = XLogReaderAllocate(wal_segment_size, NULL,
                                   XL_ROUTINE(.segment_open = WalSndSegmentOpen,
                                             .segment_close = wal_segment_close),
                                   NULL);
    if (!xlogreader) {
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
                       errmsg("out of memory")));
    }

    // Phase 2: Handle replication slot if specified
    if (cmd->slotname) {
        ReplicationSlotAcquire(cmd->slotname, true);
        if (SlotIsLogical(MyReplicationSlot)) {
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                           errmsg("cannot use logical slot for physical replication")));
        }
    }

    // Phase 3: Determine timeline and flush position
    am_cascading_walsender = RecoveryInProgress();
    if (am_cascading_walsender) {
        FlushPtr = GetStandbyFlushRecPtr(&FlushTLI);
    } else {
        FlushPtr = GetFlushRecPtr(&FlushTLI);
    }

    // Phase 4: Timeline selection and validation
    if (cmd->timeline != 0) {
        sendTimeLine = cmd->timeline;
        if (sendTimeLine == FlushTLI) {
            // Current timeline - no validation needed
            sendTimeLineIsHistoric = false;
            sendTimeLineValidUpto = InvalidXLogRecPtr;
        } else {
            // Historic timeline - validate against timeline history
            sendTimeLineIsHistoric = true;
            List *timeLineHistory = readTimeLineHistory(FlushTLI);
            XLogRecPtr switchpoint = tliSwitchPoint(cmd->timeline, timeLineHistory,
                                                   &sendTimeLineNextTLI);
            list_free_deep(timeLineHistory);

            // Ensure requested startpoint is valid on this timeline
            if (!XLogRecPtrIsInvalid(switchpoint) && switchpoint < cmd->startpoint) {
                ereport(ERROR, (errmsg("requested starting point is not in server history")));
            }
            sendTimeLineValidUpto = switchpoint;
        }
    } else {
        // Use current timeline
        sendTimeLine = FlushTLI;
        sendTimeLineValidUpto = InvalidXLogRecPtr;
        sendTimeLineIsHistoric = false;
    }

    // Phase 5: Start streaming if we have data to send
    streamingDoneSending = streamingDoneReceiving = false;

    if (!sendTimeLineIsHistoric || cmd->startpoint < sendTimeLineValidUpto) {
        // Set initial catchup state
        WalSndSetState(WALSNDSTATE_CATCHUP);

        // Send CopyBothResponse to initiate streaming protocol
        pq_beginmessage(&buf, PqMsg_CopyBothResponse);
        pq_sendbyte(&buf, 0);
        pq_sendint16(&buf, 0);
        pq_endmessage(&buf);
        pq_flush();

        // Validate start position is not beyond flush position
        if (FlushPtr < cmd->startpoint) {
            ereport(ERROR, (errmsg("requested starting point is ahead of WAL flush position")));
        }

        // Initialize streaming position
        sentPtr = cmd->startpoint;
        SpinLockAcquire(&MyWalSnd->mutex);
        MyWalSnd->sentPtr = sentPtr;
        SpinLockRelease(&MyWalSnd->mutex);

        // Initialize synchronous replication and enter main loop
        SyncRepInitConfig();
        replication_active = true;
        WalSndLoop(XLogSendPhysical);  // Main streaming loop - never returns normally

        // Cleanup after streaming ends
        replication_active = false;
        if (got_STOPPING) {
            proc_exit(0);
        }
        WalSndSetState(WALSNDSTATE_STARTUP);
    }

    // Phase 6: Release replication slot if used
    if (cmd->slotname) {
        ReplicationSlotRelease();
    }

    // Phase 7: Send result for historic timelines
    if (sendTimeLineIsHistoric) {
        // Send single-row result with next timeline info
        char startpos_str[18];
        snprintf(startpos_str, sizeof(startpos_str), "%X/%X",
                LSN_FORMAT_ARGS(sendTimeLineValidUpto));

        // Create and send tuple with next timeline ID and start position
        DestReceiver *dest = CreateDestReceiver(DestRemoteSimple);
        TupleDesc tupdesc = CreateTemplateTupleDesc(2);
        TupleDescInitBuiltinEntry(tupdesc, 1, "next_tli", INT8OID, -1, 0);
        TupleDescInitBuiltinEntry(tupdesc, 2, "next_tli_startpos", TEXTOID, -1, 0);

        TupOutputState *tstate = begin_tup_output_tupdesc(dest, tupdesc, &TTSOpsVirtual);
        Datum values[2] = {
            Int64GetDatum((int64) sendTimeLineNextTLI),
            CStringGetTextDatum(startpos_str)
        };
        bool nulls[2] = {false, false};

        do_tup_output(tstate, values, nulls);
        end_tup_output(tstate);
    }

    // Send completion message
    EndReplicationCommand("START_STREAMING");
}
```

Key simplifications made:
- Removed verbose comments and kept essential phase descriptions
- Consolidated error handling with simplified messages
- Abstracted complex tuple output operations while preserving logic
- Focused on main execution path with clear phase separation
- Maintained all critical validations and state management
- Simplified variable declarations and reduced redundant code