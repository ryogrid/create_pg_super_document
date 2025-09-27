# StartLogicalReplication

## Location
[src/backend/replication/walsender.c:1456-1548](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walsender.c#L1456-L1548)

## Overview
Initiates logical replication streaming by acquiring a replication slot, setting up the decoding context, and entering the main WAL sending loop.

## Definition
```c
static void StartLogicalReplication(StartReplicationCmd *cmd)
```

## Detailed Description
StartLogicalReplication is the main entry point for beginning logical replication streaming in PostgreSQL. It orchestrates the complex process of setting up logical decoding and streaming decoded changes to a replication client.

The function performs several critical steps: First, it validates that logical decoding requirements are met and acquires the specified replication slot. It handles cascading walsender scenarios by forcing disconnection if the server has been promoted from standby to primary. Next, it creates a decoding context starting from the previously acknowledged position, sets up the protocol communication by sending a CopyBothResponse message, and positions the WAL reader at the restart LSN.

The function updates replication state tracking in both local and shared memory, initializes synchronous replication configuration, and enters the main WalSndLoop with XLogSendLogical as the sending function. Upon completion or interruption, it performs cleanup by freeing the decoding context, releasing the slot, and properly terminating the replication session.

## Parameters / Member Variables
- `cmd`: StartReplicationCmd structure containing the slot name, starting LSN (startpoint), and decoding options for configuring the logical replication session

## Dependencies
- Functions called/Symbols referenced:
  - [CheckLogicalDecodingRequirements](../C/CheckLogicalDecodingRequirements.md)
  - [ReplicationSlotAcquire](../R/ReplicationSlotAcquire.md)
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - [CreateDecodingContext](../C/CreateDecodingContext.md)
  - [logical_read_xlog_page](../l/logical_read_xlog_page.md)
  - [WalSndSegmentOpen](../W/WalSndSegmentOpen.md)
  - [wal_segment_close](../w/wal_segment_close.md)
  - [WalSndPrepareWrite](../W/WalSndPrepareWrite.md)
  - [WalSndWriteData](../W/WalSndWriteData.md)
  - [WalSndUpdateProgress](../W/WalSndUpdateProgress.md)
  - [WalSndSetState](../W/WalSndSetState.md)
  - [pq_beginmessage](../p/pq_beginmessage.md)
  - [pq_sendbyte](../p/pq_sendbyte.md)
  - [pq_sendint16](../p/pq_sendint16.md)
  - [pq_endmessage](../p/pq_endmessage.md)
  - pq_flush
  - [XLogBeginRead](../X/XLogBeginRead.md)
  - [SyncRepInitConfig](SyncRepInitConfig.md)
  - [WalSndLoop](../W/WalSndLoop.md)
  - [XLogSendLogical](../X/XLogSendLogical.md)
  - [FreeDecodingContext](../F/FreeDecodingContext.md)
  - [ReplicationSlotRelease](../R/ReplicationSlotRelease.md)
  - [SetQueryCompletion](SetQueryCompletion.md)
  - [EndCommand](../E/EndCommand.md)
- Called from (representative examples):
  - [exec_replication_command](../e/exec_replication_command.md)

## Notes and Other Information
- This function handles the transition from setup to active logical replication streaming
- Includes special handling for cascading walsender promotion scenarios to ensure clean disconnection
- Sets up the PostgreSQL wire protocol for streaming replication with CopyBothResponse
- Manages both local and shared memory state for replication progress tracking
- The main replication work happens in WalSndLoop with the XLogSendLogical callback
- Proper cleanup ensures resources are freed even when replication is interrupted
- Critical for PostgreSQL's logical replication feature used in logical standby servers and change data capture scenarios

## Simplified Source

```c
// Simplified version of StartLogicalReplication
static void StartLogicalReplication(StartReplicationCmd *cmd) {
    StringInfoData buf;
    QueryCompletion qc;

    // Step 1: Validate requirements and acquire replication slot
    CheckLogicalDecodingRequirements();
    Assert(!MyReplicationSlot);
    ReplicationSlotAcquire(cmd->slotname, true);

    // Step 2: Handle cascading walsender promotion case
    if (am_cascading_walsender && !RecoveryInProgress()) {
        ereport(LOG, (errmsg("terminating walsender process after promotion")));
        got_STOPPING = true;
    }

    // Step 3: Create logical decoding context starting from acknowledged position
    logical_decoding_ctx = CreateDecodingContext(
        cmd->startpoint,
        cmd->options,
        false,
        callback_functions,  // XL_ROUTINE with read/write/progress callbacks
        WalSndPrepareWrite,
        WalSndWriteData,
        WalSndUpdateProgress
    );
    xlogreader = logical_decoding_ctx->reader;

    // Step 4: Update state and send protocol response to client
    WalSndSetState(WALSNDSTATE_CATCHUP);

    // Send CopyBothResponse message to start streaming protocol
    pq_beginmessage(&buf, PqMsg_CopyBothResponse);
    pq_sendbyte(&buf, 0);
    pq_sendint16(&buf, 0);
    pq_endmessage(&buf);
    pq_flush();

    // Step 5: Initialize WAL reading and position tracking
    XLogBeginRead(logical_decoding_ctx->reader, MyReplicationSlot->data.restart_lsn);
    sentPtr = MyReplicationSlot->data.confirmed_flush;

    // Update shared memory state
    SpinLockAcquire(&MyWalSnd->mutex);
    MyWalSnd->sentPtr = MyReplicationSlot->data.restart_lsn;
    SpinLockRelease(&MyWalSnd->mutex);

    // Step 6: Start replication and enter main loop
    replication_active = true;
    SyncRepInitConfig();
    WalSndLoop(XLogSendLogical);  // Main streaming loop

    // Step 7: Cleanup when loop exits
    FreeDecodingContext(logical_decoding_ctx);
    ReplicationSlotRelease();
    replication_active = false;

    if (got_STOPPING)
        proc_exit(0);

    WalSndSetState(WALSNDSTATE_STARTUP);
    SetQueryCompletion(&qc, CMDTAG_COPY, 0);
    EndCommand(&qc, DestRemote, false);
}
```

Key simplifications made:
- Consolidated callback function setup into a single comment block
- Abstracted the complex XL_ROUTINE macro into "callback_functions" with explanation
- Grouped related operations into logical steps with descriptive comments
- Simplified the protocol message construction while maintaining the essential flow
- Focused on the main execution path and key state transitions
- Preserved all critical operations: slot acquisition, context creation, loop execution, and cleanup