# StartLogicalReplication

## Location
src/backend/replication/walsender.c: 1456 - 1548

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
  - CheckLogicalDecodingRequirements
  - ReplicationSlotAcquire
  - RecoveryInProgress
  - CreateDecodingContext
  - logical_read_xlog_page
  - WalSndSegmentOpen
  - wal_segment_close
  - WalSndPrepareWrite
  - WalSndWriteData
  - WalSndUpdateProgress
  - WalSndSetState
  - pq_beginmessage
  - pq_sendbyte
  - pq_sendint16
  - pq_endmessage
  - pq_flush
  - XLogBeginRead
  - SyncRepInitConfig
  - WalSndLoop
  - XLogSendLogical
  - FreeDecodingContext
  - ReplicationSlotRelease
  - SetQueryCompletion
  - EndCommand
- Called from (representative examples):
  - exec_replication_command

## Notes and Other Information
- This function handles the transition from setup to active logical replication streaming
- Includes special handling for cascading walsender promotion scenarios to ensure clean disconnection
- Sets up the PostgreSQL wire protocol for streaming replication with CopyBothResponse
- Manages both local and shared memory state for replication progress tracking
- The main replication work happens in WalSndLoop with the XLogSendLogical callback
- Proper cleanup ensures resources are freed even when replication is interrupted
- Critical for PostgreSQL's logical replication feature used in logical standby servers and change data capture scenarios