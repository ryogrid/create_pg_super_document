# WalReceiverMain

## Location
[src/backend/replication/walreceiver.c:183-664](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walreceiver.c#L183-L664)

## Overview
WalReceiverMain is the main entry point for the WAL receiver process that handles streaming replication from a primary PostgreSQL server to a standby server.

## Definition
void WalReceiverMain(char *startup_data, size_t startup_data_len)

## Detailed Description
This function implements the core logic of the WAL receiver process in PostgreSQL's streaming replication system. It establishes a connection to the primary server, manages the streaming of WAL (Write-Ahead Log) records, and handles various operational states throughout the replication lifecycle.

The function operates in several key phases:
1. **Initialization**: Sets up process type, shared memory state, and signal handlers
2. **Connection establishment**: Connects to the primary server using provided connection information
3. **System validation**: Verifies system identifiers and timeline compatibility between primary and standby
4. **Timeline management**: Fetches missing timeline history files to maintain consistency
5. **Streaming loop**: Continuously receives WAL data, processes messages, and manages timeouts
6. **Error handling**: Gracefully handles connection issues, timeouts, and shutdown requests

The function runs in an infinite loop, streaming WAL data until instructed to restart with new parameters or until termination. It manages temporary replication slots when requested and maintains status communication with both the startup process and the primary server.

## Parameters / Member Variables
- : Startup data passed to the process (currently unused, expected to be NULL)
- : Length of startup data (expected to be 0)

## Dependencies
- Functions called/Symbols referenced:
  - [AuxiliaryProcessMainCommon](../A/AuxiliaryProcessMainCommon.md)
  - walrcv_connect, walrcv_identify_system, walrcv_startstreaming
  - [ProcessWalRcvInterrupts](../P/ProcessWalRcvInterrupts.md)
  - [WalRcvFetchTimeLineHistoryFiles](WalRcvFetchTimeLineHistoryFiles.md)
  - [WalRcvWaitForStartPosition](WalRcvWaitForStartPosition.md)
  - [XLogWalRcvProcessMsg](../X/XLogWalRcvProcessMsg.md), XLogWalRcvSendReply, XLogWalRcvFlush
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md), RecoveryInProgress
  - [WaitLatchOrSocket](WaitLatchOrSocket.md), ResetLatch
  - [on_shmem_exit](../o/on_shmem_exit.md), WalRcvDie

- Called from (representative examples):
  - child_process_kind (process launcher)
  - [walrcv_clear_result](../w/walrcv_clear_result.md) (via function pointer)

## Notes and Other Information
- Central component of PostgreSQL's streaming replication infrastructure
- Manages connection state in shared memory for coordination with other processes
- Implements timeout-based communication with primary server to detect connection issues
- Handles timeline switches and history file management automatically
- Supports both permanent and temporary replication slot management
- Uses latch-based signaling for efficient process communication and interrupt handling
- Coordinates with the startup process for recovery management and restart scenarios