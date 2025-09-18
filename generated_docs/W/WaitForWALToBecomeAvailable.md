# WaitForWALToBecomeAvailable

## Location
src/backend/access/transam/xlogrecovery.c: 3542 - 4030

## Overview
WaitForWALToBecomeAvailable implements a sophisticated state machine that manages WAL retrieval from multiple sources (archive, pg_wal, streaming) during PostgreSQL recovery, handling source switching and waiting logic.

## Definition


## Detailed Description
WaitForWALToBecomeAvailable is the central orchestrator for WAL availability management during recovery operations. It implements a state machine with multiple sources: XLOG_FROM_ARCHIVE, XLOG_FROM_PG_WAL, and XLOG_FROM_STREAM. The function manages source transitions based on availability and failure conditions, handles timeline validation, and coordinates with the WAL receiver for streaming scenarios.

The state machine progression is:
1. Try archive/pg_wal sources 
2. Check for promotion triggers
3. Switch to streaming from primary
4. Handle timeline rescans
5. Sleep with retry intervals before restarting the cycle

Key behaviors include:
- Automatic source switching on failure with sophisticated retry logic
- Timeline history management and validation
- WAL receiver lifecycle management (start/stop)
- Non-blocking operation support for prefetching
- Recovery pause handling and startup process interrupt management
- Integration with PostgreSQL's latch-based waiting mechanism

## Parameters / Member Variables
- : XLogRecPtr indicating the WAL location that needs to be available
- : Boolean flag indicating random access mode for timeline handling
- : Boolean flag indicating whether fetching a checkpoint record
- : XLogRecPtr position of the actual record of interest (for timeline decisions)
- : TimeLineID currently being replayed
- : XLogRecPtr of current replay position for timeline validation
- : Boolean flag enabling immediate return instead of waiting

## Dependencies
- Functions called/Symbols referenced:
  - [CheckForStandbyTrigger](../C/CheckForStandbyTrigger.md)
  - [XLogShutdownWalRcv](../X/XLogShutdownWalRcv.md)
  - [WalRcvStreaming](WalRcvStreaming.md)
  - [XLogFileReadAnyTLI](../X/XLogFileReadAnyTLI.md)
  - [RequestXLogStreaming](../R/RequestXLogStreaming.md)
  - [GetWalRcvFlushRecPtr](../G/GetWalRcvFlushRecPtr.md)
  - [WaitLatch](WaitLatch.md)
  - [rescanLatestTimeLine](../r/rescanLatestTimeLine.md)
  - [tliOfPointInHistory](../t/tliOfPointInHistory.md)
  - [readTimeLineHistory](../r/readTimeLineHistory.md)
  - [HandleStartupProcInterrupts](../H/HandleStartupProcInterrupts.md)
- Called from (representative examples):
  - [XLogPageRead](../X/XLogPageRead.md)

## Notes and Other Information
- Returns XLREAD_SUCCESS when WAL becomes available, XLREAD_FAIL on permanent failure, or XLREAD_WOULDBLOCK for non-blocking operations
- Manages global state variables including currentSource, lastSourceFailed, readFile, and flushedUpto
- Implements sophisticated retry timing using wal_retrieve_retry_interval to avoid busy-waiting
- Coordinates timeline switches and history file management for point-in-time recovery scenarios
- The function includes comprehensive logging and maintains source tracking for debugging purposes
- In standby mode, it manages the promotion trigger checking and graceful transition to read-only mode
- Handles recovery pause states and ensures proper cleanup of resources during state transitions