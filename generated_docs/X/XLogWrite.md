# XLogWrite

## Location
src/backend/access/transam/xlog.c: 2297 - 2613

## Overview
Core function responsible for writing WAL (Write-Ahead Log) data from memory buffers to disk files, with optional fsync operations, segment management, and checkpoint triggering.

## Definition
```c
static void XLogWrite(XLogwrtRqst WriteRqst, TimeLineID tli, bool flexible)
```

## Detailed Description
XLogWrite is the central mechanism for persisting WAL data from shared memory buffers to disk. It efficiently handles multiple consecutive pages by gathering them together for batch writes, manages WAL segment file transitions, performs fsync operations when needed, and triggers important housekeeping tasks like archival notifications and checkpoint requests. The function operates under strict concurrency control (must hold WALWriteLock) and ensures data durability through proper synchronization with the WAL insertion process.

## Parameters / Member Variables
- `WriteRqst`: XLogwrtRqst structure specifying the Write and Flush positions to achieve
- `tli`: TimeLineID indicating the timeline for which to write WAL data
- `flexible`: bool allowing the function to stop at convenient boundaries rather than writing exactly to WriteRqst (optimization for reducing multiple writes)

## Dependencies
- Functions called/Symbols referenced:
  - RefreshXLogWriteResult (updates local LogwrtResult)
  - XLogRecPtrToBufIdx (converts LSN to buffer index)
  - XLogFileClose/XLogFileOpen/XLogFileInit (file operations)
  - pg_pwrite (physical write operation)
  - issue_xlog_fsync (fsync operations)
  - XLogCheckpointNeeded (checkpoint threshold checking)
  - WalSndWakeupRequest (walsender notification)
  - XLogArchiveNotifySeg (archival notification)
  - RequestCheckpoint (checkpoint initiation)
- Global variables used:
  - XLogCtl (shared WAL control structure)
  - LogwrtResult (local write result tracking)
  - openLogFile/openLogSegNo/openLogTLI (current open file state)
- Called from (representative examples):
  - XLogFlush (in xlog.c:2902)
  - XLogBackgroundFlush (in xlog.c:3080)
  - AdvanceXLInsertBuffer (in xlog.c:2060)

## Notes and Other Information
- Must be called with WALWriteLock held and within a critical section
- WaitXLogInsertionsToFinish(WriteRqst) must be called before acquiring the lock
- Implements sophisticated page batching to minimize system calls by gathering consecutive pages
- Handles WAL segment file transitions automatically, including creation of new segment files
- Performs immediate fsync when completing a WAL segment to optimize performance
- Triggers checkpoint requests when WAL consumption exceeds configured thresholds
- Updates shared memory atomically with proper memory barriers for concurrent readers
- Includes comprehensive error handling with PANIC on write failures
- Supports flexible writing mode to avoid unnecessary partial writes in high-throughput scenarios