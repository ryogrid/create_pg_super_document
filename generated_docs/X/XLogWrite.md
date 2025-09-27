# XLogWrite

## Location
[src/backend/access/transam/xlog.c:2297-2613](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L2297-L2613)

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
  - [XLogFileClose](XLogFileClose.md)/XLogFileOpen/XLogFileInit (file operations)
  - [pg_pwrite](../p/pg_pwrite.md) (physical write operation)
  - [issue_xlog_fsync](../i/issue_xlog_fsync.md) (fsync operations)
  - [XLogCheckpointNeeded](XLogCheckpointNeeded.md) (checkpoint threshold checking)
  - WalSndWakeupRequest (walsender notification)
  - [XLogArchiveNotifySeg](XLogArchiveNotifySeg.md) (archival notification)
  - [RequestCheckpoint](../R/RequestCheckpoint.md) (checkpoint initiation)
- Global variables used:
  - XLogCtl (shared WAL control structure)
  - LogwrtResult (local write result tracking)
  - openLogFile/openLogSegNo/openLogTLI (current open file state)
- Called from (representative examples):
  - [XLogFlush](XLogFlush.md) (in xlog.c:2902)
  - [XLogBackgroundFlush](XLogBackgroundFlush.md) (in xlog.c:3080)
  - [AdvanceXLInsertBuffer](../A/AdvanceXLInsertBuffer.md) (in xlog.c:2060)

## Notes and Other Information
- Must be called with WALWriteLock held and within a critical section
- [WaitXLogInsertionsToFinish](../W/WaitXLogInsertionsToFinish.md)(WriteRqst) must be called before acquiring the lock
- Implements sophisticated page batching to minimize system calls by gathering consecutive pages
- Handles WAL segment file transitions automatically, including creation of new segment files
- Performs immediate fsync when completing a WAL segment to optimize performance
- Triggers checkpoint requests when WAL consumption exceeds configured thresholds
- Updates shared memory atomically with proper memory barriers for concurrent readers
- Includes comprehensive error handling with PANIC on write failures
- Supports flexible writing mode to avoid unnecessary partial writes in high-throughput scenarios

## Simplified Source

```c
// Simplified version of XLogWrite
static void XLogWrite(XLogwrtRqst WriteRqst, TimeLineID tli, bool flexible) {
    bool ispartialpage;
    bool last_iteration;
    bool finishing_seg;
    int curridx;
    int npages = 0;
    int startidx = 0;
    uint32 startoffset = 0;

    // Core logic step 1: Update local write result from shared memory
    RefreshXLogWriteResult(LogwrtResult);

    // Core logic step 2: Find starting buffer index for unwritten data
    curridx = XLogRecPtrToBufIdx(LogwrtResult.Write);

    // Core logic step 3: Main write loop - process pages until request satisfied
    while (LogwrtResult.Write < WriteRqst.Write) {
        // Validate we're not ahead of insert process
        XLogRecPtr EndPtr = pg_atomic_read_u64(&XLogCtl->xlblocks[curridx]);
        if (LogwrtResult.Write >= EndPtr)
            elog(PANIC, "xlog write request is past end of log");

        // Advance to end of current buffer page
        LogwrtResult.Write = EndPtr;
        ispartialpage = WriteRqst.Write < LogwrtResult.Write;

        // Core logic step 4: Handle WAL segment file transitions
        if (!XLByteInPrevSeg(LogwrtResult.Write, openLogSegNo, wal_segment_size)) {
            // Switch to new logfile segment
            if (openLogFile >= 0)
                XLogFileClose();
            XLByteToPrevSeg(LogwrtResult.Write, openLogSegNo, wal_segment_size);
            openLogTLI = tli;
            openLogFile = XLogFileInit(openLogSegNo, tli);
            ReserveExternalFD();
        }

        // Ensure current logfile is open
        if (openLogFile < 0) {
            XLByteToPrevSeg(LogwrtResult.Write, openLogSegNo, wal_segment_size);
            openLogTLI = tli;
            openLogFile = XLogFileOpen(openLogSegNo, tli);
            ReserveExternalFD();
        }

        // Core logic step 5: Accumulate pages for batch writing
        if (npages == 0) {
            // Start new batch
            startidx = curridx;
            startoffset = XLogSegmentOffset(LogwrtResult.Write - XLOG_BLCKSZ, wal_segment_size);
        }
        npages++;

        // Determine if we should write the accumulated pages now
        last_iteration = WriteRqst.Write <= LogwrtResult.Write;
        finishing_seg = !ispartialpage &&
                       (startoffset + npages * XLOG_BLCKSZ) >= wal_segment_size;

        // Core logic step 6: Write accumulated pages to disk
        if (last_iteration || curridx == XLogCtl->XLogCacheBlck || finishing_seg) {
            char *from = XLogCtl->pages + startidx * (Size) XLOG_BLCKSZ;
            Size nbytes = npages * (Size) XLOG_BLCKSZ;
            Size nleft = nbytes;

            // Write loop with retry on partial writes
            do {
                pgstat_report_wait_start(WAIT_EVENT_WAL_WRITE);
                ssize_t written = pg_pwrite(openLogFile, from, nleft, startoffset);
                pgstat_report_wait_end();

                if (written <= 0) {
                    if (errno == EINTR)
                        continue;
                    ereport(PANIC, "could not write to log file");
                }

                nleft -= written;
                from += written;
                startoffset += written;
            } while (nleft > 0);

            npages = 0;

            // Core logic step 7: Handle segment completion
            if (finishing_seg) {
                // Fsync completed segment immediately
                issue_xlog_fsync(openLogFile, openLogSegNo, tli);
                WalSndWakeupRequest();
                LogwrtResult.Flush = LogwrtResult.Write;

                // Notify archiver and update timing
                if (XLogArchivingActive())
                    XLogArchiveNotifySeg(openLogSegNo, tli);
                XLogCtl->lastSegSwitchTime = (pg_time_t) time(NULL);
                XLogCtl->lastSegSwitchLSN = LogwrtResult.Flush;

                // Request checkpoint if needed
                if (IsUnderPostmaster && XLogCheckpointNeeded(openLogSegNo)) {
                    (void) GetRedoRecPtr();
                    if (XLogCheckpointNeeded(openLogSegNo))
                        RequestCheckpoint(CHECKPOINT_CAUSE_XLOG);
                }
            }
        }

        // Handle partial page or move to next buffer
        if (ispartialpage) {
            LogwrtResult.Write = WriteRqst.Write;
            break;
        }
        curridx = NextBufIdx(curridx);

        // Support flexible mode - break early if we wrote something
        if (flexible && npages == 0)
            break;
    }

    // Core logic step 8: Handle flush request
    if (LogwrtResult.Flush < WriteRqst.Flush &&
        LogwrtResult.Flush < LogwrtResult.Write) {

        // Ensure correct file is open for fsync
        if (wal_sync_method != WAL_SYNC_METHOD_OPEN &&
            wal_sync_method != WAL_SYNC_METHOD_OPEN_DSYNC) {
            // Open appropriate file if needed
            if (openLogFile >= 0 &&
                !XLByteInPrevSeg(LogwrtResult.Write, openLogSegNo, wal_segment_size))
                XLogFileClose();
            if (openLogFile < 0) {
                XLByteToPrevSeg(LogwrtResult.Write, openLogSegNo, wal_segment_size);
                openLogTLI = tli;
                openLogFile = XLogFileOpen(openLogSegNo, tli);
                ReserveExternalFD();
            }
            issue_xlog_fsync(openLogFile, openLogSegNo, tli);
        }

        WalSndWakeupRequest();
        LogwrtResult.Flush = LogwrtResult.Write;
    }

    // Core logic step 9: Update shared memory atomically
    SpinLockAcquire(&XLogCtl->info_lck);
    if (XLogCtl->LogwrtRqst.Write < LogwrtResult.Write)
        XLogCtl->LogwrtRqst.Write = LogwrtResult.Write;
    if (XLogCtl->LogwrtRqst.Flush < LogwrtResult.Flush)
        XLogCtl->LogwrtRqst.Flush = LogwrtResult.Flush;
    SpinLockRelease(&XLogCtl->info_lck);

    // Update atomic variables with proper barriers
    pg_atomic_write_u64(&XLogCtl->logWriteResult, LogwrtResult.Write);
    pg_write_barrier();
    pg_atomic_write_u64(&XLogCtl->logFlushResult, LogwrtResult.Flush);
}
```

Key simplifications made:
- Removed detailed I/O timing instrumentation for clarity
- Consolidated error handling paths
- Simplified complex conditional logic in the main loop
- Abstracted low-level memory barrier and assertion details
- Focused on the main execution path and core algorithm
- Removed platform-specific optimization code
- Added descriptive comments for each major logic step