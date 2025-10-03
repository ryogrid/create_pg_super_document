# WaitForWALToBecomeAvailable

## Location
[src/backend/access/transam/xlogrecovery.c:3542-4030](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L3542-L4030)

## Overview
WaitForWALToBecomeAvailable implements a sophisticated state machine that manages WAL retrieval from multiple sources (archive, pg_wal, streaming) during PostgreSQL recovery, handling source switching and waiting logic.

## Definition

```c
static XLogPageReadResult
WaitForWALToBecomeAvailable(XLogRecPtr RecPtr, bool randAccess,
							bool fetching_ckpt, XLogRecPtr tliRecPtr,
							TimeLineID replayTLI, XLogRecPtr replayLSN,
							bool nonblocking)
```
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
- `RecPtr`: XLogRecPtr indicating the WAL location that needs to be available
- `randAccess`: Boolean flag indicating random access mode for timeline handling
- `fetching_ckpt`: Boolean flag indicating whether fetching a checkpoint record
- `tliRecPtr`: XLogRecPtr position of the actual record of interest (for timeline decisions)
- `replayTLI`: TimeLineID currently being replayed
- `replayLSN`: XLogRecPtr of current replay position for timeline validation
- `nonblocking`: Boolean flag enabling immediate return instead of waiting
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

## Simplified Source

```c
static XLogPageReadResult WaitForWALToBecomeAvailable(XLogRecPtr RecPtr, bool randAccess,
                                                     bool fetching_ckpt, XLogRecPtr tliRecPtr,
                                                     TimeLineID replayTLI, XLogRecPtr replayLSN,
                                                     bool nonblocking)
{
    static TimestampTz last_fail_time = 0;
    TimestampTz now;
    bool streaming_reply_sent = false;

    // Initialize source state
    if (!InArchiveRecovery)
        currentSource = XLOG_FROM_PG_WAL;
    else if (currentSource == XLOG_FROM_ANY || (!StandbyMode && currentSource == XLOG_FROM_STREAM))
    {
        lastSourceFailed = false;
        currentSource = XLOG_FROM_ARCHIVE;
    }

    // Main state machine loop
    for (;;)
    {
        XLogSource oldSource = currentSource;
        bool startWalReceiver = false;

        // Handle failures and advance state machine
        if (lastSourceFailed)
        {
            if (nonblocking)
                return XLREAD_WOULDBLOCK;

            switch (currentSource)
            {
                case XLOG_FROM_ARCHIVE:
                case XLOG_FROM_PG_WAL:
                    // Check for promotion trigger
                    if (StandbyMode && CheckForStandbyTrigger())
                    {
                        XLogShutdownWalRcv();
                        return XLREAD_FAIL;
                    }
                    if (!StandbyMode)
                        return XLREAD_FAIL;

                    // Move to streaming
                    currentSource = XLOG_FROM_STREAM;
                    startWalReceiver = true;
                    break;

                case XLOG_FROM_STREAM:
                    Assert(StandbyMode);
                    XLogShutdownWalRcv();

                    // Rescan timelines if needed
                    if (recoveryTargetTimeLineGoal == RECOVERY_TARGET_TIMELINE_LATEST)
                    {
                        if (rescanLatestTimeLine(replayTLI, replayLSN))
                        {
                            currentSource = XLOG_FROM_ARCHIVE;
                            break;
                        }
                    }

                    // Sleep before retrying
                    now = GetCurrentTimestamp();
                    if (!TimestampDifferenceExceeds(last_fail_time, now, wal_retrieve_retry_interval))
                    {
                        long wait_time = wal_retrieve_retry_interval -
                                       TimestampDifferenceMilliseconds(last_fail_time, now);

                        elog(LOG, "waiting for WAL to become available at %X/%X", LSN_FORMAT_ARGS(RecPtr));

                        KnownAssignedTransactionIdsIdleMaintenance();
                        WaitLatch(&XLogRecoveryCtl->recoveryWakeupLatch,
                                WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                                wait_time, WAIT_EVENT_RECOVERY_RETRIEVE_RETRY_INTERVAL);
                        ResetLatch(&XLogRecoveryCtl->recoveryWakeupLatch);

                        HandleStartupProcInterrupts();
                    }
                    last_fail_time = now;
                    currentSource = XLOG_FROM_ARCHIVE;
                    break;
            }
        }
        else if (currentSource == XLOG_FROM_PG_WAL)
        {
            // Prefer archive over pg_wal
            if (InArchiveRecovery)
                currentSource = XLOG_FROM_ARCHIVE;
        }

        // Try to read from chosen source
        lastSourceFailed = false;

        switch (currentSource)
        {
            case XLOG_FROM_ARCHIVE:
            case XLOG_FROM_PG_WAL:
                Assert(!WalRcvStreaming());

                // Close old file and reset state
                if (readFile >= 0)
                {
                    close(readFile);
                    readFile = -1;
                }
                if (randAccess)
                    curFileTLI = 0;

                // Try to read file
                readFile = XLogFileReadAnyTLI(readSegNo, DEBUG2,
                                            currentSource == XLOG_FROM_ARCHIVE ? XLOG_FROM_ANY : currentSource);
                if (readFile >= 0)
                    return XLREAD_SUCCESS;

                lastSourceFailed = true;
                break;

            case XLOG_FROM_STREAM:
                {
                    bool havedata;
                    Assert(StandbyMode);

                    // Handle WAL receiver restart
                    if (pendingWalRcvRestart && !startWalReceiver)
                    {
                        XLogShutdownWalRcv();
                        if (recoveryTargetTimeLineGoal == RECOVERY_TARGET_TIMELINE_LATEST)
                            rescanLatestTimeLine(replayTLI, replayLSN);
                        startWalReceiver = true;
                    }
                    pendingWalRcvRestart = false;

                    // Start WAL receiver if needed
                    if (startWalReceiver && PrimaryConnInfo && strcmp(PrimaryConnInfo, "") != 0)
                    {
                        XLogRecPtr ptr;
                        TimeLineID tli;

                        if (fetching_ckpt)
                        {
                            ptr = RedoStartLSN;
                            tli = RedoStartTLI;
                        }
                        else
                        {
                            ptr = RecPtr;
                            tli = tliOfPointInHistory(tliRecPtr, expectedTLEs);

                            if (curFileTLI > 0 && tli < curFileTLI)
                                elog(ERROR, "timeline inconsistency detected");
                        }

                        curFileTLI = tli;
                        SetInstallXLogFileSegmentActive();
                        RequestXLogStreaming(tli, ptr, PrimaryConnInfo, PrimarySlotName, wal_receiver_create_temp_slot);
                        flushedUpto = 0;
                    }

                    // Check if WAL receiver is active
                    if (!WalRcvStreaming())
                    {
                        lastSourceFailed = true;
                        break;
                    }

                    // Check for new data
                    if (RecPtr < flushedUpto)
                        havedata = true;
                    else
                    {
                        XLogRecPtr latestChunkStart;
                        flushedUpto = GetWalRcvFlushRecPtr(&latestChunkStart, &receiveTLI);

                        if (RecPtr < flushedUpto && receiveTLI == curFileTLI)
                        {
                            havedata = true;
                            if (latestChunkStart <= RecPtr)
                            {
                                XLogReceiptTime = GetCurrentTimestamp();
                                SetCurrentChunkStartTime(XLogReceiptTime);
                            }
                        }
                        else
                            havedata = false;
                    }

                    if (havedata)
                    {
                        // Open file if needed
                        if (readFile < 0)
                        {
                            if (!expectedTLEs)
                                expectedTLEs = readTimeLineHistory(recoveryTargetTLI);
                            readFile = XLogFileRead(readSegNo, PANIC, receiveTLI, XLOG_FROM_STREAM, false);
                            Assert(readFile >= 0);
                        }
                        else
                        {
                            readSource = XLOG_FROM_STREAM;
                            XLogReceiptSource = XLOG_FROM_STREAM;
                            return XLREAD_SUCCESS;
                        }
                        break;
                    }

                    if (nonblocking)
                        return XLREAD_WOULDBLOCK;

                    // Check for trigger
                    if (CheckForStandbyTrigger())
                    {
                        lastSourceFailed = true;
                        break;
                    }

                    // Send replication status
                    if (!streaming_reply_sent)
                    {
                        WalRcvForceReply();
                        streaming_reply_sent = true;
                    }

                    // Background maintenance and wait
                    KnownAssignedTransactionIdsIdleMaintenance();
                    XLogPrefetcherComputeStats(xlogprefetcher);

                    WaitLatch(&XLogRecoveryCtl->recoveryWakeupLatch,
                            WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
                            -1L, WAIT_EVENT_RECOVERY_WAL_STREAM);
                    ResetLatch(&XLogRecoveryCtl->recoveryWakeupLatch);
                    break;
                }
        }

        // Handle recovery pause
        if (((volatile XLogRecoveryCtlData *) XLogRecoveryCtl)->recoveryPauseState != RECOVERY_NOT_PAUSED)
            recoveryPausesHere(false);

        HandleStartupProcInterrupts();
    }

    return XLREAD_FAIL;
}
```