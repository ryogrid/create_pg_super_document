# XLogPageRead

## Location
[src/backend/access/transam/xlogrecovery.c:3298-3541](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L3298-L3541)

## Overview
XLogPageRead is a critical function that reads WAL pages from various sources (local pg_wal, archive, or streaming) during PostgreSQL recovery, managing source switching, error handling, and nonblocking operations.

## Definition

```c
static int
XLogPageRead(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr, int reqLen,
			 XLogRecPtr targetRecPtr, char *readBuf)
```
## Detailed Description
XLogPageRead serves as the primary page reading mechanism for WAL recovery operations. It abstracts the complexity of reading WAL pages from multiple sources and handles the intricate logic of source switching when pages are unavailable. The function is designed to work in both blocking and non-blocking modes, supporting WAL prefetching operations.

Key responsibilities include:
- Reading WAL pages from the appropriate source (local files, archive, streaming)
- Managing segment file opening/closing and source transitions
- Handling checkpoint requests when too much WAL has been replayed
- Providing non-blocking operation support for WAL prefetching
- Validating page headers and handling corruption gracefully
- Supporting retry logic in standby mode

The function implements sophisticated error handling that differentiates between temporary failures (requiring retry) and permanent failures (requiring recovery termination).

## Parameters / Member Variables
- : XLogReaderState containing the reader context and configuration
- : XLogRecPtr specifying the WAL page location to read
- : Integer indicating the minimum number of bytes required
- : XLogRecPtr of the target record being read (for error reporting)
- : Character buffer where the read page data will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [WaitForWALToBecomeAvailable](../W/WaitForWALToBecomeAvailable.md)
  - [XLogCheckpointNeeded](XLogCheckpointNeeded.md)
  - [GetRedoRecPtr](../G/GetRedoRecPtr.md)
  - [RequestCheckpoint](../R/RequestCheckpoint.md)
  - [emode_for_corrupt_record](../e/emode_for_corrupt_record.md)
  - [XLogReaderValidatePageHeader](XLogReaderValidatePageHeader.md)
  - [pg_pread](../p/pg_pread.md)
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md)/end
- Called from (representative examples):
  - [InitWalRecovery](../I/InitWalRecovery.md) (as page_read callback)
  - [XLogReaderState](XLogReaderState.md) callback mechanism

## Notes and Other Information
- Returns the number of bytes read on success, XLREAD_FAIL on permanent failure, or XLREAD_WOULDBLOCK for non-blocking operations
- Manages global variables readFile, readSegNo, readSource, readLen, and readOff to track current read state
- In standby mode, implements retry logic with source switching when pages are unavailable
- Page header validation occurs immediately to prevent issues with continuation records spanning different sources
- The function coordinates with the checkpoint system to request checkpoints when significant WAL has been consumed
- Non-blocking mode support enables efficient WAL prefetching without blocking the recovery process

## Simplified Source

```c
// Simplified version of XLogPageRead
static int
XLogPageRead(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr, int reqLen,
             XLogRecPtr targetRecPtr, char *readBuf)
{
    XLogPageReadPrivate *private = (XLogPageReadPrivate *) xlogreader->private_data;
    int emode = private->emode;
    uint32 targetPageOff;
    XLogSegNo targetSegNo;
    int r;

    // Calculate target segment and page offset
    XLByteToSeg(targetPagePtr, targetSegNo, wal_segment_size);
    targetPageOff = XLogSegmentOffset(targetPagePtr, wal_segment_size);

    // Check if we need to switch to a new segment
    if (readFile >= 0 && !XLByteInSeg(targetPagePtr, readSegNo, wal_segment_size)) {
        // Request checkpoint if needed in archive recovery
        if (ArchiveRecoveryRequested && IsUnderPostmaster) {
            if (XLogCheckpointNeeded(readSegNo)) {
                RequestCheckpoint(CHECKPOINT_CAUSE_XLOG);
            }
        }

        // Close current file and reset read state
        close(readFile);
        readFile = -1;
        readSource = XLOG_FROM_ANY;
    }

    XLByteToSeg(targetPagePtr, readSegNo, wal_segment_size);

retry:
    // Check if we need to retrieve more data
    if (readFile < 0 || (readSource == XLOG_FROM_STREAM && flushedUpto < targetPagePtr + reqLen)) {
        // Handle non-blocking mode for streaming
        if (readFile >= 0 && xlogreader->nonblocking &&
            readSource == XLOG_FROM_STREAM && flushedUpto < targetPagePtr + reqLen) {
            return XLREAD_WOULDBLOCK;
        }

        // Wait for WAL to become available from appropriate source
        switch (WaitForWALToBecomeAvailable(targetPagePtr + reqLen,
                                           private->randAccess,
                                           private->fetching_ckpt,
                                           targetRecPtr,
                                           private->replayTLI,
                                           xlogreader->EndRecPtr,
                                           xlogreader->nonblocking)) {
            case XLREAD_WOULDBLOCK:
                return XLREAD_WOULDBLOCK;
            case XLREAD_FAIL:
                // Clean up and return failure
                if (readFile >= 0) close(readFile);
                readFile = -1;
                readSource = XLOG_FROM_ANY;
                return XLREAD_FAIL;
            case XLREAD_SUCCESS:
                break;
        }
    }

    // Calculate how much data we can read
    if (readSource == XLOG_FROM_STREAM) {
        if ((targetPagePtr / XLOG_BLCKSZ) != (flushedUpto / XLOG_BLCKSZ)) {
            readLen = XLOG_BLCKSZ;
        } else {
            readLen = XLogSegmentOffset(flushedUpto, wal_segment_size) - targetPageOff;
        }
    } else {
        readLen = XLOG_BLCKSZ;
    }

    // Read the requested page
    readOff = targetPageOff;
    pgstat_report_wait_start(WAIT_EVENT_WAL_READ);
    r = pg_pread(readFile, readBuf, XLOG_BLCKSZ, (off_t) readOff);
    pgstat_report_wait_end();

    // Handle read errors
    if (r != XLOG_BLCKSZ) {
        char fname[MAXFNAMELEN];
        XLogFileName(fname, curFileTLI, readSegNo, wal_segment_size);

        if (r < 0) {
            ereport(emode_for_corrupt_record(emode, targetPagePtr + reqLen),
                    (errcode_for_file_access(),
                     errmsg("could not read from WAL segment %s: %m", fname)));
        } else {
            ereport(emode_for_corrupt_record(emode, targetPagePtr + reqLen),
                    (errcode(ERRCODE_DATA_CORRUPTED),
                     errmsg("short read from WAL segment %s: read %d of %d",
                            fname, r, XLOG_BLCKSZ)));
        }
        goto next_record_is_invalid;
    }

    // Set timeline info and validate page header in standby mode
    xlogreader->seg.ws_tli = curFileTLI;

    if (StandbyMode && (targetPagePtr % wal_segment_size) == 0 &&
        !XLogReaderValidatePageHeader(xlogreader, targetPagePtr, readBuf)) {
        // Report validation error and retry
        if (xlogreader->errormsg_buf[0]) {
            ereport(emode_for_corrupt_record(emode, xlogreader->EndRecPtr),
                    (errmsg_internal("%s", xlogreader->errormsg_buf)));
        }
        XLogReaderResetError(xlogreader);
        goto next_record_is_invalid;
    }

    return readLen;

next_record_is_invalid:
    // Handle invalid records - either retry in standby mode or fail
    if (xlogreader->nonblocking) {
        return XLREAD_WOULDBLOCK;
    }

    lastSourceFailed = true;

    // Clean up current read state
    if (readFile >= 0) close(readFile);
    readFile = -1;
    readLen = 0;
    readSource = XLOG_FROM_ANY;

    // Retry in standby mode, otherwise fail
    if (StandbyMode) {
        goto retry;
    } else {
        return XLREAD_FAIL;
    }
}
```

Key simplifications made:
- Removed detailed error message formatting and kept essential error reporting
- Simplified assertion statements and debug-only variables
- Consolidated similar error handling paths
- Abstracted complex segment calculation details while preserving core logic
- Removed extensive comments about edge cases, kept functional descriptions
- Simplified the page header validation logic while maintaining correctness
- Streamlined the retry mechanism flow