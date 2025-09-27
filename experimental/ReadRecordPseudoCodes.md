# ReadRecord Function Simplified Pseudo-Code

## Main Function

```c
/* Function Prototype */
static XLogRecord *ReadRecord(XLogPrefetcher *xlogprefetcher, int emode,
                              bool fetching_ckpt, TimeLineID replayTLI);

/* Simplified Code */
ReadRecord()
{
    // Setup page reader parameters
    SetupPageReadParameters(fetching_ckpt, emode, replayTLI);

    for (;;)
    {
        // Try to read next record using prefetcher
        record = XLogPrefetcherReadRecord(xlogprefetcher);

        if (record == NULL)
        {
            // Track incomplete records for later processing
            if (!ArchiveRecoveryRequested)
                TrackIncompleteRecord();

            // Close open file if any
            CloseReadFile();

            // Report error if any
            ReportCorruptRecordError(emode);
        }
        else if (!ValidateTimelineID(record))
        {
            // Timeline ID not in expected history
            ReportUnexpectedTimelineError();
            record = NULL;
        }

        if (record)
            return record;  // Success

        // Handle recovery mode transitions
        if (!InArchiveRecovery && ArchiveRecoveryRequested && !fetching_ckpt)
        {
            SwitchToArchiveRecovery();
            continue;  // Retry with archive
        }

        // In standby mode, keep retrying unless triggered
        if (StandbyMode && !CheckForStandbyTrigger())
            continue;
        else
            return NULL;  // End of WAL or standby triggered
    }
}
```

## XLogPrefetcherReadRecord

```c
/* Function Prototype */
XLogRecord *XLogPrefetcherReadRecord(XLogPrefetcher *prefetcher, char **errmsg);

/* Simplified Code */
XLogPrefetcherReadRecord()
{
    // Reconfigure prefetcher if settings changed
    if (SettingsChanged())
        ReconfigurePrefetcher();

    // Release previous record
    XLogReleasePreviousRecord();

    // Update filters for completed operations
    UpdatePrefetchFilters();

    // Complete IO operations up to replayed position
    CompleteIOOperations(replayed_position);

    // Start prefetching if nothing queued yet
    if (!HasQueuedRecords())
        StartPrefetching();

    // Get next record from queue
    record = XLogNextRecord();

    if (record == NULL)
        return NULL;

    // Update statistics if needed
    if (TimeToUpdateStats())
        ComputeStatistics();

    return record;
}
```

## XLogNextRecord

```c
/* Function Prototype */
DecodedXLogRecord *XLogNextRecord(XLogReaderState *state, char **errormsg);

/* Simplified Code */
XLogNextRecord()
{
    // Release previous record
    XLogReleasePreviousRecord();

    // Check if there's a record in the queue
    if (decode_queue_head == NULL)
    {
        // Return any deferred error message
        if (errormsg_deferred)
            *errormsg = errormsg_buf;
        return NULL;
    }

    // Get record from queue head
    record = decode_queue_head;

    // Update reader state for compatibility
    UpdateReaderPointers(record);

    return record;
}
```

## XLogReadAhead

```c
/* Function Prototype */
DecodedXLogRecord *XLogReadAhead(XLogReaderState *state, bool nonblocking);

/* Simplified Code */
XLogReadAhead()
{
    // Don't read ahead if there's a deferred error
    if (errormsg_deferred)
        return NULL;

    // Try to decode next record
    result = XLogDecodeNextRecord(nonblocking);

    if (result == XLREAD_SUCCESS)
        return decode_queue_tail;
    else
        return NULL;
}
```

## XLogDecodeNextRecord

```c
/* Function Prototype */
static XLogPageReadResult XLogDecodeNextRecord(XLogReaderState *state, bool nonblocking);

/* Simplified Code */
XLogDecodeNextRecord()
{
restart:
    // Initialize for reading
    targetPagePtr = AlignToPageBoundary(RecPtr);
    targetRecOff = PageOffset(RecPtr);

    // Read page containing record start
    readResult = ReadPageInternal(targetPagePtr, targetRecOff + HeaderSize);
    if (readResult == WOULDBLOCK)
        return XLREAD_WOULDBLOCK;
    if (readResult < 0)
        goto error;

    // Skip page header if at page start
    if (targetRecOff == 0)
        RecPtr += PageHeaderSize;

    // Read record header
    record = GetRecordFromBuffer(RecPtr);
    total_len = record->xl_tot_len;

    // Validate header
    if (!ValidateRecordHeader(record))
        goto error;

    // Allocate space for decoded record
    decoded = AllocateDecodeSpace(total_len);
    if (decoded == NULL && nonblocking)
        return XLREAD_WOULDBLOCK;

    // Check if record spans pages
    if (RecordSpansPages(total_len, RecPtr))
    {
        // Assemble multi-page record
        for (each continuation page)
        {
            // Read next page header
            readResult = ReadPageInternal(nextPage);
            if (readResult == WOULDBLOCK)
                return XLREAD_WOULDBLOCK;

            // Check for overwrite contrecord (restart if found)
            if (PageHasOverwriteFlag())
            {
                RecPtr = nextPage;
                goto restart;
            }

            // Verify continuation record
            if (!ValidateContinuation())
                goto error;

            // Copy continuation data
            CopyDataToBuffer();
        }
    }
    else
    {
        // Record fits in single page - just read it
        readResult = ReadPageInternal(targetPagePtr, targetRecOff + total_len);
        if (readResult == WOULDBLOCK)
            return XLREAD_WOULDBLOCK;
    }

    // Validate complete record
    if (!ValidateCompleteRecord(record))
        goto error;

    // Handle special XLOG SWITCH records
    if (IsXLogSwitch(record))
        AlignNextRecPtrToSegmentBoundary();

    // Decode the record
    if (!DecodeXLogRecord(decoded, record))
        goto error;

    // Add to decode queue
    AddToDecodeQueue(decoded);
    return XLREAD_SUCCESS;

error:
    // Track aborted records for later processing
    if (assembled)
        TrackAbortedRecord(RecPtr);

    InvalidateReadState();
    return XLREAD_FAIL;
}
```

## ReadPageInternal

```c
/* Function Prototype */
static int ReadPageInternal(XLogReaderState *state, XLogRecPtr pageptr, int reqLen);

/* Simplified Code */
ReadPageInternal()
{
    // Check if page is already in cache
    if (PageInCache(pageptr))
    {
        if (EnoughDataInCache(reqLen))
            return cached_length;
    }

    // Need to read from source
    for (;;)
    {
        // Call page_read callback (XLogPageRead)
        readLen = page_read(pageptr, reqLen);

        if (readLen == WOULDBLOCK)
            return XLREAD_WOULDBLOCK;

        if (readLen < 0)
            return XLREAD_FAIL;

        // Validate page header
        if (!ValidatePageHeader())
            continue;  // Try again

        // Cache the page
        CachePage(pageptr, readLen);

        return readLen;
    }
}
```

## XLogPageRead

```c
/* Function Prototype */
int XLogPageRead(XLogReaderState *state, XLogRecPtr targetPagePtr,
                 int reqLen, char *readBuf);

/* Simplified Code */
XLogPageRead()
{
    // Determine which source to read from
    for (;;)
    {
        // Try sources in order of preference
        switch (currentSource)
        {
            case XLOG_FROM_STREAM:
                // Try to read from streaming replication
                result = ReadFromStream(targetPagePtr);
                if (result >= 0)
                    return result;
                if (nonblocking)
                    return WOULDBLOCK;
                // Fall through to try next source

            case XLOG_FROM_ARCHIVE:
                // Try to read from archive
                result = ReadFromArchive(targetPagePtr);
                if (result >= 0)
                    return result;
                // Fall through to try next source

            case XLOG_FROM_PG_WAL:
                // Try to read from local pg_wal
                result = ReadFromPgWal(targetPagePtr);
                if (result >= 0)
                    return result;
                break;
        }

        // All sources failed
        if (StandbyMode)
        {
            // Wait and retry in standby mode
            WaitForWAL();
            continue;
        }
        else
        {
            // No more WAL available
            return -1;
        }
    }
}
```

## Helper Functions

### CheckForStandbyTrigger

```c
/* Function Prototype */
static bool CheckForStandbyTrigger(void);

/* Simplified Code */
CheckForStandbyTrigger()
{
    // Check if promotion has been requested
    if (PromotionSignalReceived())
        return true;

    // Check for trigger file
    if (TriggerFileExists())
        return true;

    return false;
}
```

### SwitchIntoArchiveRecovery

```c
/* Function Prototype */
static void SwitchIntoArchiveRecovery(XLogRecPtr EndRecPtr, TimeLineID replayTLI);

/* Simplified Code */
SwitchIntoArchiveRecovery()
{
    // Update control file state
    UpdateControlFile(IN_ARCHIVE_RECOVERY);

    // Initialize archive recovery parameters
    InitializeArchiveRecovery();

    // Enable standby mode if requested
    if (StandbyModeRequested)
        EnableStandbyMode();

    // Update recovery target info
    minRecoveryPoint = EndRecPtr;
    minRecoveryPointTLI = replayTLI;

    // Check if we're now consistent
    CheckRecoveryConsistency();
}
```

### XLogReleasePreviousRecord

```c
/* Function Prototype */
XLogRecPtr XLogReleasePreviousRecord(XLogReaderState *state);

/* Simplified Code */
XLogReleasePreviousRecord()
{
    if (current_record == NULL)
        return InvalidXLogRecPtr;

    // Get LSN of record being released
    released_lsn = current_record->lsn;

    // Remove from decode queue
    RemoveFromDecodeQueue(current_record);

    // Free memory if oversized
    if (current_record->oversized)
        FreeRecord(current_record);

    current_record = NULL;
    return released_lsn;
}
```

### DecodeXLogRecord

```c
/* Function Prototype */
static bool DecodeXLogRecord(XLogReaderState *state, DecodedXLogRecord *decoded,
                             XLogRecord *record, XLogRecPtr lsn, char **errormsg);

/* Simplified Code */
DecodeXLogRecord()
{
    // Copy record header
    decoded->header = *record;
    decoded->lsn = lsn;

    // Parse record data
    ptr = record_data;

    // Extract block data
    for (each block reference)
    {
        ParseBlockReference(ptr, &block_info);
        decoded->blocks[i] = block_info;
        ptr += block_size;
    }

    // Extract main data
    decoded->main_data = ptr;
    decoded->main_data_len = remaining_len;

    // Validate CRC if enabled
    if (ValidateCRC && !CheckRecordCRC(record))
        return false;

    return true;
}
```

## Key Data Flow

1. **ReadRecord** - Main entry point, orchestrates reading
2. **XLogPrefetcherReadRecord** - Adds prefetching layer
3. **XLogNextRecord** - Returns records from decode queue
4. **XLogReadAhead** - Reads ahead to fill decode queue
5. **XLogDecodeNextRecord** - Core decoding logic
6. **ReadPageInternal** - Page-level reading with caching
7. **XLogPageRead** - Source selection and actual I/O

The flow implements a multi-layered approach:
- **Prefetching layer** - Reads ahead for performance
- **Decoding layer** - Validates and decodes records
- **Page reading layer** - Handles page-level I/O
- **Source management layer** - Switches between pg_wal, archive, and streaming

Key features:
- Non-blocking operation support for streaming replication
- Multi-page record assembly
- Source failover (stream → archive → pg_wal)
- Standby mode with continuous retry
- Archive recovery mode transition
- Incomplete record tracking for crash recovery