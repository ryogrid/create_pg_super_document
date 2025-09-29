# XLogDecodeNextRecord

## Location
[src/backend/access/transam/xlogreader.c:528-975](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogreader.c#L528-L975)

## Overview
Decodes and validates the next WAL record from the input stream, handling both single-page and multi-page records with comprehensive error checking and recovery mechanisms.

## Definition
```c
static XLogPageReadResult XLogDecodeNextRecord(XLogReaderState *state, bool nonblocking)
```

## Detailed Description
This is the core function for reading and decoding WAL records. It handles the complex logic of:

1. **Sequential vs Random Access**: Determines whether to verify the previous-record pointer based on read pattern
2. **Page Boundary Handling**: Manages records that span multiple WAL pages, including proper header validation
3. **Record Assembly**: Reconstructs records split across pages by following continuation record markers
4. **Memory Management**: Allocates decode buffer space through XLogReadRecordAlloc with fallback strategies
5. **Validation**: Performs comprehensive header and record validation at multiple stages
6. **Special Record Processing**: Handles XLOG_SWITCH records that affect segment boundaries
7. **Error Recovery**: Detects and handles overwritten continuation records and other corruption scenarios

The function implements a state machine that can restart record reading when encountering overwritten continuation records, and maintains detailed error state for diagnostic purposes.

## Parameters / Member Variables
- `state`: XLogReaderState containing all reader context, buffers, and position information
- `nonblocking`: Boolean flag indicating whether the function should block waiting for data or return immediately if data is not available

## Dependencies
- Functions called/Symbols referenced:
  - [ReadPageInternal](../R/ReadPageInternal.md)
  - XLogPageHeaderSize
  - [ValidXLogRecordHeader](../V/ValidXLogRecordHeader.md)
  - [ValidXLogRecord](../V/ValidXLogRecord.md)
  - [XLogReadRecordAlloc](XLogReadRecordAlloc.md)
  - DecodeXLogRecord
  - [XLogReaderInvalReadState](XLogReaderInvalReadState.md)
  - [report_invalid_record](../r/report_invalid_record.md)
  - [allocate_recordbuf](../a/allocate_recordbuf.md)
- Called from (representative examples):
  - [XLogReadAhead](XLogReadAhead.md)

## Notes and Other Information
- Returns XLREAD_SUCCESS on successful decode, XLREAD_WOULDBLOCK for nonblocking reads without data, or XLREAD_FAIL on errors
- Maintains decode queue for successfully decoded records
- Handles continuation records across page boundaries with xlp_rem_len validation
- Sets abortedRecPtr and missingContrecPtr for recovery when multi-page record assembly fails
- Special handling for XLOG_SWITCH records that extend to segment boundaries
- Uses randAccess flag to control previous-record pointer validation during sequential reads
- Implements circular buffer management through decode_buffer_tail updates

## Simplified Source

```c
static XLogPageReadResult
XLogDecodeNextRecord(XLogReaderState *state, bool nonblocking)
{
    XLogRecPtr RecPtr;
    XLogRecord *record;
    uint32 total_len;
    bool randAccess = false;
    bool assembled = false;
    DecodedXLogRecord *decoded = NULL;

    // Clear error state and set starting position
    state->errormsg_buf[0] = '\0';
    state->abortedRecPtr = InvalidXLogRecPtr;
    state->missingContrecPtr = InvalidXLogRecPtr;
    RecPtr = state->NextRecPtr;

    // Determine access pattern - random access for caller-supplied positions
    if (state->DecodeRecPtr == InvalidXLogRecPtr) {
        randAccess = true;
    }

restart:
    state->nonblocking = nonblocking;
    state->currRecPtr = RecPtr;

    // Calculate page and record offset
    XLogRecPtr targetPagePtr = RecPtr - (RecPtr % XLOG_BLCKSZ);
    uint32 targetRecOff = RecPtr % XLOG_BLCKSZ;

    // Read the page containing record header
    int readOff = ReadPageInternal(state, targetPagePtr,
                                   Min(targetRecOff + SizeOfXLogRecord, XLOG_BLCKSZ));
    if (readOff == XLREAD_WOULDBLOCK)
        return XLREAD_WOULDBLOCK;
    else if (readOff < 0)
        goto err;

    // Handle page header and validate record position
    uint32 pageHeaderSize = XLogPageHeaderSize((XLogPageHeader) state->readBuf);
    if (targetRecOff == 0) {
        RecPtr += pageHeaderSize;
        targetRecOff = pageHeaderSize;
    }

    // Read record length and validate header if on same page
    record = (XLogRecord *) (state->readBuf + RecPtr % XLOG_BLCKSZ);
    total_len = record->xl_tot_len;

    bool gotheader = false;
    if (targetRecOff <= XLOG_BLCKSZ - SizeOfXLogRecord) {
        if (!ValidXLogRecordHeader(state, RecPtr, state->DecodeRecPtr, record, randAccess))
            goto err;
        gotheader = true;
    }

    // Try to allocate decode space
    decoded = XLogReadRecordAlloc(state, total_len, false);
    if (decoded == NULL && nonblocking)
        return XLREAD_WOULDBLOCK;

    uint32 len = XLOG_BLCKSZ - RecPtr % XLOG_BLCKSZ;
    if (total_len > len) {
        // Multi-page record - reassemble from continuation pages
        assembled = true;
        memcpy(state->readRecordBuf, state->readBuf + RecPtr % XLOG_BLCKSZ, len);

        char *buffer = state->readRecordBuf + len;
        uint32 gotlen = len;

        // Read continuation pages
        do {
            targetPagePtr += XLOG_BLCKSZ;
            readOff = ReadPageInternal(state, targetPagePtr, SizeOfXLogShortPHD);
            if (readOff == XLREAD_WOULDBLOCK)
                return XLREAD_WOULDBLOCK;
            else if (readOff < 0)
                goto err;

            XLogPageHeader pageHeader = (XLogPageHeader) state->readBuf;

            // Handle overwritten continuation records
            if (pageHeader->xlp_info & XLP_FIRST_IS_OVERWRITE_CONTRECORD) {
                state->overwrittenRecPtr = RecPtr;
                RecPtr = targetPagePtr;
                goto restart;
            }

            // Validate continuation record markers
            if (!(pageHeader->xlp_info & XLP_FIRST_IS_CONTRECORD))
                goto err;

            // Read continuation data
            readOff = ReadPageInternal(state, targetPagePtr,
                                       Min(total_len - gotlen + SizeOfXLogShortPHD, XLOG_BLCKSZ));
            if (readOff == XLREAD_WOULDBLOCK)
                return XLREAD_WOULDBLOCK;

            pageHeaderSize = XLogPageHeaderSize(pageHeader);
            char *contdata = (char *) state->readBuf + pageHeaderSize;
            len = Min(pageHeader->xlp_rem_len, XLOG_BLCKSZ - pageHeaderSize);

            memcpy(buffer, contdata, len);
            buffer += len;
            gotlen += len;

            // Validate header once assembled
            if (!gotheader && gotlen >= SizeOfXLogRecord) {
                record = (XLogRecord *) state->readRecordBuf;
                if (!ValidXLogRecordHeader(state, RecPtr, state->DecodeRecPtr, record, randAccess))
                    goto err;
                gotheader = true;
            }

        } while (gotlen < total_len);

        record = (XLogRecord *) state->readRecordBuf;
        if (!ValidXLogRecord(state, record, RecPtr))
            goto err;

        state->NextRecPtr = targetPagePtr + pageHeaderSize + MAXALIGN(pageHeader->xlp_rem_len);
    } else {
        // Single page record
        if (!ValidXLogRecord(state, record, RecPtr))
            goto err;
        state->NextRecPtr = RecPtr + MAXALIGN(total_len);
    }

    state->DecodeRecPtr = RecPtr;

    // Handle XLOG_SWITCH records - extend to segment boundary
    if (record->xl_rmid == RM_XLOG_ID &&
        (record->xl_info & ~XLR_INFO_MASK) == XLOG_SWITCH) {
        state->NextRecPtr += state->segcxt.ws_segsize - 1;
        state->NextRecPtr -= XLogSegmentOffset(state->NextRecPtr, state->segcxt.ws_segsize);
    }

    // Allocate final decode buffer if needed
    if (decoded == NULL) {
        decoded = XLogReadRecordAlloc(state, total_len, true);
    }

    // Decode the record and add to queue
    char *errormsg;
    if (DecodeXLogRecord(state, decoded, record, RecPtr, &errormsg)) {
        decoded->next_lsn = state->NextRecPtr;

        // Update decode buffer management
        if (!decoded->oversized) {
            if ((char *) decoded == state->decode_buffer)
                state->decode_buffer_tail = state->decode_buffer + decoded->size;
            else
                state->decode_buffer_tail += decoded->size;
        }

        // Add to decode queue
        if (state->decode_queue_tail)
            state->decode_queue_tail->next = decoded;
        state->decode_queue_tail = decoded;
        if (!state->decode_queue_head)
            state->decode_queue_head = decoded;

        return XLREAD_SUCCESS;
    }

err:
    // Handle assembly errors for multi-page records
    if (assembled) {
        state->abortedRecPtr = RecPtr;
        state->missingContrecPtr = targetPagePtr;
        state->errormsg_deferred = true;
    }

    if (decoded && decoded->oversized)
        pfree(decoded);

    XLogReaderInvalReadState(state);
    return XLREAD_FAIL;
}
```