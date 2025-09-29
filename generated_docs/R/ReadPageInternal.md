# ReadPageInternal

## Location
[src/backend/access/transam/xlogreader.c:1010-1122](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogreader.c#L1010-L1122)

## Overview
Reads a single WAL page through the page_read callback with caching, validation, and comprehensive error handling for both blocking and non-blocking scenarios.

## Definition
```c
static int ReadPageInternal(XLogReaderState *state, XLogRecPtr pageptr, int reqLen)
```

## Detailed Description
This function is the core page reading mechanism for the WAL reader subsystem. It manages a reader-local cache to avoid redundant reads and implements sophisticated logic for:

1. **Cache Management**: Checks if requested data is already available in the buffer before attempting new reads
2. **Segment Validation**: When switching to a new WAL segment, always validates the first page header even if not the target
3. **Progressive Reading**: Reads minimum required data first, then expands as needed for header validation
4. **Header Validation**: Performs comprehensive page header validation through XLogReaderValidatePageHeader
5. **State Management**: Updates read state information and handles cache invalidation on errors

The function ensures data integrity by re-validating pages even if previously read, since the page_read callback might be reading from different sources.

## Parameters / Member Variables
- `state`: XLogReaderState containing read buffers, segment context, and callback routines
- `pageptr`: XLogRecPtr pointing to the beginning of the page to read (must be XLOG_BLCKSZ aligned)
- `reqLen`: Minimum number of bytes required from the page

## Dependencies
- Functions called/Symbols referenced:
  - XLByteToSeg
  - XLogSegmentOffset
  - [XLogReaderValidatePageHeader](../X/XLogReaderValidatePageHeader.md)
  - XLogPageHeaderSize
  - [XLogReaderInvalReadState](../X/XLogReaderInvalReadState.md)
  - [state](../s/state.md)->routine.page_read (callback)
- Called from (representative examples):
  - [XLogDecodeNextRecord](../X/XLogDecodeNextRecord.md) (multiple times)
  - [XLogFindNextRecord](../X/XLogFindNextRecord.md)

## Notes and Other Information
- Returns actual bytes read on success, XLREAD_WOULDBLOCK for non-blocking reads without data, or XLREAD_FAIL on errors
- Maintains internal cache keyed by segment number and page offset
- Always reads at least SizeOfXLogShortPHD bytes for header validation
- Invalidates cache on any error to ensure clean state
- Supports non-blocking operation when page_read callback respects nonblocking flag
- Special handling for segment boundary crossings with first-page header validation
- pageptr parameter must be aligned to XLOG_BLCKSZ boundaries

## Simplified Source

```c
static int ReadPageInternal(XLogReaderState *state, XLogRecPtr pageptr, int reqLen) {
    int readLen;
    uint32 targetPageOff;
    XLogSegNo targetSegNo;
    XLogPageHeader hdr;

    Assert((pageptr % XLOG_BLCKSZ) == 0);

    // Get segment number and offset
    XLByteToSeg(pageptr, targetSegNo, state->segcxt.ws_segsize);
    targetPageOff = XLogSegmentOffset(pageptr, state->segcxt.ws_segsize);

    // Check if we already have the requested data in cache
    if (targetSegNo == state->seg.ws_segno &&
        targetPageOff == state->segoff &&
        reqLen <= state->readLen)
        return state->readLen;

    // Invalidate cache before new read
    state->readLen = 0;

    // For new segments, validate first page header
    if (targetSegNo != state->seg.ws_segno && targetPageOff != 0) {
        XLogRecPtr targetSegmentPtr = pageptr - targetPageOff;
        readLen = state->routine.page_read(state, targetSegmentPtr, XLOG_BLCKSZ,
                                          state->currRecPtr, state->readBuf);
        if (readLen == XLREAD_WOULDBLOCK || readLen < 0)
            goto err;

        if (!XLogReaderValidatePageHeader(state, targetSegmentPtr, state->readBuf))
            goto err;
    }

    // Read requested page data
    readLen = state->routine.page_read(state, pageptr, Max(reqLen, SizeOfXLogShortPHD),
                                      state->currRecPtr, state->readBuf);
    if (readLen == XLREAD_WOULDBLOCK || readLen < 0)
        goto err;

    // Validate we have enough data for header
    if (readLen <= SizeOfXLogShortPHD)
        goto err;

    hdr = (XLogPageHeader) state->readBuf;

    // Read full header if needed
    if (readLen < XLogPageHeaderSize(hdr)) {
        readLen = state->routine.page_read(state, pageptr, XLogPageHeaderSize(hdr),
                                          state->currRecPtr, state->readBuf);
        if (readLen == XLREAD_WOULDBLOCK || readLen < 0)
            goto err;
    }

    // Validate complete header
    if (!XLogReaderValidatePageHeader(state, pageptr, (char *) hdr))
        goto err;

    // Update read state
    state->seg.ws_segno = targetSegNo;
    state->segoff = targetPageOff;
    state->readLen = readLen;

    return readLen;

err:
    XLogReaderInvalReadState(state);
    return XLREAD_FAIL;
}
```