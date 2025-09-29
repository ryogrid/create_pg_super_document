# XLogFindNextRecord

## Location
[src/backend/access/transam/xlogreader.c:1393-1512](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogreader.c#L1393-L1512)

## Overview
XLogFindNextRecord locates the first valid XLOG record at or after a given LSN position, handling cases where the starting position may not align with record boundaries.

## Definition
```c
XLogRecPtr XLogFindNextRecord(XLogReaderState *state, XLogRecPtr RecPtr)
```

## Detailed Description
XLogFindNextRecord is a robust WAL record locator that differs from XLogBeginRead() by accepting starting positions that don't necessarily point to valid record boundaries. The function implements a sophisticated algorithm to handle various WAL layout complexities:

**Phase 1 - Skip Continuation Data:**
1. **Page Alignment**: Calculates the target page and record offset from the input LSN
2. **Page Reading**: Uses ReadPageInternal() to read WAL pages containing potential records
3. **Header Processing**: Examines page headers to determine header size and continuation status
4. **Continuation Handling**: When XLP_FIRST_IS_CONTRECORD flag is set:
   - Calculates whether continuation data spans multiple pages
   - Either skips to the next page or advances past continuation data within the current page
   - Uses MAXALIGN() to ensure proper alignment of record boundaries

**Phase 2 - Record Search:**
1. **Reader Positioning**: Calls XLogBeginRead() to position the reader at the calculated starting point
2. **Sequential Reading**: Uses XLogReadRecord() to iterate through records until finding one at or after the target LSN
3. **Position Management**: When the target is found, rewinds the reader to the beginning of that record

This function is particularly valuable for debugging tools and recovery scenarios where precise record boundary information may not be available.

## Parameters / Member Variables
- `state`: XLogReaderState pointer to the reader state that will be positioned for subsequent reads
- `RecPtr`: XLogRecPtr specifying the minimum LSN to search from (doesn't need to be record-aligned)

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecPtrIsInvalid (LSN validation function)
  - [ReadPageInternal](../R/ReadPageInternal.md) (internal page reading function)
  - XLogPageHeader, XLogPageHeaderSize (page header handling)
  - XLP_FIRST_IS_CONTRECORD (page flag for continuation records)
  - [XLogBeginRead](XLogBeginRead.md) (reader positioning function)
  - [XLogReadRecord](XLogReadRecord.md) (record reading function)
  - [XLogReaderInvalReadState](XLogReaderInvalReadState.md) (error state cleanup function)
  - MAXALIGN (alignment macro)
- Called from (representative examples):
  - [SummarizeWAL](../S/SummarizeWAL.md) (at line 970 in walsummarizer.c)
  - [main](../m/main.md) (at line 1214 in pg_waldump.c)
  - [XLogReaderHasQueuedRecordOrError](XLogReaderHasQueuedRecordOrError.md) (header function at line 346)

## Notes and Other Information
- Returns the LSN of the first valid record at or after RecPtr, or InvalidXLogRecPtr on failure
- Positions the reader state so that subsequent XLogReadRecord() calls will read from the found position
- Handles complex WAL layout scenarios including multi-page continuation records
- Sets state->nonblocking to false to ensure ReadPageInternal() doesn't return XLREAD_WOULDBLOCK
- Critical for WAL debugging tools like pg_waldump that need to start reading from arbitrary positions
- More flexible than XLogBeginRead() but with higher computational overhead due to searching
- Properly handles page boundaries and record alignment requirements

## Simplified Source

```c
XLogRecPtr
XLogFindNextRecord(XLogReaderState *state, XLogRecPtr RecPtr)
{
    XLogRecPtr tmpRecPtr;
    XLogRecPtr found = InvalidXLogRecPtr;
    XLogPageHeader header;
    char *errormsg;

    // Ensure nonblocking reads don't interfere
    state->nonblocking = false;

    // Phase 1: Skip over any continuation data spanning multiple pages
    tmpRecPtr = RecPtr;
    while (true) {
        // Calculate page boundary and record offset
        uint32 targetRecOff = tmpRecPtr % XLOG_BLCKSZ;
        XLogRecPtr targetPagePtr = tmpRecPtr - targetRecOff;

        // Read the page containing potential record
        int readLen = ReadPageInternal(state, targetPagePtr, targetRecOff);
        if (readLen < 0)
            goto err;

        header = (XLogPageHeader) state->readBuf;
        uint32 pageHeaderSize = XLogPageHeaderSize(header);

        // Ensure we have full page header
        readLen = ReadPageInternal(state, targetPagePtr, pageHeaderSize);
        if (readLen < 0)
            goto err;

        // Handle continuation records
        if (header->xlp_info & XLP_FIRST_IS_CONTRECORD) {
            // Check if continuation spans to next page
            if (MAXALIGN(header->xlp_rem_len) >= (XLOG_BLCKSZ - pageHeaderSize)) {
                // Continuation continues on next page
                tmpRecPtr = targetPagePtr + XLOG_BLCKSZ;
            } else {
                // Continuation ends on this page - skip past it
                tmpRecPtr = targetPagePtr + pageHeaderSize + MAXALIGN(header->xlp_rem_len);
                break;
            }
        } else {
            // No continuation - start of page after header
            tmpRecPtr = targetPagePtr + pageHeaderSize;
            break;
        }
    }

    // Phase 2: Search for record at or after target LSN
    XLogBeginRead(state, tmpRecPtr);
    while (XLogReadRecord(state, &errormsg) != NULL) {
        // Found a record at or past our target
        if (RecPtr <= state->ReadRecPtr) {
            found = state->ReadRecPtr;
            XLogBeginRead(state, found);  // Rewind to start of found record
            return found;
        }
    }

err:
    XLogReaderInvalReadState(state);
    return InvalidXLogRecPtr;
}
```