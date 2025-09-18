# XLogFindNextRecord

## Location
src/backend/access/transam/xlogreader.c: 1393 - 1512

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
  - ReadPageInternal (internal page reading function)
  - XLogPageHeader, XLogPageHeaderSize (page header handling)
  - XLP_FIRST_IS_CONTRECORD (page flag for continuation records)
  - XLogBeginRead (reader positioning function)
  - XLogReadRecord (record reading function)
  - XLogReaderInvalReadState (error state cleanup function)
  - MAXALIGN (alignment macro)
- Called from (representative examples):
  - SummarizeWAL (at line 970 in walsummarizer.c)
  - main (at line 1214 in pg_waldump.c)
  - XLogReaderHasQueuedRecordOrError (header function at line 346)

## Notes and Other Information
- Returns the LSN of the first valid record at or after RecPtr, or InvalidXLogRecPtr on failure
- Positions the reader state so that subsequent XLogReadRecord() calls will read from the found position
- Handles complex WAL layout scenarios including multi-page continuation records
- Sets state->nonblocking to false to ensure ReadPageInternal() doesn't return XLREAD_WOULDBLOCK
- Critical for WAL debugging tools like pg_waldump that need to start reading from arbitrary positions
- More flexible than XLogBeginRead() but with higher computational overhead due to searching
- Properly handles page boundaries and record alignment requirements