# XLogReaderValidatePageHeader

## Location
src/backend/access/transam/xlogreader.c: 1234 - 1374

## Overview
XLogReaderValidatePageHeader validates WAL page headers to ensure structural integrity and consistency before processing page contents.

## Definition
```c
bool XLogReaderValidatePageHeader(XLogReaderState *state, XLogRecPtr recptr, char *phdr)
```

## Detailed Description
XLogReaderValidatePageHeader performs comprehensive validation of WAL page headers to ensure data integrity and system consistency. The function executes multiple validation checks:

1. **Magic Number Validation**: Verifies the page header contains the correct XLOG_PAGE_MAGIC value
2. **Info Bits Validation**: Ensures only valid flag bits are set in xlp_info field
3. **Long Header Processing**: For pages with XLP_LONG_HEADER flag:
   - Validates system identifier matches the expected database system
   - Verifies segment size consistency
   - Checks XLOG_BLCKSZ consistency
4. **First Page Validation**: Ensures the first page of a WAL segment has a long header
5. **Page Address Validation**: Confirms the page address matches the expected location (detects recycled segments)
6. **Timeline Validation**: Ensures timeline IDs don't go backwards across successive pages

The function also updates the reader state by tracking the latest validated page pointer and timeline ID for future consistency checks.

## Parameters / Member Variables
- `state`: XLogReaderState pointer containing reader state, system configuration, and error reporting context
- `recptr`: XLogRecPtr specifying the expected page location in the WAL (must be page-aligned)
- `phdr`: Character pointer to the page header data to be validated

## Dependencies
- Functions called/Symbols referenced:
  - XLogSegNo, XLogPageHeader, XLogLongPageHeader (WAL data types)
  - XLByteToSeg (converts LSN to segment number)
  - XLogSegmentOffset (calculates offset within segment)
  - XLogFileName (generates WAL filename for error reporting)
  - XLOG_PAGE_MAGIC (expected magic number constant)
  - XLP_ALL_FLAGS, XLP_LONG_HEADER (page header flag constants)
  - MAXFNAMELEN (maximum filename length constant)
  - report_invalid_record (error reporting function)
- Called from (representative examples):
  - ReadPageInternal (at lines 1061 and 1103)
  - XLogPageRead (at line 3467 in xlogrecovery.c)
  - XLogPageReadResult (header function)

## Notes and Other Information
- Public function (non-static) available for use by other WAL processing components
- Assumes recptr is page-aligned (asserted with XLOG_BLCKSZ alignment check)
- Critical for detecting recycled WAL segments that haven't been overwritten yet
- Timeline validation prevents reading inconsistent WAL sequences
- Updates state->latestPagePtr and state->latestPageTLI to track progression
- Long headers are mandatory on the first page of each WAL segment
- Returns boolean indicating validation success/failure with detailed error reporting