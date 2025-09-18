# XLogDecodeNextRecord

## Location
src/backend/access/transam/xlogreader.c: 528 - 975

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