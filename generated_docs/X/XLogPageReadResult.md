# XLogPageReadResult

## Location
[src/include/access/xlogreader.h:354-381](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xlogreader.h#L354-L381)

## Overview
XLogPageReadResult is an enumeration that defines the return values for XLogPageReadCB callback functions, indicating the success, failure, or blocking status of WAL (Write-Ahead Logging) page read operations.

## Definition

```c
struct XLogRecord *XLogReadRecord(XLogReaderState *state,
										 char **errormsg);
```
## Detailed Description
XLogPageReadResult serves as a standardized return type for XLogPageReadCB functions that handle reading WAL pages. This enum provides three distinct outcomes for page read operations, supporting both blocking and non-blocking read modes. The enum values use negative numbers for error conditions (following Unix convention) while success is represented by zero.

This enumeration is central to PostgreSQL's WAL reading infrastructure, allowing the system to handle various scenarios that can occur during WAL page retrieval, including I/O failures, missing data, and non-blocking operations where data is not immediately available.

## Parameters / Member Variables
-  (0): Indicates the WAL record/page was successfully read and is available for processing
-  (-1): Indicates a failure occurred during the read operation, such as I/O errors, corrupted data, or missing WAL files
-  (-2): Used exclusively in nonblocking mode to indicate that the requested data is not currently available and the operation would need to block to wait for it

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is an enum definition)
- Called from (representative examples):
  - [XLogReadRecordAlloc](XLogReadRecordAlloc.md) (src/backend/access/transam/xlogreader.c:527)
  - [XLogReadAhead](XLogReadAhead.md) (src/backend/access/transam/xlogreader.c:978) 
  - [XLogPageRead](XLogPageRead.md) (src/backend/access/transam/xlogrecovery.c:3541)
  - [WaitForWALToBecomeAvailable](../W/WaitForWALToBecomeAvailable.md) (referenced in XLogPageRead implementation)

## Notes and Other Information
- This enum is specifically designed for use with XLogPageReadCB callback functions (defined at src/include/access/xlogreader.h:62)
- The XLREAD_WOULDBLOCK value is only meaningful when xlogreader->nonblocking is set to true
- Used extensively in WAL recovery operations, streaming replication, and archive recovery scenarios
- The enum values follow PostgreSQL's error handling conventions where zero indicates success and negative values indicate different types of failures
- Functions returning this enum type should handle all three cases appropriately, with XLREAD_WOULDBLOCK requiring special consideration for non-blocking operation workflows