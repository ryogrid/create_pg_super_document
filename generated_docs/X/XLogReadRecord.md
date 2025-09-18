# XLogReadRecord

## Location
[src/backend/access/transam/xlogreader.c:389-437](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogreader.c#L389-L437)

## Overview
Main interface function for reading WAL records, providing a blocking read operation that ensures a record is available before returning.

## Definition
```c
XLogRecord *XLogReadRecord(XLogReaderState *state, char **errormsg)
```

## Detailed Description
XLogReadRecord serves as the primary synchronous interface for reading WAL records. Unlike XLogNextRecord(), which only returns immediately available records from the queue, XLogReadRecord ensures that a record is available by calling XLogReadAhead() in blocking mode if the queue is empty.

The function manages the complete lifecycle of record retrieval: it releases the previous record, ensures new data is available through read-ahead operations, and returns the record header for compatibility with legacy PostgreSQL code. The actual decoded record data is accessible through XLogRecGetXXX() macros that reference state->record.

This function is the standard blocking interface used throughout PostgreSQL for sequential WAL record reading in recovery, replication, and analysis operations.

## Parameters / Member Variables
- `state`: Pointer to XLogReaderState containing WAL reading state and configuration
- `errormsg`: Double pointer to char for returning error messages; set to NULL on success or error string on failure

## Return Value
- Returns pointer to XLogRecord header on success, or NULL on failure/end of log
- On error, *errormsg contains error details; on success, *errormsg is set to NULL
- The returned pointer points to the record header within the decoded record structure

## Dependencies
- Functions called/Symbols referenced:
  - [XLogReleasePreviousRecord](XLogReleasePreviousRecord.md) (releases previous record)
  - XLogReaderHasQueuedRecordOrError (checks if records are queued)  
  - [XLogReadAhead](XLogReadAhead.md) (performs read-ahead operations)
  - [XLogNextRecord](XLogNextRecord.md) (gets next record from queue)
  - Assert (debugging assertion macro)
- Data structures used:
  - [DecodedXLogRecord](../D/DecodedXLogRecord.md)
  - [XLogRecord](XLogRecord.md)
  - [XLogReaderState](XLogReaderState.md)
- Called from (representative examples):
  - [XlogReadTwoPhaseData](XlogReadTwoPhaseData.md)
  - [XLogFindNextRecord](XLogFindNextRecord.md)
  - [SummarizeWAL](../S/SummarizeWAL.md)
  - [DecodingContextFindStartpoint](../D/DecodingContextFindStartpoint.md)
  - LogicalReplicationSlotHasPendingWal
  - [XLogSendLogical](XLogSendLogical.md)
  - [extractPageMap](../e/extractPageMap.md)
  - pg_waldump main function

## Notes and Other Information
- Must be preceded by XLogBeginRead() or XLogFindNextRecord() call for initialization
- Provides blocking semantics - will wait for data to be available unlike XLogNextRecord()
- Returns record header pointer for compatibility with legacy XLogRecXXX() macros
- The actual decoded record data is accessible via state->record after successful return
- Handles both I/O errors (via page_read callback) and parsing/validation errors
- Used extensively throughout PostgreSQL for WAL reading in recovery, replication, logical decoding, and utility operations
- The returned record pointer remains valid until the next call to XLogReadRecord() or XLogNextRecord()
- Automatically manages read-ahead operations to ensure optimal performance