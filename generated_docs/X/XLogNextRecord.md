# XLogNextRecord

## Location
[src/backend/access/transam/xlogreader.c:325-388](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogreader.c#L325-L388)

## Overview
Retrieves the next available decoded WAL record from the internal queue, managing record lifecycle and maintaining compatibility with legacy XLogRec macros.

## Definition
```c
DecodedXLogRecord *XLogNextRecord(XLogReaderState *state, char **errormsg)
```

## Detailed Description
XLogNextRecord is responsible for returning the next available WAL record from the internal decode queue. It first releases any previously returned record by calling XLogReleasePreviousRecord(), then checks if there are any decoded records available in the queue. If a record is available, it becomes the current record and updates various state pointers for compatibility with historical PostgreSQL code that expects certain fields to be maintained in the XLogReaderState structure.

The function works in conjunction with XLogReadAhead() which populates the internal queue with decoded records. This design allows for efficient prefetching and buffering of WAL records while maintaining a simple interface for consumers.

## Parameters / Member Variables
- `state`: Pointer to XLogReaderState containing the WAL reading state and decode queue
- `errormsg`: Double pointer to char for returning error messages; set to NULL on success or to point to error string on failure

## Return Value
- Returns pointer to DecodedXLogRecord on success, or NULL if no records are available or an error occurred
- On error, *errormsg will point to an error message string; on success, *errormsg is set to NULL

## Dependencies
- Functions called/Symbols referenced:
  - [XLogReleasePreviousRecord](XLogReleasePreviousRecord.md) (releases previous record)
  - XLogRecPtrIsInvalid (validates record pointers)
  - Assert (debugging assertion macro)
- Data structures used:
  - [DecodedXLogRecord](../D/DecodedXLogRecord.md)
  - [XLogReaderState](XLogReaderState.md)
- Called from (representative examples):
  - [XLogPrefetcherReadRecord](XLogPrefetcherReadRecord.md)
  - [XLogReadRecord](XLogReadRecord.md)

## Notes and Other Information
- Must be preceded by XLogBeginRead() or XLogFindNextRecord() and XLogReadAhead() calls
- Automatically releases the previous record to manage memory efficiently
- Maintains backward compatibility by updating ReadRecPtr and EndRecPtr in the state structure
- Returns records that were pre-decoded and queued by XLogReadAhead()
- Handles deferred error messages that may have been set during background reading
- The returned record pointer is valid only until the next call to XLogNextRecord()
- Uses decode_queue_head to track the next record to be returned
- Updates state->record to point to the current record for use by legacy XLogRecXXX() macros
- Error handling includes support for deferred error reporting from background operations

## Simplified Source

```c
DecodedXLogRecord *XLogNextRecord(XLogReaderState *state, char **errormsg)
{
    // Release the previously returned record to free memory
    XLogReleasePreviousRecord(state);

    // Check if there are any records in the decode queue
    if (state->decode_queue_head == NULL) {
        *errormsg = NULL;

        // Handle any deferred error messages
        if (state->errormsg_deferred) {
            if (state->errormsg_buf[0] != '\0')
                *errormsg = state->errormsg_buf;
            state->errormsg_deferred = false;
        }

        Assert(!XLogRecPtrIsInvalid(state->EndRecPtr));
        return NULL;  // No records available
    }

    // Set this record as the current one for legacy compatibility
    state->record = state->decode_queue_head;

    // Update state pointers for historical XLogRecXXX() macros
    state->ReadRecPtr = state->record->lsn;
    state->EndRecPtr = state->record->next_lsn;

    *errormsg = NULL;
    return state->record;
}
```