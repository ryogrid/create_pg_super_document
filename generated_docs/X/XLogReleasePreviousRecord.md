# XLogReleasePreviousRecord

## Location
[src/backend/access/transam/xlogreader.c:249-324](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogreader.c#L249-L324)

## Overview
Releases memory and resources associated with the previously returned WAL record, managing the decoded record queue and buffer space efficiently.

## Definition
```c
XLogRecPtr XLogReleasePreviousRecord(XLogReaderState *state)
```

## Detailed Description
XLogReleasePreviousRecord manages memory cleanup for WAL records that have been processed by the XLogReader. It removes the current record from the decoded record queue and frees up associated memory space. The function handles two types of records differently: normal records stored in the decode buffer, and oversized records that are allocated separately.

For normal records, it updates the decode buffer head pointer to reclaim space up to the next record. For oversized records, it directly frees the allocated memory. The function also maintains the integrity of the decode queue by updating head and tail pointers appropriately.

This function is essential for preventing memory leaks and managing buffer space efficiently during WAL reading operations, especially in long-running processes that read many WAL records.

## Parameters / Member Variables
- `state`: Pointer to XLogReaderState containing the current WAL reading state and decoded record queue

## Return Value
- Returns XLogRecPtr pointing to the LSN past the end of the released record, or InvalidXLogRecPtr if no record was available to release

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md) (memory deallocation function)
  - Assert (debugging assertion macro)
  - unlikely (branch prediction hint macro)
  - InvalidXLogRecPtr (constant for invalid record pointer)
- Data structures used:
  - [DecodedXLogRecord](../D/DecodedXLogRecord.md)
  - [XLogReaderState](XLogReaderState.md)
- Called from (representative examples):
  - [XLogPrefetcherReadRecord](XLogPrefetcherReadRecord.md)
  - [XLogNextRecord](XLogNextRecord.md)  
  - [XLogReadRecord](XLogReadRecord.md)

## Notes and Other Information
- The function safely handles the case where no previous record exists (returns InvalidXLogRecPtr)
- Distinguishes between regular records (stored in decode buffer) and oversized records (separately allocated)
- Maintains decode queue integrity by properly updating head and tail pointers
- For oversized records, directly calls pfree() to release memory
- For buffer-stored records, updates decode_buffer_head to reclaim space
- When the buffer becomes empty, resets both head and tail pointers to the buffer start for optimal memory reuse
- Uses likely/unlikely hints for branch prediction optimization
- Critical for memory management in long-running WAL reading operations

## Simplified Source

```c
// Simplified version of XLogReleasePreviousRecord
XLogRecPtr XLogReleasePreviousRecord(XLogReaderState *state) {
    // Return early if no record to release
    if (!state->record)
        return InvalidXLogRecPtr;

    // Get the record to release and its next LSN
    DecodedXLogRecord *record = state->record;
    XLogRecPtr next_lsn = record->next_lsn;

    // Remove record from decode queue
    state->record = NULL;
    state->decode_queue_head = record->next;

    // Update tail pointer if this was the last record
    if (state->decode_queue_tail == record)
        state->decode_queue_tail = NULL;

    // Handle memory deallocation based on record type
    if (record->oversized) {
        // Oversized records are separately allocated - free directly
        pfree(record);
    } else {
        // Regular records are in the decode buffer - update buffer head
        state->decode_buffer_head = (char *) record;

        // Find next non-oversized record to set as new buffer head
        record = record->next;
        while (record && record->oversized)
            record = record->next;

        if (record) {
            // Move buffer head to next record
            state->decode_buffer_head = (char *) record;
        } else {
            // No more records - reset buffer to beginning
            state->decode_buffer_head = state->decode_buffer;
            state->decode_buffer_tail = state->decode_buffer;
        }
    }

    return next_lsn;
}
```

Key simplifications made:
- Removed complex assertions for clarity
- Combined variable declarations with assignments
- Added clear comments explaining memory management strategy
- Simplified buffer management logic flow
- Focused on core functionality while preserving essential memory cleanup