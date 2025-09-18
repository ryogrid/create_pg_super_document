# XLogReleasePreviousRecord

## Location
src/backend/access/transam/xlogreader.c: 249 - 324

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
  - pfree (memory deallocation function)
  - Assert (debugging assertion macro)
  - unlikely (branch prediction hint macro)
  - InvalidXLogRecPtr (constant for invalid record pointer)
- Data structures used:
  - DecodedXLogRecord
  - XLogReaderState
- Called from (representative examples):
  - XLogPrefetcherReadRecord
  - XLogNextRecord  
  - XLogReadRecord

## Notes and Other Information
- The function safely handles the case where no previous record exists (returns InvalidXLogRecPtr)
- Distinguishes between regular records (stored in decode buffer) and oversized records (separately allocated)
- Maintains decode queue integrity by properly updating head and tail pointers
- For oversized records, directly calls pfree() to release memory
- For buffer-stored records, updates decode_buffer_head to reclaim space
- When the buffer becomes empty, resets both head and tail pointers to the buffer start for optimal memory reuse
- Uses likely/unlikely hints for branch prediction optimization
- Critical for memory management in long-running WAL reading operations