# DecodeUpdate

## Location
src/backend/replication/logical/decode.c: 965 - 1031

## Overview
DecodeUpdate processes HEAP_UPDATE and HEAP_HOT_UPDATE WAL records in PostgreSQL's logical replication, extracting update operations and converting them into reorder buffer changes with both old and new tuple data.

## Definition


## Detailed Description
DecodeUpdate handles the decoding of heap update operations from WAL records for logical replication. It processes both XLOG_HEAP_UPDATE and XLOG_HEAP_HOT_UPDATE records, which share the same layout structure. The function extracts both the new tuple data and the old tuple data (when available) from the WAL record.

The function performs standard validation steps including database filtering and origin filtering, then creates a ReorderBufferChange with REORDER_BUFFER_CHANGE_UPDATE action. Depending on the flags in the WAL record, it may extract:
1. New tuple data (when XLH_UPDATE_CONTAINS_NEW_TUPLE is set)
2. Old tuple data (when XLH_UPDATE_CONTAINS_OLD is set)

The old tuple data is stored separately in the record after the heap update structure, requiring careful offset calculations to extract properly.

## Parameters / Member Variables
- : LogicalDecodingContext containing the decoding state, replication slot, and configuration
- : XLogRecordBuffer containing the WAL record with update data to be processed

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - XLogRecGetBlockTag
  - FilterByOrigin
  - XLogRecGetOrigin
  - ReorderBufferGetChange
  - XLogRecGetBlockData
  - ReorderBufferGetTupleBuf
  - DecodeXLogTuple
  - XLogRecGetDataLen
  - ReorderBufferQueueChange
  - XLogRecGetXid
- Called from (representative examples):
  - heap_decode

## Notes and Other Information
- Handles both regular updates (XLOG_HEAP_UPDATE) and HOT updates (XLOG_HEAP_HOT_UPDATE) which have identical record layouts
- Conditionally extracts new tuple data based on XLH_UPDATE_CONTAINS_NEW_TUPLE flag
- Conditionally extracts old tuple data based on XLH_UPDATE_CONTAINS_OLD flag, with special handling for unaligned data positioning
- Old tuple data is located after the SizeOfHeapUpdate offset in the record data
- Sets clear_toast_afterwards flag to ensure proper cleanup of TOAST data after processing
- Critical component enabling logical replication of update operations, providing both before and after row states for output plugins