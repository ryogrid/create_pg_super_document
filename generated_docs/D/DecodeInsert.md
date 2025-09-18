# DecodeInsert

## Location
[src/backend/replication/logical/decode.c:906-964](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/decode.c#L906-L964)

## Overview
DecodeInsert processes HEAP_INSERT WAL records in PostgreSQL's logical replication, extracting insert operations and converting them into reorder buffer changes for logical decoding output.

## Definition


## Detailed Description
DecodeInsert handles the decoding of heap insert operations from WAL records for logical replication. It parses XLOG_HEAP_INSERT records (excluding MULTI_INSERT records) and converts them into ReorderBufferChange structures that can be processed by output plugins.

The function performs several validation steps:
1. Checks if the record contains new tuple data (ignoring TOAST-only records)
2. Filters records to only process those from the target database
3. Applies origin filtering if configured
4. Distinguishes between regular and speculative inserts

After validation, it extracts the tuple data from the WAL record, creates a reorder buffer change with the appropriate action type, and queues the change for processing. The function also handles TOAST relation considerations and ensures proper cleanup after processing.

## Parameters / Member Variables
- : LogicalDecodingContext containing the decoding state, replication slot, and configuration
- : XLogRecordBuffer containing the WAL record with insert data to be processed

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - [XLogRecGetBlockTag](../X/XLogRecGetBlockTag.md)
  - [FilterByOrigin](../F/FilterByOrigin.md)
  - XLogRecGetOrigin
  - [ReorderBufferGetChange](../R/ReorderBufferGetChange.md)
  - [XLogRecGetBlockData](../X/XLogRecGetBlockData.md)
  - [ReorderBufferGetTupleBuf](../R/ReorderBufferGetTupleBuf.md)
  - [DecodeXLogTuple](DecodeXLogTuple.md)
  - [ReorderBufferQueueChange](../R/ReorderBufferQueueChange.md)
  - XLogRecGetXid
- Called from (representative examples):
  - [heap_decode](../h/heap_decode.md)

## Notes and Other Information
- Only processes XLOG_HEAP_INSERT records, not MULTI_INSERT records which are handled separately
- Distinguishes between regular inserts (REORDER_BUFFER_CHANGE_INSERT) and speculative inserts (REORDER_BUFFER_CHANGE_INTERNAL_SPEC_INSERT)
- Ignores records without new tuple data, which can occur when TOAST records are marked with HEAP_INSERT_NO_LOGICAL
- Handles TOAST relation inserts specially through the XLH_INSERT_ON_TOAST_RELATION flag
- Sets clear_toast_afterwards flag to ensure proper cleanup of TOAST data after processing
- Critical component of the heap decode pathway that enables logical replication of insert operations