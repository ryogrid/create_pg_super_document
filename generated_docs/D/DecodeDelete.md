# DecodeDelete

## Location
[src/backend/replication/logical/decode.c:1032-1085](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/decode.c#L1032-L1085)

## Overview
DecodeDelete processes HEAP_DELETE WAL records in PostgreSQL's logical replication, extracting delete operations and converting them into reorder buffer changes with old tuple data when available.

## Definition

```c
static void
DecodeDelete(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
```
## Detailed Description
DecodeDelete handles the decoding of heap delete operations from WAL records for logical replication. It processes XLOG_HEAP_DELETE records and creates ReorderBufferChange structures for logical decoding output plugins. The function distinguishes between regular deletes and super deletes (speculative delete aborts).

The function performs standard validation including database filtering and origin filtering. For regular deletes, it creates a REORDER_BUFFER_CHANGE_DELETE action, while super deletes (marked with XLH_DELETE_IS_SUPER flag) are treated as internal speculative aborts (REORDER_BUFFER_CHANGE_INTERNAL_SPEC_ABORT).

When the XLH_DELETE_CONTAINS_OLD flag is set, the function extracts the old tuple data from the WAL record, which contains the primary key or full row information needed for logical replication subscribers to identify and process the delete operation.

## Parameters / Member Variables
- `*ctx`: LogicalDecodingContext containing the decoding state, replication slot, and configuration
- `*buf`: XLogRecordBuffer containing the WAL record with delete data to be processed
## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - [XLogRecGetBlockTag](../X/XLogRecGetBlockTag.md)
  - [FilterByOrigin](../F/FilterByOrigin.md)
  - XLogRecGetOrigin
  - [ReorderBufferGetChange](../R/ReorderBufferGetChange.md)
  - XLogRecGetDataLen
  - [ReorderBufferGetTupleBuf](../R/ReorderBufferGetTupleBuf.md)
  - [DecodeXLogTuple](DecodeXLogTuple.md)
  - [ReorderBufferQueueChange](../R/ReorderBufferQueueChange.md)
  - XLogRecGetXid
- Called from (representative examples):
  - [heap_decode](../h/heap_decode.md)

## Notes and Other Information
- Distinguishes between regular deletes and super deletes (speculative delete aborts) through the XLH_DELETE_IS_SUPER flag
- Conditionally extracts old tuple data based on XLH_DELETE_CONTAINS_OLD flag, which contains the deleted row's primary key or full row data
- Old tuple data is located after the SizeOfHeapDelete offset in the record
- Includes assertion to validate that sufficient data exists in the record when extracting old tuple information
- Sets clear_toast_afterwards flag to ensure proper cleanup of TOAST data after processing
- Essential for logical replication delete processing, providing subscribers with necessary information to identify and remove the corresponding rows