# DecodeUpdate

## Location
[src/backend/replication/logical/decode.c:965-1031](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/decode.c#L965-L1031)

## Overview
DecodeUpdate processes HEAP_UPDATE and HEAP_HOT_UPDATE WAL records in PostgreSQL's logical replication, extracting update operations and converting them into reorder buffer changes with both old and new tuple data.

## Definition

```c
static void
DecodeUpdate(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
```
## Detailed Description
DecodeUpdate handles the decoding of heap update operations from WAL records for logical replication. It processes both XLOG_HEAP_UPDATE and XLOG_HEAP_HOT_UPDATE records, which share the same layout structure. The function extracts both the new tuple data and the old tuple data (when available) from the WAL record.

The function performs standard validation steps including database filtering and origin filtering, then creates a ReorderBufferChange with REORDER_BUFFER_CHANGE_UPDATE action. Depending on the flags in the WAL record, it may extract:
1. New tuple data (when XLH_UPDATE_CONTAINS_NEW_TUPLE is set)
2. Old tuple data (when XLH_UPDATE_CONTAINS_OLD is set)

The old tuple data is stored separately in the record after the heap update structure, requiring careful offset calculations to extract properly.

## Parameters / Member Variables
- `*ctx`: LogicalDecodingContext containing the decoding state, replication slot, and configuration
- `*buf`: XLogRecordBuffer containing the WAL record with update data to be processed
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
  - XLogRecGetDataLen
  - [ReorderBufferQueueChange](../R/ReorderBufferQueueChange.md)
  - XLogRecGetXid
- Called from (representative examples):
  - [heap_decode](../h/heap_decode.md)

## Notes and Other Information
- Handles both regular updates (XLOG_HEAP_UPDATE) and HOT updates (XLOG_HEAP_HOT_UPDATE) which have identical record layouts
- Conditionally extracts new tuple data based on XLH_UPDATE_CONTAINS_NEW_TUPLE flag
- Conditionally extracts old tuple data based on XLH_UPDATE_CONTAINS_OLD flag, with special handling for unaligned data positioning
- Old tuple data is located after the SizeOfHeapUpdate offset in the record data
- Sets clear_toast_afterwards flag to ensure proper cleanup of TOAST data after processing
- Critical component enabling logical replication of update operations, providing both before and after row states for output plugins

## Simplified Source

```c
static void DecodeUpdate(LogicalDecodingContext *ctx, XLogRecordBuffer *buf) {
    XLogReaderState *r = buf->record;
    xl_heap_update *xlrec;
    ReorderBufferChange *change;
    char *data;
    RelFileLocator target_locator;

    xlrec = (xl_heap_update *) XLogRecGetData(r);

    // Check if this is for our target database
    XLogRecGetBlockTag(r, 0, &target_locator, NULL, NULL);
    if (target_locator.dbOid != ctx->slot->data.database)
        return;

    // Apply origin filtering
    if (FilterByOrigin(ctx, XLogRecGetOrigin(r)))
        return;

    // Create reorder buffer change
    change = ReorderBufferGetChange(ctx->reorder);
    change->action = REORDER_BUFFER_CHANGE_UPDATE;
    change->origin_id = XLogRecGetOrigin(r);
    memcpy(&change->data.tp.rlocator, &target_locator, sizeof(RelFileLocator));

    // Extract new tuple if present
    if (xlrec->flags & XLH_UPDATE_CONTAINS_NEW_TUPLE) {
        Size datalen;
        Size tuplelen;

        data = XLogRecGetBlockData(r, 0, &datalen);
        tuplelen = datalen - SizeOfHeapHeader;

        change->data.tp.newtuple = ReorderBufferGetTupleBuf(ctx->reorder, tuplelen);
        DecodeXLogTuple(data, datalen, change->data.tp.newtuple);
    }

    // Extract old tuple if present
    if (xlrec->flags & XLH_UPDATE_CONTAINS_OLD) {
        Size datalen;
        Size tuplelen;

        // Old tuple data follows the heap update structure
        data = XLogRecGetData(r) + SizeOfHeapUpdate;
        datalen = XLogRecGetDataLen(r) - SizeOfHeapUpdate;
        tuplelen = datalen - SizeOfHeapHeader;

        change->data.tp.oldtuple = ReorderBufferGetTupleBuf(ctx->reorder, tuplelen);
        DecodeXLogTuple(data, datalen, change->data.tp.oldtuple);
    }

    change->data.tp.clear_toast_afterwards = true;

    // Queue the change for processing
    ReorderBufferQueueChange(ctx->reorder, XLogRecGetXid(r), buf->origptr,
                            change, false);
}
```