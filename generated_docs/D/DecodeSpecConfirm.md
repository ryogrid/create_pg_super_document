# DecodeSpecConfirm

## Location
[src/backend/replication/logical/decode.c:1230-1265](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/decode.c#L1230-L1265)

## Overview
DecodeSpecConfirm processes XLOG_HEAP_CONFIRM WAL records during logical replication, creating confirmation changes for previously speculative tuple insertions.

## Definition
```c
static void DecodeSpecConfirm(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
```

## Detailed Description
DecodeSpecConfirm handles the confirmation phase of speculative insertions during logical replication. In PostgreSQL, speculative insertions are used for operations like INSERT ... ON CONFLICT DO NOTHING/UPDATE, where a tuple is tentatively inserted and then either confirmed (if no conflict occurs) or killed (if a conflict is detected).

When a speculative insertion is confirmed, PostgreSQL generates an XLOG_HEAP_CONFIRM WAL record. This function processes such records and creates an internal confirmation change that pairs with the previously decoded speculative insertion. The confirmation change signals to the logical replication system that the speculative tuple should be treated as a committed insertion.

The function is relatively simple compared to other decode functions because most of the work was done during the initial speculative insertion - this just provides the confirmation signal.

Key characteristics:
1. Only processes records from the target database
2. Applies origin filtering for selective replication
3. Creates an internal confirmation change type
4. Always sets toast clearing flag since confirmation finalizes the operation
5. Pairs with previously queued speculative insertion changes

## Parameters / Member Variables
- `ctx`: LogicalDecodingContext containing the decoding session state, replication slot, and reorder buffer
- `buf`: XLogRecordBuffer containing the heap confirmation WAL record and its metadata

## Dependencies
- Functions called/Symbols referenced:
  - [XLogRecGetBlockTag](../X/XLogRecGetBlockTag.md)
  - [FilterByOrigin](../F/FilterByOrigin.md)
  - XLogRecGetOrigin
  - [ReorderBufferGetChange](../R/ReorderBufferGetChange.md)
  - [ReorderBufferQueueChange](../R/ReorderBufferQueueChange.md)
  - XLogRecGetXid
- Called from (representative examples):
  - [heap_decode](../h/heap_decode.md)

## Notes and Other Information
- This function is part of PostgreSQL's speculative insertion mechanism used by INSERT ... ON CONFLICT
- The confirmation change uses the special REORDER_BUFFER_CHANGE_INTERNAL_SPEC_CONFIRM action type
- Always sets clear_toast_afterwards to true since confirmation finalizes the tuple state
- The actual tuple data was already processed during the speculative insertion phase
- Works in conjunction with speculative insertion and potential speculative deletion (kill) operations
- The target_locator identifies which relation the confirmed insertion belongs to
- This is an internal change type that may not be directly visible to all output plugins

## Simplified Source

```c
static void DecodeSpecConfirm(LogicalDecodingContext *ctx, XLogRecordBuffer *buf) {
    XLogReaderState *r = buf->record;
    ReorderBufferChange *change;
    RelFileLocator target_locator;

    // Check if this is for our target database
    XLogRecGetBlockTag(r, 0, &target_locator, NULL, NULL);
    if (target_locator.dbOid != ctx->slot->data.database)
        return;

    // Apply origin filtering
    if (FilterByOrigin(ctx, XLogRecGetOrigin(r)))
        return;

    // Create confirmation change
    change = ReorderBufferGetChange(ctx->reorder);
    change->action = REORDER_BUFFER_CHANGE_INTERNAL_SPEC_CONFIRM;
    change->origin_id = XLogRecGetOrigin(r);

    // Copy relation file locator
    memcpy(&change->data.tp.rlocator, &target_locator, sizeof(RelFileLocator));

    // Set toast clearing flag (confirmation finalizes the operation)
    change->data.tp.clear_toast_afterwards = true;

    // Queue the confirmation change
    ReorderBufferQueueChange(ctx->reorder, XLogRecGetXid(r), buf->origptr,
                            change, false);
}
```