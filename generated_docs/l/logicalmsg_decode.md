# logicalmsg_decode

## Location
[src/backend/replication/logical/decode.c:598-678](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/decode.c#L598-L678)

## Overview
The `logicalmsg_decode` function handles resource manager LOGICALMSG_ID records for logical decoding, processing logical messages that can be sent through replication streams.

## Definition
```c
void logicalmsg_decode(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
```

## Detailed Description
This function processes logical message WAL records that represent application-level messages sent through the replication stream using `pg_logical_emit_message()`. These messages can be either transactional (part of a transaction) or non-transactional (independent of transaction boundaries).

The function performs several filtering and validation steps:
1. **Database filtering**: Ensures messages belong to the correct database
2. **Origin filtering**: Applies origin-based filtering to prevent replication loops
3. **Snapshot validation**: Checks if appropriate snapshots are available
4. **Transactional handling**: Differentiates between transactional and non-transactional messages

For transactional messages, the function uses the standard snapshot management through ReorderBuffer. For non-transactional messages, it requires a consistent snapshot and handles them immediately without waiting for transaction boundaries.

## Parameters / Member Variables
- `ctx`: LogicalDecodingContext containing the decoding state, reorder buffer, snapshot builder, and database slot information
- `buf`: XLogRecordBuffer containing the logical message WAL record with original LSN position

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetXid
  - XLogRecGetInfo
  - XLogRecGetOrigin
  - XLogRecGetData
  - [ReorderBufferProcessXid](../R/ReorderBufferProcessXid.md)
  - [SnapBuildCurrentState](../S/SnapBuildCurrentState.md)
  - [FilterByOrigin](../F/FilterByOrigin.md)
  - [SnapBuildProcessChange](../S/SnapBuildProcessChange.md)
  - [SnapBuildXactNeedsSkip](../S/SnapBuildXactNeedsSkip.md)
  - [SnapBuildGetOrBuildSnapshot](../S/SnapBuildGetOrBuildSnapshot.md)
  - [ReorderBufferQueueMessage](../R/ReorderBufferQueueMessage.md)
- Called from (representative examples):
  - Referenced in rmgrlist.h as the decode function for RM_LOGICALMSG_ID
  - Used by the logical decoding infrastructure for processing logical messages

## Notes and Other Information
- Supports both transactional and non-transactional logical messages
- Non-transactional messages are processed immediately when the snapshot is consistent
- Transactional messages are queued in the reorder buffer and processed with their containing transaction
- Database filtering ensures messages are only decoded for the correct target database
- Fast-forward mode skips actual decoding but sets processing_required flag for non-transactional messages
- The function validates that only XLOG_LOGICAL_MESSAGE records are processed
- Messages contain a prefix and content, both of which are passed to the reorder buffer
- Origin filtering prevents infinite loops in multi-master replication scenarios

## Simplified Source

```c
void logicalmsg_decode(LogicalDecodingContext *ctx, XLogRecordBuffer *buf) {
    SnapBuild *builder = ctx->snapshot_builder;
    XLogReaderState *r = buf->record;
    TransactionId xid = XLogRecGetXid(r);
    uint8 info = XLogRecGetInfo(r) & ~XLR_INFO_MASK;
    RepOriginId origin_id = XLogRecGetOrigin(r);
    Snapshot snapshot = NULL;
    xl_logical_message *message;

    // Validate record type
    if (info != XLOG_LOGICAL_MESSAGE)
        elog(ERROR, "unexpected RM_LOGICALMSG_ID record type: %u", info);

    // Process transaction ID
    ReorderBufferProcessXid(ctx->reorder, XLogRecGetXid(r), buf->origptr);

    // Check if we have a valid snapshot for decoding
    if (SnapBuildCurrentState(builder) < SNAPBUILD_FULL_SNAPSHOT)
        return;

    message = (xl_logical_message *) XLogRecGetData(r);

    // Filter by database and origin
    if (message->dbId != ctx->slot->data.database || FilterByOrigin(ctx, origin_id))
        return;

    // Handle transactional vs non-transactional messages
    if (message->transactional) {
        if (!SnapBuildProcessChange(builder, xid, buf->origptr))
            return;
    } else {
        if (SnapBuildCurrentState(builder) != SNAPBUILD_CONSISTENT ||
            SnapBuildXactNeedsSkip(builder, buf->origptr))
            return;
    }

    // Skip decoding in fast-forward mode but mark processing required
    if (ctx->fast_forward) {
        if (!message->transactional)
            ctx->processing_required = true;
        return;
    }

    // Get snapshot for non-transactional messages
    if (!message->transactional)
        snapshot = SnapBuildGetOrBuildSnapshot(builder);

    // Queue the message in reorder buffer
    ReorderBufferQueueMessage(ctx->reorder, xid, snapshot, buf->endptr,
                              message->transactional,
                              message->message,
                              message->message_size,
                              message->message + message->prefix_size);
}
```