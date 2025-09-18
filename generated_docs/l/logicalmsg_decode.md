# logicalmsg_decode

## Location
src/backend/replication/logical/decode.c: 598 - 678

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
  - ReorderBufferProcessXid
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