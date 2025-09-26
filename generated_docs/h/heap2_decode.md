# heap2_decode

## Location
[src/backend/replication/logical/decode.c:404-467](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/decode.c#L404-L467)

## Overview
Handles specialized heap-related WAL records (RM_HEAP2_ID) during logical decoding, processing multi-inserts, command ID assignments, and table rewrites for logical replication consistency.

## Definition
```c
void heap2_decode(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
```

## Detailed Description
This function is the logical decoding handler for the secondary heap resource manager (RM_HEAP2_ID), which handles specialized heap operations that don't fit into the primary heap resource manager. It processes several types of records that are important for maintaining consistency during logical replication.

Key record types processed:

1. **MULTI_INSERT**: Handles bulk insert operations, which are common in high-throughput scenarios and require special decoding logic
2. **NEW_CID**: Processes command ID changes that are essential for proper visibility and transaction semantics
3. **REWRITE**: Handles table rewrites, though the actual work is done during crash/archive recovery

The function implements important optimizations:
- Skips processing during fast-forward mode unless building base snapshots
- Only processes changes when a full snapshot is available
- Defers to specialized decode functions for complex operations

Physical maintenance operations (pruning, visibility changes, locking) are ignored as they don't affect logical replication content.

## Parameters / Member Variables
- `ctx`: LogicalDecodingContext pointer containing snapshot builder, reorder buffer, fast-forward flags, and other decoding state
- `buf`: XLogRecordBuffer pointer containing the current HEAP2 record, including WAL position and record data

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetInfo
  - XLogRecGetXid
  - [ReorderBufferProcessXid](../R/ReorderBufferProcessXid.md)
  - [SnapBuildCurrentState](../S/SnapBuildCurrentState.md)
  - [SnapBuildProcessChange](../S/SnapBuildProcessChange.md)
  - [DecodeMultiInsert](../D/DecodeMultiInsert.md)
  - XLogRecGetData
  - [SnapBuildProcessNewCid](../S/SnapBuildProcessNewCid.md)
- Constants used:
  - XLOG_HEAP_OPMASK
  - SNAPBUILD_FULL_SNAPSHOT
  - XLOG_HEAP2_MULTI_INSERT
  - XLOG_HEAP2_NEW_CID
  - XLOG_HEAP2_REWRITE
  - Various physical operation constants (PRUNE, VISIBLE, LOCK_UPDATED)
- Data types used:
  - [xl_heap_new_cid](../x/xl_heap_new_cid.md)
- Called from:
  - Resource manager system via LogicalDecodingProcessRecord (registered in rmgrlist.h)

## Notes and Other Information
- This function is registered as the decode handler for RM_HEAP2_ID in the resource manager list
- Critical for building base snapshots during fast-forward mode, even when not decoding actual changes
- Multi-insert processing is conditional on both snapshot state and fast-forward mode
- [Command](../C/Command.md) ID processing is essential for maintaining proper transaction visibility semantics
- Table rewrite records exist specifically for logical decoding but are handled during recovery, not here
- Physical maintenance operations (pruning, visibility, locking) are ignored as they don't affect logical content
- Requires a full snapshot (SNAPBUILD_FULL_SNAPSHOT) before processing most operations
- Works closely with the snapshot builder to maintain transaction visibility consistency
- Performance-optimized to avoid unnecessary work during fast-forward operations while still maintaining required state