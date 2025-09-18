# LogicalDecodingProcessRecord

## Location
[src/backend/replication/logical/decode.c:88-128](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/decode.c#L88-L128)

## Overview
Processes each WAL record read by XLogReadRecord() and performs the necessary actions to decode it using the configured output plugin in the logical decoding context.

## Definition
```c
void LogicalDecodingProcessRecord(LogicalDecodingContext *ctx, XLogReaderState *record)
```

## Detailed Description
This is the main entry point for processing WAL records during logical replication decoding. It takes every record read from the WAL and applies the appropriate decoding logic through the resource manager system. The function handles transaction ID assignment, subxact-to-top-level-xact mapping, and delegates specific record type processing to the appropriate resource manager decode functions.

The function ensures that all transaction IDs from records are processed by the reorder buffer, which is essential for maintaining transaction ordering during logical replication. It also supports fast-forwarding through certain record types when appropriate.

Key responsibilities:
1. Sets up record buffer information (original pointer, end pointer, record data)
2. Extracts and processes transaction IDs for proper transaction tracking
3. Assigns child transactions to their parent transactions when applicable
4. Delegates record-specific decoding to the appropriate resource manager
5. Ensures transaction processing through the reorder buffer system

## Parameters / Member Variables
- `ctx`: LogicalDecodingContext pointer containing the decoding state, reorder buffer, and output plugin configuration
- `record`: XLogReaderState pointer representing the current WAL record being processed

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetTopXid
  - ReorderBufferAssignChild
  - XLogRecGetXid
  - GetRmgr
  - XLogRecGetRmid
  - ReorderBufferProcessXid
- Data types used:
  - [LogicalDecodingContext](LogicalDecodingContext.md)
  - [XLogRecordBuffer](../X/XLogRecordBuffer.md)
  - RmgrData
- Called from (representative examples):
  - [DecodingContextFindStartpoint](../D/DecodingContextFindStartpoint.md)
  - LogicalReplicationSlotHasPendingWal
  - [pg_logical_slot_get_changes_guts](../p/pg_logical_slot_get_changes_guts.md)
  - [XLogSendLogical](../X/XLogSendLogical.md)

## Notes and Other Information
- Every record's XID must be processed by the reorder buffer, regardless of whether the record content is relevant
- The function optimizes handling of empty transactions by avoiding unnecessary ReorderBufferProcessXid calls when resource managers can handle records more efficiently
- The rm_decode function pointer may be NULL for some resource managers, in which case only XID processing is performed
- This function is central to the logical replication system and is called for every WAL record during decoding operations