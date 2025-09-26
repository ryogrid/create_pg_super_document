# heap_decode

## Location
[src/backend/replication/logical/decode.c:468-562](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/decode.c#L468-L562)

## Overview
The `heap_decode` function handles resource manager HEAP_ID records for logical decoding, processing various heap operation types from WAL records to decode changes for logical replication.

## Definition
```c
void heap_decode(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
```

## Detailed Description
This function serves as the main entry point for decoding heap-related WAL records during logical replication. It extracts the operation type from the WAL record and dispatches to the appropriate decoding function based on the operation type (INSERT, UPDATE, DELETE, etc.). The function implements essential logic for snapshot building and fast-forward mode handling, ensuring that only relevant changes are processed when a full snapshot is available.

The function handles several types of heap operations:
- **INSERT**: New row insertions
- **UPDATE/HOT_UPDATE**: Row modifications (treats HOT updates as normal updates)
- **DELETE**: Row deletions  
- **TRUNCATE**: Table truncations
- **INPLACE**: In-place catalog updates (marks transaction as catalog-modifying)
- **CONFIRM**: Speculative insertion confirmations
- **LOCK**: Row-level locks (currently ignored)

## Parameters / Member Variables
- `ctx`: LogicalDecodingContext containing the decoding state, reorder buffer, snapshot builder, and fast-forward flag
- `buf`: XLogRecordBuffer containing the WAL record to be decoded with original LSN position

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetInfo
  - XLogRecGetXid
  - [ReorderBufferProcessXid](../R/ReorderBufferProcessXid.md)
  - [SnapBuildCurrentState](../S/SnapBuildCurrentState.md)
  - [SnapBuildProcessChange](../S/SnapBuildProcessChange.md)
  - [DecodeInsert](../D/DecodeInsert.md)
  - [DecodeUpdate](../D/DecodeUpdate.md)
  - [DecodeDelete](../D/DecodeDelete.md)
  - [DecodeTruncate](../D/DecodeTruncate.md)
  - [DecodeSpecConfirm](../D/DecodeSpecConfirm.md)
  - [ReorderBufferXidSetCatalogChanges](../R/ReorderBufferXidSetCatalogChanges.md)
- Called from (representative examples):
  - Referenced in rmgrlist.h as the decode function for RM_HEAP_ID
  - Used by the logical decoding infrastructure

## Notes and Other Information
- The function performs early exit if snapshot building is incomplete (`SNAPBUILD_FULL_SNAPSHOT` not reached)
- In-place updates are treated specially as they only affect catalog tuples and don't change tuple visibility
- Fast-forward mode skips actual decoding but maintains snapshot building for determining catalog_xmin
- HOT (Heap-Only Tuple) updates are processed identically to regular updates for logical decoding purposes
- Row-level locks are currently ignored as they don't affect logical replication output