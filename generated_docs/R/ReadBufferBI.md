# ReadBufferBI

## Location
[src/backend/access/heap/hio.c:88-139](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/hio.c#L88-L139)

## Overview
ReadBufferBI is a static function that reads a buffer using bulk-insert optimization strategy, providing efficient buffer management for bulk insert operations by caching and reusing the current buffer when possible.

## Definition

```c
static Buffer
ReadBufferBI(Relation relation, BlockNumber targetBlock,
			 ReadBufferMode mode, BulkInsertState bistate)
```
## Detailed Description
This function optimizes buffer reading for bulk insert operations by implementing a caching strategy. When a BulkInsertState is provided, it maintains a cached current buffer to avoid repeated reads of the same block. Key behaviors include:

- If bistate is NULL, falls back to standard ReadBufferExtended behavior
- If the desired block is already pinned in bistate->current_buf, reuses it by incrementing reference count
- If a different block is needed, releases the old cached buffer and reads the new one
- Uses the bulk insert buffer strategy for optimal I/O performance
- Caches the newly read buffer for potential future reuse

The function includes assertions to ensure LOCK variants are only used for relation extension, not for accessing existing blocks.

## Parameters / Member Variables
- : The relation from which to read the buffer
- : The block number to read from the relation
- : The buffer read mode specifying access pattern and locking behavior
- : Bulk insert state containing cached buffer and I/O strategy (NULL for standard read)

## Dependencies
- Functions called/Symbols referenced:
  - [ReadBufferExtended](ReadBufferExtended.md)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - [IncrBufferRefCount](../I/IncrBufferRefCount.md)
  - ReleaseBuffer
- Constants/Types referenced:
  - MAIN_FORKNUM
  - InvalidBuffer
  - RBM_ZERO_AND_LOCK
  - RBM_ZERO_AND_CLEANUP_LOCK
- Called from:
  - [RelationGetBufferForTuple](RelationGetBufferForTuple.md)

## Notes and Other Information
- Static function within hio.c, not exposed to external modules
- Optimization specifically designed for bulk insert workloads where consecutive operations often target the same block
- Buffer reference counting ensures proper cleanup and prevents premature eviction
- The caching strategy reduces I/O overhead during bulk operations by avoiding redundant buffer reads
- Assert checks prevent misuse of locking modes with cached buffers