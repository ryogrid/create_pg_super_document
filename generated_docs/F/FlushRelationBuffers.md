# FlushRelationBuffers

## Location
[src/backend/storage/buffer/bufmgr.c:4482-4579](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L4482-L4579)

## Overview
Writes all dirty pages of a specific relation to disk, ensuring the kernel has an up-to-date view of the relation's data.

## Definition


## Detailed Description
This function ensures that all dirty (modified) pages belonging to a specific relation are written out to disk (or more precisely, to kernel disk buffers). It handles both local buffers (for temporary relations) and shared buffers (for permanent relations) appropriately. The function performs a sequential search through the appropriate buffer pool, identifying buffers that belong to the target relation and are both valid and dirty, then flushes them to storage.

For local buffers, the function directly writes pages using smgrwrite() and handles checksums, I/O timing statistics, and error context tracking. For shared buffers, it uses the standard buffer management protocol with proper locking, pinning the buffer during the flush operation to ensure consistency.

The caller should typically hold AccessExclusiveLock on the target relation to prevent concurrent modifications that could dirty additional pages during the flush operation.

## Parameters / Member Variables
- : The Relation structure representing the relation whose buffers should be flushed to disk

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetSmgr
  - RelationUsesLocalBuffers
  - [GetLocalBufferDescriptor](../G/GetLocalBufferDescriptor.md)
  - [BufTagMatchesRelFileLocator](../B/BufTagMatchesRelFileLocator.md)
  - [pg_atomic_read_u32](../p/pg_atomic_read_u32.md)
  - LocalBufHdrGetBlock
  - [PageSetChecksumInplace](../P/PageSetChecksumInplace.md)
  - [pgstat_prepare_io_time](../p/pgstat_prepare_io_time.md)
  - smgrwrite
  - [BufTagGetForkNum](../B/BufTagGetForkNum.md)
  - [pgstat_count_io_op_time](../p/pgstat_count_io_op_time.md)
  - [pg_atomic_unlocked_write_u32](../p/pg_atomic_unlocked_write_u32.md)
  - [GetBufferDescriptor](../G/GetBufferDescriptor.md)
  - [ReservePrivateRefCountEntry](../R/ReservePrivateRefCountEntry.md)
  - ResourceOwnerEnlarge
  - LockBufHdr
  - PinBuffer_Locked
  - [BufferDescriptorGetContentLock](../B/BufferDescriptorGetContentLock.md)
  - [FlushBuffer](FlushBuffer.md)
  - UnpinBuffer
  - [UnlockBufHdr](../U/UnlockBufHdr.md)
- Constants used:
  - BM_VALID, BM_DIRTY, BM_JUST_DIRTIED
  - IOOBJECT_TEMP_RELATION, IOOBJECT_RELATION
  - IOCONTEXT_NORMAL, IOOP_WRITE
  - LW_SHARED
- Types used:
  - [BufferDesc](../B/BufferDesc.md), SMgrRelation, instr_time
- Called from (representative examples):
  - [heapam_relation_copy_data](../h/heapam_relation_copy_data.md)
  - [fill_seq_with_data](../f/fill_seq_with_data.md)
  - [index_copy_data](../i/index_copy_data.md)

## Notes and Other Information
- Currently uses sequential search through buffer pools, which is noted as suboptimal but acceptable since the function is not used in performance-critical paths
- Handles both local buffers (temporary relations) and shared buffers (permanent relations) with different code paths
- For local buffers: directly manages checksums, I/O statistics, and error handling
- For shared buffers: uses proper locking protocol with buffer pinning during flush
- Uses unlocked precheck optimization to avoid unnecessary locking when buffer relations don't match
- Caller should typically hold AccessExclusiveLock on the relation to prevent concurrent dirtying
- Effects may not persist after the exclusive lock is released due to potential concurrent modifications
- Includes comprehensive error context tracking for local buffer writes
- Updates I/O statistics and buffer usage counters appropriately