# FlushRelationBuffers

## Location
src/backend/storage/buffer/bufmgr.c: 4482 - 4579

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
  - GetLocalBufferDescriptor
  - BufTagMatchesRelFileLocator
  - pg_atomic_read_u32
  - LocalBufHdrGetBlock
  - PageSetChecksumInplace
  - pgstat_prepare_io_time
  - smgrwrite
  - BufTagGetForkNum
  - pgstat_count_io_op_time
  - pg_atomic_unlocked_write_u32
  - GetBufferDescriptor
  - ReservePrivateRefCountEntry
  - ResourceOwnerEnlarge
  - LockBufHdr
  - PinBuffer_Locked
  - BufferDescriptorGetContentLock
  - FlushBuffer
  - UnpinBuffer
  - UnlockBufHdr
- Constants used:
  - BM_VALID, BM_DIRTY, BM_JUST_DIRTIED
  - IOOBJECT_TEMP_RELATION, IOOBJECT_RELATION
  - IOCONTEXT_NORMAL, IOOP_WRITE
  - LW_SHARED
- Types used:
  - BufferDesc, SMgrRelation, instr_time
- Called from (representative examples):
  - heapam_relation_copy_data
  - fill_seq_with_data
  - index_copy_data

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