# FlushRelationBuffers

## Location
[src/backend/storage/buffer/bufmgr.c:4482-4579](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L4482-L4579)

## Overview
Writes all dirty pages of a specific relation to disk, ensuring the kernel has an up-to-date view of the relation's data.

## Definition

```c
void
FlushRelationBuffers(Relation rel)
```
## Detailed Description
This function ensures that all dirty (modified) pages belonging to a specific relation are written out to disk (or more precisely, to kernel disk buffers). It handles both local buffers (for temporary relations) and shared buffers (for permanent relations) appropriately. The function performs a sequential search through the appropriate buffer pool, identifying buffers that belong to the target relation and are both valid and dirty, then flushes them to storage.

For local buffers, the function directly writes pages using smgrwrite() and handles checksums, I/O timing statistics, and error context tracking. For shared buffers, it uses the standard buffer management protocol with proper locking, pinning the buffer during the flush operation to ensure consistency.

The caller should typically hold AccessExclusiveLock on the target relation to prevent concurrent modifications that could dirty additional pages during the flush operation.

## Parameters / Member Variables
- : The Relation structure representing the relation whose buffers should be flushed to disk

## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetSmgr](../R/RelationGetSmgr.md)
  - RelationUsesLocalBuffers
  - [GetLocalBufferDescriptor](../G/GetLocalBufferDescriptor.md)
  - [BufTagMatchesRelFileLocator](../B/BufTagMatchesRelFileLocator.md)
  - [pg_atomic_read_u32](../p/pg_atomic_read_u32.md)
  - LocalBufHdrGetBlock
  - [PageSetChecksumInplace](../P/PageSetChecksumInplace.md)
  - [pgstat_prepare_io_time](../p/pgstat_prepare_io_time.md)
  - [smgrwrite](../s/smgrwrite.md)
  - [BufTagGetForkNum](../B/BufTagGetForkNum.md)
  - [pgstat_count_io_op_time](../p/pgstat_count_io_op_time.md)
  - [pg_atomic_unlocked_write_u32](../p/pg_atomic_unlocked_write_u32.md)
  - [GetBufferDescriptor](../G/GetBufferDescriptor.md)
  - [ReservePrivateRefCountEntry](../R/ReservePrivateRefCountEntry.md)
  - [ResourceOwnerEnlarge](../R/ResourceOwnerEnlarge.md)
  - [LockBufHdr](../L/LockBufHdr.md)
  - [PinBuffer_Locked](../P/PinBuffer_Locked.md)
  - [BufferDescriptorGetContentLock](../B/BufferDescriptorGetContentLock.md)
  - [FlushBuffer](FlushBuffer.md)
  - [UnpinBuffer](../U/UnpinBuffer.md)
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

## Simplified Source

```c
void FlushRelationBuffers(Relation rel) {
    int i;
    BufferDesc *bufHdr;
    SMgrRelation srel = RelationGetSmgr(rel);

    // Handle local buffers (temporary relations)
    if (RelationUsesLocalBuffers(rel)) {
        for (i = 0; i < NLocBuffer; i++) {
            bufHdr = GetLocalBufferDescriptor(i);
            uint32 buf_state = pg_atomic_read_u32(&bufHdr->state);

            // Check if buffer matches relation and is dirty
            if (BufTagMatchesRelFileLocator(&bufHdr->tag, &rel->rd_locator) &&
                (buf_state & (BM_VALID | BM_DIRTY)) == (BM_VALID | BM_DIRTY)) {

                Page localpage = (char *) LocalBufHdrGetBlock(bufHdr);

                // Set up error context for better error reporting
                ErrorContextCallback errcallback;
                errcallback.callback = local_buffer_write_error_callback;
                errcallback.arg = (void *) bufHdr;
                errcallback.previous = error_context_stack;
                error_context_stack = &errcallback;

                // Write page with checksum and I/O timing
                PageSetChecksumInplace(localpage, bufHdr->tag.blockNum);
                instr_time io_start = pgstat_prepare_io_time(track_io_timing);

                smgrwrite(srel, BufTagGetForkNum(&bufHdr->tag),
                         bufHdr->tag.blockNum, localpage, false);

                pgstat_count_io_op_time(IOOBJECT_TEMP_RELATION, IOCONTEXT_NORMAL,
                                       IOOP_WRITE, io_start, 1);

                // Clear dirty flags and update statistics
                buf_state &= ~(BM_DIRTY | BM_JUST_DIRTIED);
                pg_atomic_unlocked_write_u32(&bufHdr->state, buf_state);
                pgBufferUsage.local_blks_written++;

                error_context_stack = errcallback.previous;
            }
        }
        return;
    }

    // Handle shared buffers (permanent relations)
    for (i = 0; i < NBuffers; i++) {
        bufHdr = GetBufferDescriptor(i);

        // Quick unlocked check to avoid unnecessary work
        if (!BufTagMatchesRelFileLocator(&bufHdr->tag, &rel->rd_locator))
            continue;

        // Prepare for buffer operations
        ReservePrivateRefCountEntry();
        ResourceOwnerEnlarge(CurrentResourceOwner);

        // Lock buffer and recheck conditions
        uint32 buf_state = LockBufHdr(bufHdr);
        if (BufTagMatchesRelFileLocator(&bufHdr->tag, &rel->rd_locator) &&
            (buf_state & (BM_VALID | BM_DIRTY)) == (BM_VALID | BM_DIRTY)) {

            // Pin buffer and flush with proper locking
            PinBuffer_Locked(bufHdr);
            LWLockAcquire(BufferDescriptorGetContentLock(bufHdr), LW_SHARED);
            FlushBuffer(bufHdr, srel, IOOBJECT_RELATION, IOCONTEXT_NORMAL);
            LWLockRelease(BufferDescriptorGetContentLock(bufHdr));
            UnpinBuffer(bufHdr);
        } else {
            UnlockBufHdr(bufHdr, buf_state);
        }
    }
}
```