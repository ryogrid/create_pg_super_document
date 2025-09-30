# ExtendBufferedRelShared

## Location
[src/backend/storage/buffer/bufmgr.c:2179-2458](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L2179-L2458)

## Overview
ExtendBufferedRelShared extends shared persistent relations by acquiring victim buffers, coordinating with extension locks, and managing buffer table insertions for concurrent access.

## Definition

```c
static BlockNumber
ExtendBufferedRelShared(BufferManagerRelation bmr,
						ForkNumber fork,
						BufferAccessStrategy strategy,
						uint32 flags,
						uint32 extend_by,
						BlockNumber extend_upto,
						Buffer *buffers,
						uint32 *extended_by)
```
## Detailed Description
ExtendBufferedRelShared implements the complex logic for extending shared (persistent) relations in PostgreSQL. It operates in several phases: first acquiring victim buffers and zeroing them outside the extension lock to minimize lock hold time; then taking the extension lock and determining the actual extension size based on concurrent changes; inserting buffers into the buffer mapping table; performing the actual storage extension via smgrzeroextend; and finally marking buffers as valid and waking waiting backends.

The function handles several edge cases including concurrent extensions, existing buffers from failed previous attempts, and enforces relation size limits. It coordinates with buffer access strategies for victim buffer selection and includes comprehensive error handling for corrupted data scenarios. The implementation optimizes performance by doing expensive operations (victim buffer writeout, zeroing) before acquiring locks.

## Parameters / Member Variables
- : BufferManagerRelation containing relation metadata and storage manager
- : ForkNumber specifying which fork of the relation to extend (main, FSM, VM, etc.)
- : BufferAccessStrategy for buffer management policy and victim selection
- : uint32 controlling extension behavior (EB_SKIP_EXTENSION_LOCK, EB_CLEAR_SIZE_CACHE, EB_LOCK_FIRST, EB_LOCK_TARGET)
- : uint32 specifying the number of blocks to extend by (modified by LimitAdditionalPins)
- : BlockNumber specifying target block number to extend up to (InvalidBlockNumber for unlimited)
- : Buffer array to receive handles for newly allocated blocks
- : Pointer to uint32 that receives the actual number of blocks extended

## Dependencies
- Functions called/Symbols referenced:
  - [IOContextForStrategy](../I/IOContextForStrategy.md)
  - [LimitAdditionalPins](../L/LimitAdditionalPins.md)
  - [GetVictimBuffer](../G/GetVictimBuffer.md)
  - BufHdrGetBlock/GetBufferDescriptor
  - MemSet
  - [LockRelationForExtension](../L/LockRelationForExtension.md)/UnlockRelationForExtension
  - [smgrnblocks](../s/smgrnblocks.md)/smgrzeroextend
  - [BufTableInsert](../B/BufTableInsert.md)/BufTableHashCode
  - [BufferDescriptorGetContentLock](../B/BufferDescriptorGetContentLock.md)
  - [PinBuffer](../P/PinBuffer.md)/UnpinBuffer
  - [StartBufferIO](../S/StartBufferIO.md)/TerminateBufferIO
  - [StrategyFreeBuffer](../S/StrategyFreeBuffer.md)
  - [pgstat_count_io_op_time](../p/pgstat_count_io_op_time.md)
  - [PageIsNew](../P/PageIsNew.md)
- Called from (representative examples):
  - [ExtendBufferedRelCommon](ExtendBufferedRelCommon.md)

## Notes and Other Information
- Handles concurrent extension scenarios by rechecking relation size after acquiring lock
- Implements deadlock avoidance by doing expensive operations before lock acquisition  
- Supports partial extensions when extend_upto parameter limits the final size
- Enforces MaxBlockNumber limit to prevent relation overflow
- Includes comprehensive error handling for unexpected data beyond EOF
- Optimizes performance through careful lock ordering and batched operations
- Critical component of PostgreSQL's relation extension and buffer management system

## Simplified Source

```c
static BlockNumber ExtendBufferedRelShared(BufferManagerRelation bmr,
                                         ForkNumber fork,
                                         BufferAccessStrategy strategy,
                                         uint32 flags,
                                         uint32 extend_by,
                                         BlockNumber extend_upto,
                                         Buffer *buffers,
                                         uint32 *extended_by) {
    BlockNumber first_block;
    IOContext io_context = IOContextForStrategy(strategy);

    LimitAdditionalPins(&extend_by);

    // Phase 1: Acquire and zero victim buffers before taking extension lock
    for (uint32 i = 0; i < extend_by; i++) {
        buffers[i] = GetVictimBuffer(strategy, io_context);
        Block buf_block = BufHdrGetBlock(GetBufferDescriptor(buffers[i] - 1));
        MemSet((char *) buf_block, 0, BLCKSZ);  // Zero-fill new buffers
    }

    // Phase 2: Take extension lock and determine actual extension size
    if (!(flags & EB_SKIP_EXTENSION_LOCK))
        LockRelationForExtension(bmr.rel, ExclusiveLock);

    if (flags & EB_CLEAR_SIZE_CACHE)
        bmr.smgr->smgr_cached_nblocks[fork] = InvalidBlockNumber;

    first_block = smgrnblocks(bmr.smgr, fork);

    // Adjust extension size based on extend_upto limit and concurrent changes
    if (extend_upto != InvalidBlockNumber) {
        uint32 orig_extend_by = extend_by;
        if (first_block >= extend_upto)
            extend_by = 0;
        else if ((uint64) first_block + extend_by > extend_upto)
            extend_by = extend_upto - first_block;

        // Release excess buffers
        for (uint32 i = extend_by; i < orig_extend_by; i++) {
            BufferDesc *buf_hdr = GetBufferDescriptor(buffers[i] - 1);
            StrategyFreeBuffer(buf_hdr);
            UnpinBuffer(buf_hdr);
        }

        if (extend_by == 0) {
            if (!(flags & EB_SKIP_EXTENSION_LOCK))
                UnlockRelationForExtension(bmr.rel, ExclusiveLock);
            *extended_by = 0;
            return first_block;
        }
    }

    // Check relation size limit
    if ((uint64) first_block + extend_by >= MaxBlockNumber)
        ereport(ERROR, "cannot extend relation beyond maximum blocks");

    // Phase 3: Insert buffers into buffer table
    for (uint32 i = 0; i < extend_by; i++) {
        Buffer victim_buf = buffers[i];
        BufferDesc *victim_buf_hdr = GetBufferDescriptor(victim_buf - 1);
        BufferTag tag;
        uint32 hash;
        LWLock *partition_lock;

        InitBufferTag(&tag, &bmr.smgr->smgr_rlocator.locator, fork, first_block + i);
        hash = BufTableHashCode(&tag);
        partition_lock = BufMappingPartitionLock(hash);

        LWLockAcquire(partition_lock, LW_EXCLUSIVE);
        int existing_id = BufTableInsert(&tag, hash, victim_buf_hdr->buf_id);

        if (existing_id >= 0) {
            // Handle existing buffer (corner case from failed previous extension)
            BufferDesc *existing_hdr = GetBufferDescriptor(existing_id);
            PinBuffer(existing_hdr, strategy);
            LWLockRelease(partition_lock);

            StrategyFreeBuffer(victim_buf_hdr);
            UnpinBuffer(victim_buf_hdr);
            buffers[i] = BufferDescriptorGetBuffer(existing_hdr);

            // Prepare existing buffer for extension
            uint32 buf_state = LockBufHdr(existing_hdr);
            buf_state &= ~BM_VALID;
            UnlockBufHdr(existing_hdr, buf_state);
            StartBufferIO(existing_hdr, true, false);
        } else {
            // Set up new buffer
            uint32 buf_state = LockBufHdr(victim_buf_hdr);
            victim_buf_hdr->tag = tag;
            buf_state |= BM_TAG_VALID | BUF_USAGECOUNT_ONE;
            if (bmr.relpersistence == RELPERSISTENCE_PERMANENT || fork == INIT_FORKNUM)
                buf_state |= BM_PERMANENT;
            UnlockBufHdr(victim_buf_hdr, buf_state);
            LWLockRelease(partition_lock);
            StartBufferIO(victim_buf_hdr, true, false);
        }
    }

    // Phase 4: Extend storage and finalize buffers
    smgrzeroextend(bmr.smgr, fork, first_block, extend_by, false);

    if (!(flags & EB_SKIP_EXTENSION_LOCK))
        UnlockRelationForExtension(bmr.rel, ExclusiveLock);

    // Mark buffers valid and optionally lock them
    for (uint32 i = 0; i < extend_by; i++) {
        BufferDesc *buf_hdr = GetBufferDescriptor(buffers[i] - 1);
        bool lock = (flags & EB_LOCK_FIRST && i == 0) ||
                   (flags & EB_LOCK_TARGET && first_block + i + 1 == extend_upto);

        if (lock)
            LWLockAcquire(BufferDescriptorGetContentLock(buf_hdr), LW_EXCLUSIVE);

        TerminateBufferIO(buf_hdr, false, BM_VALID, true);
    }

    *extended_by = extend_by;
    return first_block;
}
```