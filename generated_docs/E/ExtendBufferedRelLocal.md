# ExtendBufferedRelLocal

## Location
[src/backend/storage/buffer/localbuf.c:313-448](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/localbuf.c#L313-L448)

## Overview
ExtendBufferedRelLocal extends temporary relations by allocating new blocks and associated local buffers, serving as the local buffer implementation of ExtendBufferedRelBy() and ExtendBufferedRelTo().

## Definition

```c
BlockNumber
ExtendBufferedRelLocal(BufferManagerRelation bmr,
					   ForkNumber fork,
					   uint32 flags,
					   uint32 extend_by,
					   BlockNumber extend_upto,
					   Buffer *buffers,
					   uint32 *extended_by)
```
## Detailed Description
ExtendBufferedRelLocal implements relation extension for temporary relations using local buffers. The function performs several critical steps:

1. **Resource Management**: Limits the extension request using LimitAdditionalLocalPins() to prevent buffer exhaustion
2. **Buffer Allocation**: Obtains victim buffers through GetLocalVictimBuffer() and zero-initializes their contents
3. **Size Validation**: Checks current relation size and validates extension limits against MaxBlockNumber
4. **Hash Table Management**: For each new block, either reuses existing buffer entries or creates new hash table entries
5. **Physical Extension**: Performs actual disk space allocation via smgrzeroextend()
6. **State Finalization**: Sets BM_VALID flag on all extended buffers to mark them as ready for use

The function handles the complexity of coordinating buffer allocation, hash table updates, and physical storage extension while maintaining consistency between in-memory and on-disk state.

## Parameters
- : Buffer manager relation containing the storage manager relation handle
- : Fork number specifying which fork of the relation to extend
- : Extension flags controlling behavior (currently unused in local implementation)
- : Number of blocks to extend the relation by
- : Target block number for extension (used for validation)
- : Output array to store Buffer handles for the newly allocated blocks
- : Output parameter indicating actual number of blocks extended

## Dependencies
- Functions called/Symbols referenced:
  - [InitLocalBuffers](../I/InitLocalBuffers.md): Initializes local buffer system if needed
  - [LimitAdditionalLocalPins](../L/LimitAdditionalLocalPins.md): Limits extension size based on available pins
  - [GetLocalVictimBuffer](../G/GetLocalVictimBuffer.md): Obtains buffers for new blocks
  - [GetLocalBufferDescriptor](../G/GetLocalBufferDescriptor.md): Converts buffer IDs to BufferDesc pointers
  - LocalBufHdrGetBlock: Accesses buffer data pages
  - [smgrnblocks](../s/smgrnblocks.md): Gets current relation size in blocks
  - [InitBufferTag](../I/InitBufferTag.md)/hash_search: Manages local buffer hash table entries
  - [smgrzeroextend](../s/smgrzeroextend.md): Performs physical extension of relation on disk
  - Various buffer state management functions (PinLocalBuffer, UnpinLocalBuffer, etc.)
  - I/O statistics tracking (pgstat_prepare_io_time, pgstat_count_io_op_time)
- Called from (representative examples):
  - [ExtendBufferedRelCommon](ExtendBufferedRelCommon.md): Main relation extension function delegates to this for temporary relations
  - [ResourceOwnerForgetBufferIO](../R/ResourceOwnerForgetBufferIO.md): Buffer resource management

## Notes and Other Information
- Unlike shared relations, temporary relations don't require concurrency control during extension
- All new buffer pages are zero-initialized to ensure consistent initial state
- Includes comprehensive validation to prevent extending relations beyond PostgreSQL's maximum block limit
- Handles both new buffer allocation and reuse of existing buffers for the same blocks
- Buffer state flags are carefully managed with atomic operations to maintain consistency
- I/O timing and statistics are tracked for performance monitoring of temporary relation operations
- The function ensures resource owner tracking for all pinned buffers
- Part of PostgreSQL's buffered relation extension system optimized for temporary relation performance

## Simplified Source

```c
BlockNumber ExtendBufferedRelLocal(BufferManagerRelation bmr, ForkNumber fork,
                                  uint32 flags, uint32 extend_by, BlockNumber extend_upto,
                                  Buffer *buffers, uint32 *extended_by)
{
    BlockNumber first_block;

    // Initialize local buffer hash table if not done yet
    if (LocalBufHash == NULL)
        InitLocalBuffers();

    // Limit extension size based on available pins
    LimitAdditionalLocalPins(&extend_by);

    // Allocate victim buffers and zero-fill them
    for (uint32 i = 0; i < extend_by; i++) {
        buffers[i] = GetLocalVictimBuffer();
        BufferDesc *buf_hdr = GetLocalBufferDescriptor(-buffers[i] - 1);
        Block buf_block = LocalBufHdrGetBlock(buf_hdr);
        MemSet((char *) buf_block, 0, BLCKSZ);  // Zero-initialize new pages
    }

    // Get current relation size
    first_block = smgrnblocks(bmr.smgr, fork);

    // Validate extension parameters
    if (extend_upto != InvalidBlockNumber) {
        Assert(first_block <= extend_upto);
        Assert((uint64) first_block + extend_by <= extend_upto);
    }

    // Check for relation size limit
    if ((uint64) first_block + extend_by >= MaxBlockNumber)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                       errmsg("cannot extend relation %s beyond %u blocks",
                             relpath(bmr.smgr->smgr_rlocator, fork), MaxBlockNumber)));

    // Set up hash table entries for each new block
    for (uint32 i = 0; i < extend_by; i++) {
        int victim_buf_id = -buffers[i] - 1;
        BufferDesc *victim_buf_hdr = GetLocalBufferDescriptor(victim_buf_id);
        BufferTag tag;
        LocalBufferLookupEnt *hresult;
        bool found;

        ResourceOwnerEnlarge(CurrentResourceOwner);
        InitBufferTag(&tag, &bmr.smgr->smgr_rlocator.locator, fork, first_block + i);

        // Try to insert into hash table
        hresult = (LocalBufferLookupEnt *) hash_search(LocalBufHash, (void *) &tag, HASH_ENTER, &found);

        if (found) {
            // Block already exists, use existing buffer
            BufferDesc *existing_hdr;
            uint32 buf_state;

            UnpinLocalBuffer(BufferDescriptorGetBuffer(victim_buf_hdr));
            existing_hdr = GetLocalBufferDescriptor(hresult->id);
            PinLocalBuffer(existing_hdr, false);
            buffers[i] = BufferDescriptorGetBuffer(existing_hdr);

            // Mark buffer as invalid (will be validated after extension)
            buf_state = pg_atomic_read_u32(&existing_hdr->state);
            buf_state &= ~BM_VALID;
            pg_atomic_unlocked_write_u32(&existing_hdr->state, buf_state);
        } else {
            // New block, set up victim buffer
            uint32 buf_state = pg_atomic_read_u32(&victim_buf_hdr->state);
            victim_buf_hdr->tag = tag;
            buf_state |= BM_TAG_VALID | BUF_USAGECOUNT_ONE;
            pg_atomic_unlocked_write_u32(&victim_buf_hdr->state, buf_state);
            hresult->id = victim_buf_id;
        }
    }

    // Perform actual disk extension with I/O timing
    instr_time io_start = pgstat_prepare_io_time(track_io_timing);
    smgrzeroextend(bmr.smgr, fork, first_block, extend_by, false);
    pgstat_count_io_op_time(IOOBJECT_TEMP_RELATION, IOCONTEXT_NORMAL,
                           IOOP_EXTEND, io_start, extend_by);

    // Mark all buffers as valid
    for (uint32 i = 0; i < extend_by; i++) {
        Buffer buf = buffers[i];
        BufferDesc *buf_hdr = GetLocalBufferDescriptor(-buf - 1);
        uint32 buf_state = pg_atomic_read_u32(&buf_hdr->state);
        buf_state |= BM_VALID;
        pg_atomic_unlocked_write_u32(&buf_hdr->state, buf_state);
    }

    *extended_by = extend_by;
    pgBufferUsage.local_blks_written += extend_by;

    return first_block;
}
```