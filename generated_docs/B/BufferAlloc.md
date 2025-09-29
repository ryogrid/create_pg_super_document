# BufferAlloc

## Location
[src/backend/storage/buffer/bufmgr.c:1594-1771](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L1594-L1771)

## Overview
BufferAlloc handles the core buffer allocation logic for shared buffers, including lookup, victim selection, and buffer pool management without performing actual I/O operations.

## Definition
```c
static pg_attribute_always_inline BufferDesc *
BufferAlloc(SMgrRelation smgr, char relpersistence, ForkNumber forkNum,
            BlockNumber blockNum, BufferAccessStrategy strategy,
            bool *foundPtr, IOContext io_context)
```

## Detailed Description
BufferAlloc is a critical subroutine in PostgreSQL's buffer management system that handles the complex logic of finding or allocating buffers for database pages. The function implements a sophisticated two-phase approach:

**Phase 1 - Buffer Lookup:**
- Creates a buffer tag to uniquely identify the requested page
- Performs hash table lookup to check if the buffer already exists
- If found, pins the buffer and returns it immediately

**Phase 2 - Buffer Allocation:**
- If not found, acquires a victim buffer through the replacement strategy
- Attempts to insert the new buffer mapping into the hash table
- Handles race conditions where another process may have inserted the same buffer
- Properly initializes the victim buffer with the new tag and metadata

The function is designed to handle high concurrency scenarios and includes robust error handling for race conditions. It coordinates with the buffer replacement strategy, manages buffer reference counts, and ensures proper locking protocols.

## Parameters / Member Variables
- `smgr`: Storage manager relation for the target file
- `relpersistence`: Persistence level (permanent, temporary, or unlogged)
- `forkNum`: Fork number identifying which fork of the relation (main, FSM, VM, init)
- `blockNum`: Block number within the fork to allocate
- `strategy`: Buffer replacement strategy (NULL for default strategy)
- `foundPtr`: Output parameter indicating whether buffer was found (true) or allocated (false)
- `io_context`: I/O context for statistics tracking (output parameter)

## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerEnlarge](../R/ResourceOwnerEnlarge.md)
  - [ReservePrivateRefCountEntry](../R/ReservePrivateRefCountEntry.md)
  - [InitBufferTag](../I/InitBufferTag.md)
  - [BufTableHashCode](BufTableHashCode.md)
  - [BufMappingPartitionLock](BufMappingPartitionLock.md)
  - [BufTableLookup](BufTableLookup.md)
  - [BufTableInsert](BufTableInsert.md)
  - [GetBufferDescriptor](../G/GetBufferDescriptor.md)
  - [GetVictimBuffer](../G/GetVictimBuffer.md)
  - [PinBuffer](../P/PinBuffer.md)
  - [UnpinBuffer](../U/UnpinBuffer.md)
  - [StrategyFreeBuffer](../S/StrategyFreeBuffer.md)
  - [LockBufHdr](../L/LockBufHdr.md)
  - [UnlockBufHdr](../U/UnlockBufHdr.md)
- Constants used:
  - LW_SHARED
  - BM_TAG_VALID
  - BM_VALID
  - BM_DIRTY
  - BM_IO_IN_PROGRESS
  - BM_PERMANENT
  - BUF_USAGECOUNT_ONE
  - RELPERSISTENCE_PERMANENT
  - INIT_FORKNUM
- Called from (representative examples):
  - [PinBufferForBlock](../P/PinBufferForBlock.md)

## Notes and Other Information
- This function is marked as pg_attribute_always_inline for performance optimization
- Does NOT perform actual I/O - only handles buffer pool management
- Implements sophisticated concurrency control to handle multiple backends accessing the same pages
- The function handles three main scenarios: buffer hit, buffer miss with successful allocation, and buffer miss with collision
- Race condition handling ensures that if two backends try to read the same page simultaneously, only one will perform the I/O
- Critical for PostgreSQL's buffer management performance and correctness
- The victim buffer selection and eviction logic is delegated to GetVictimBuffer()
- Proper resource management ensures no buffer leaks even in error conditions
- The BM_PERMANENT flag is set based on relation persistence and fork type to control checkpoint behavior

## Simplified Source

```c
static pg_attribute_always_inline BufferDesc *
BufferAlloc(SMgrRelation smgr, char relpersistence, ForkNumber forkNum,
            BlockNumber blockNum, BufferAccessStrategy strategy,
            bool *foundPtr, IOContext io_context) {
    BufferTag newTag;
    uint32 newHash;
    LWLock *newPartitionLock;
    int existing_buf_id;

    // Prepare for buffer allocation
    ResourceOwnerEnlarge(CurrentResourceOwner);
    ReservePrivateRefCountEntry();

    // Create buffer tag and calculate hash
    InitBufferTag(&newTag, &smgr->smgr_rlocator.locator, forkNum, blockNum);
    newHash = BufTableHashCode(&newTag);
    newPartitionLock = BufMappingPartitionLock(newHash);

    // Phase 1: Check if buffer already exists
    LWLockAcquire(newPartitionLock, LW_SHARED);
    existing_buf_id = BufTableLookup(&newTag, newHash);
    if (existing_buf_id >= 0) {
        // Buffer found - pin it and return
        BufferDesc *buf = GetBufferDescriptor(existing_buf_id);
        bool valid = PinBuffer(buf, strategy);
        LWLockRelease(newPartitionLock);

        *foundPtr = valid;  // Set to false if buffer needs I/O
        return buf;
    }

    // Phase 2: Buffer not found - allocate new one
    LWLockRelease(newPartitionLock);

    // Get victim buffer from replacement strategy
    Buffer victim_buffer = GetVictimBuffer(strategy, io_context);
    BufferDesc *victim_buf_hdr = GetBufferDescriptor(victim_buffer - 1);

    // Try to insert new buffer mapping
    LWLockAcquire(newPartitionLock, LW_EXCLUSIVE);
    existing_buf_id = BufTableInsert(&newTag, newHash, victim_buf_hdr->buf_id);

    if (existing_buf_id >= 0) {
        // Collision - someone else inserted the buffer
        UnpinBuffer(victim_buf_hdr);
        StrategyFreeBuffer(victim_buf_hdr);

        // Use the already-inserted buffer
        BufferDesc *existing_buf_hdr = GetBufferDescriptor(existing_buf_id);
        bool valid = PinBuffer(existing_buf_hdr, strategy);
        LWLockRelease(newPartitionLock);

        *foundPtr = valid;
        return existing_buf_hdr;
    }

    // Successfully allocated - initialize victim buffer
    uint32 victim_buf_state = LockBufHdr(victim_buf_hdr);
    victim_buf_hdr->tag = newTag;

    // Set buffer flags based on relation persistence
    victim_buf_state |= BM_TAG_VALID | BUF_USAGECOUNT_ONE;
    if (relpersistence == RELPERSISTENCE_PERMANENT || forkNum == INIT_FORKNUM)
        victim_buf_state |= BM_PERMANENT;

    UnlockBufHdr(victim_buf_hdr, victim_buf_state);
    LWLockRelease(newPartitionLock);

    *foundPtr = false;  // Buffer allocated but needs I/O
    return victim_buf_hdr;
}
```