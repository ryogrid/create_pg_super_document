# InvalidateVictimBuffer

## Location
[src/backend/storage/buffer/bufmgr.c:1870-1937](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L1870-L1937)

## Overview
InvalidateVictimBuffer safely invalidates a buffer selected as a victim for replacement, ensuring it can be reused while handling concurrent access scenarios.

## Definition
```c
static bool InvalidateVictimBuffer(BufferDesc *buf_hdr)
```

## Detailed Description
InvalidateVictimBuffer is a helper function for the buffer replacement mechanism that performs the delicate operation of preparing a buffer for reuse. Unlike InvalidateBuffer which is used for relation drops, this function is specifically designed for buffer replacement scenarios where a clean buffer needs to be repurposed for a different page.

**Core Functionality:**
1. **Ownership Verification**: Ensures the buffer is only pinned by the calling backend
2. **Concurrent Modification Detection**: Checks if other backends have modified or pinned the buffer
3. **Safe Invalidation**: Clears buffer metadata while maintaining proper synchronization
4. **Hash Table Cleanup**: Removes the buffer mapping from the lookup table

**Safety Mechanisms:**
- Requires the buffer to be pinned but not locked upon entry
- Verifies exclusive ownership before proceeding with invalidation
- Detects and handles cases where the buffer becomes dirty or gains additional pins
- Uses proper lock ordering to prevent deadlocks

The function returns true if invalidation succeeded (buffer can be reused) or false if the buffer is no longer suitable for replacement.

## Parameters / Member Variables
- `buf_hdr`: Pointer to the BufferDesc to be invalidated. Must have a valid tag, be pinned by exactly one backend (the caller), and not be locked upon entry.

## Dependencies
- Functions called/Symbols referenced:
  - [GetPrivateRefCount](../G/GetPrivateRefCount.md)
  - [BufferDescriptorGetBuffer](../B/BufferDescriptorGetBuffer.md)
  - [BufTableHashCode](../B/BufTableHashCode.md)
  - [BufMappingPartitionLock](../B/BufMappingPartitionLock.md)
  - [LockBufHdr](../L/LockBufHdr.md)
  - [UnlockBufHdr](../U/UnlockBufHdr.md)
  - [BufferTagsEqual](../B/BufferTagsEqual.md)
  - [ClearBufferTag](../C/ClearBufferTag.md)
  - [BufTableDelete](../B/BufTableDelete.md)
  - [pg_atomic_read_u32](../p/pg_atomic_read_u32.md)
  - BUF_STATE_GET_REFCOUNT
- Constants used:
  - LW_EXCLUSIVE
  - BM_TAG_VALID
  - BM_DIRTY
  - BM_VALID
  - BUF_FLAG_MASK
  - BUF_USAGECOUNT_MASK
- Called from (representative examples):
  - [GetVictimBuffer](../G/GetVictimBuffer.md)
  - [EvictUnpinnedBuffer](../E/EvictUnpinnedBuffer.md)

## Notes and Other Information
- Designed specifically for buffer replacement scenarios, not relation drops
- The function must be called on a buffer that is pinned but not locked
- Returns false if the buffer becomes unsuitable for replacement (e.g., dirtied by another backend)
- Clearing the buffer tag provides optimization benefits for linear buffer scans
- Multiple assertions ensure the function is called under correct conditions and verify post-condition invariants
- Critical for maintaining buffer pool integrity during high-concurrency victim selection
- The function handles the race condition where a buffer becomes dirty or gains additional pins between selection and invalidation
- Essential component of PostgreSQL's buffer replacement strategy implementation
- Optimized for performance as it's called frequently during buffer replacement operations

## Simplified Source

```c
static bool InvalidateVictimBuffer(BufferDesc *buf_hdr) {
    uint32 buf_state;
    uint32 hash;
    LWLock *partition_lock;
    BufferTag tag;

    // Verify we have exclusive pin on this buffer
    Assert(GetPrivateRefCount(BufferDescriptorGetBuffer(buf_hdr)) == 1);

    // Save buffer tag (safe to read while pinned)
    tag = buf_hdr->tag;

    // Get hash table partition lock for this buffer
    hash = BufTableHashCode(&tag);
    partition_lock = BufMappingPartitionLock(hash);
    LWLockAcquire(partition_lock, LW_EXCLUSIVE);

    // Lock buffer header to check state atomically
    buf_state = LockBufHdr(buf_hdr);

    // Verify buffer still has valid tag and is pinned
    Assert(buf_state & BM_TAG_VALID);
    Assert(BUF_STATE_GET_REFCOUNT(buf_state) > 0);
    Assert(BufferTagsEqual(&buf_hdr->tag, &tag));

    // Check if buffer is still suitable for replacement
    if (BUF_STATE_GET_REFCOUNT(buf_state) != 1 || (buf_state & BM_DIRTY)) {
        // Buffer gained additional pins or became dirty - can't use it
        UnlockBufHdr(buf_hdr, buf_state);
        LWLockRelease(partition_lock);
        return false;
    }

    // Clear buffer tag and flags to mark as invalid
    ClearBufferTag(&buf_hdr->tag);
    buf_state &= ~(BUF_FLAG_MASK | BUF_USAGECOUNT_MASK);
    UnlockBufHdr(buf_hdr, buf_state);

    // Remove buffer from hash table
    BufTableDelete(&tag, hash);
    LWLockRelease(partition_lock);

    // Verify buffer is properly invalidated
    Assert(!(buf_state & (BM_DIRTY | BM_VALID | BM_TAG_VALID)));
    Assert(BUF_STATE_GET_REFCOUNT(buf_state) > 0);

    return true; // Buffer successfully invalidated and ready for reuse
}
```