# PinBuffer_Locked

## Location
[src/backend/storage/buffer/bufmgr.c:2752-2794](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L2752-L2794)

## Overview
PinBuffer_Locked pins a buffer while the caller already holds the buffer header spinlock, providing an optimized path for pinning when the lock is already acquired and no preexisting pin exists.

## Definition
```c
static void PinBuffer_Locked(BufferDesc *buf)
```

## Detailed Description
This specialized function pins a buffer when the caller already holds the buffer header spinlock. It is designed for scenarios where the buffer is known to not have any preexisting pins by the current backend, allowing it to skip the expensive private reference count array and hash table searches that would normally be required.

The function performs the pinning operation in one atomic step: it increments the buffer's reference count and releases the spinlock simultaneously using UnlockBufHdr(). This approach is frequently mandatory rather than just an optimization, because it ensures the buffer state cannot change between the pin operation and the spinlock release.

Unlike the general PinBuffer() function, this variant does not modify the buffer's usage count and does not perform BM_VALID checks, leaving such concerns to the caller. It creates a new private reference count entry since no preexisting pin is expected.

## Parameters / Member Variables
- `buf`: Buffer descriptor to pin (caller must already hold its spinlock)

## Dependencies
- Functions called/Symbols referenced:
  - [GetPrivateRefCountEntry](../G/GetPrivateRefCountEntry.md): Assertion to verify no preexisting pin exists
  - [BufferDescriptorGetBuffer](../B/BufferDescriptorGetBuffer.md): Converts buffer descriptor to Buffer ID
  - BufHdrGetBlock: Gets block data for Valgrind instrumentation
  - VALGRIND_MAKE_MEM_DEFINED: Valgrind memory debugging support
  - [pg_atomic_read_u32](../p/pg_atomic_read_u32.md): Atomic read of buffer state
  - [UnlockBufHdr](../U/UnlockBufHdr.md): Atomically updates state and releases spinlock
  - [NewPrivateRefCountEntry](../N/NewPrivateRefCountEntry.md): Creates new private reference count entry
  - [ResourceOwnerRememberBuffer](../R/ResourceOwnerRememberBuffer.md): Tracks buffer ownership for cleanup
  - BM_LOCKED: Buffer state flag for locked status
  - BUF_REFCOUNT_ONE: Constant for incrementing reference count
- Called from (representative examples):
  - BufferIsPinned: Buffer status checking operations
  - [ReadRecentBuffer](../R/ReadRecentBuffer.md): Recent buffer access optimization
  - [GetVictimBuffer](../G/GetVictimBuffer.md): Buffer replacement victim selection
  - [SyncOneBuffer](../S/SyncOneBuffer.md): Individual buffer synchronization
  - [FlushRelationBuffers](../F/FlushRelationBuffers.md): Relation-specific buffer flushing
  - [FlushRelationsAllBuffers](../F/FlushRelationsAllBuffers.md): Multi-relation buffer flushing
  - [FlushDatabaseBuffers](../F/FlushDatabaseBuffers.md): Database-wide buffer flushing
  - [EvictUnpinnedBuffer](../E/EvictUnpinnedBuffer.md): Buffer eviction operations

## Notes and Other Information
- Requires caller to already hold the buffer header spinlock
- Assumes no preexisting pin by the current backend exists
- Must call ReservePrivateRefCountEntry() and ResourceOwnerEnlarge() before use
- Does not modify buffer usage count unlike PinBuffer()
- Does not perform BM_VALID checks - caller's responsibility
- Uses optimized path that skips private reference count array searches
- Releases the spinlock as part of the pin operation for atomicity
- Often mandatory rather than optional to prevent race conditions
- The function is static (internal to bufmgr.c) and not directly callable by external code
- Integrates with Valgrind for memory debugging support
- Designed for high-performance scenarios where lock is already held

## Simplified Source

```c
// Simplified version of PinBuffer_Locked
static void PinBuffer_Locked(BufferDesc *buf) {
    Buffer buffer;
    PrivateRefCountEntry *ref;
    uint32 buf_state;

    // Mark buffer page as defined for Valgrind debugging
    VALGRIND_MAKE_MEM_DEFINED(BufHdrGetBlock(buf), BLCKSZ);

    // Atomically increment reference count and release spinlock
    buf_state = pg_atomic_read_u32(&buf->state);
    buf_state += BUF_REFCOUNT_ONE;
    UnlockBufHdr(buf, buf_state);

    // Get buffer identifier and create private reference count entry
    buffer = BufferDescriptorGetBuffer(buf);
    ref = NewPrivateRefCountEntry(buffer);
    ref->refcount++;

    // Track buffer ownership for resource cleanup
    ResourceOwnerRememberBuffer(CurrentResourceOwner, buffer);
}
```

Key simplifications made:
- Removed detailed assertions and extensive comments
- Simplified the atomic buffer state update logic
- Focused on the core pinning operation
- Maintained essential reference counting and resource tracking
- Preserved Valgrind integration and atomic operations