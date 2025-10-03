# GetVictimBuffer

## Location
[src/backend/storage/buffer/bufmgr.c:1938-2103](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L1938-L2103)

## Overview
GetVictimBuffer finds and prepares a buffer to be reused by evicting its current contents, handling dirty buffers, and ensuring proper cleanup for PostgreSQL's buffer pool management.

## Definition

```c
static Buffer
GetVictimBuffer(BufferAccessStrategy strategy, IOContext io_context)
```
## Detailed Description
GetVictimBuffer is a critical function in PostgreSQL's buffer management that implements the buffer replacement policy. It selects a victim buffer from the buffer pool, handles dirty buffer writeout if necessary, and ensures the buffer is properly prepared for reuse. The function implements several safety mechanisms including deadlock avoidance for content locks, proper resource management, and coordination with buffer access strategies.

The function operates in a loop (with 'again' label) to handle cases where a selected victim buffer becomes unavailable due to concurrent access. It ensures WAL-before-data consistency by flushing dirty buffers before reuse and coordinates with PostgreSQL's I/O statistics tracking.

## Parameters / Member Variables
- `strategy`: BufferAccessStrategy that guides buffer selection policy (can be NULL for default strategy)
- `io_context`: IOContext that tracks the type of I/O operation for statistics and optimization
## Dependencies
- Functions called/Symbols referenced:
  - [ReservePrivateRefCountEntry](../R/ReservePrivateRefCountEntry.md)
  - [ResourceOwnerEnlarge](../R/ResourceOwnerEnlarge.md)
  - [StrategyGetBuffer](../S/StrategyGetBuffer.md)
  - [BufferDescriptorGetBuffer](../B/BufferDescriptorGetBuffer.md)
  - [PinBuffer_Locked](../P/PinBuffer_Locked.md)
  - [CheckBufferIsPinnedOnce](../C/CheckBufferIsPinnedOnce.md)
  - [BufferDescriptorGetContentLock](../B/BufferDescriptorGetContentLock.md)
  - [LWLockConditionalAcquire](../L/LWLockConditionalAcquire.md)
  - [UnpinBuffer](../U/UnpinBuffer.md)
  - [LockBufHdr](../L/LockBufHdr.md)/UnlockBufHdr
  - BufferGetLSN
  - [XLogNeedsFlush](../X/XLogNeedsFlush.md)
  - [StrategyRejectBuffer](../S/StrategyRejectBuffer.md)
  - [FlushBuffer](../F/FlushBuffer.md)
  - [ScheduleBufferTagForWriteback](../S/ScheduleBufferTagForWriteback.md)
  - [pgstat_count_io_op](../p/pgstat_count_io_op.md)
  - [InvalidateVictimBuffer](../I/InvalidateVictimBuffer.md)
- Called from (representative examples):
  - [BufferAlloc](../B/BufferAlloc.md)
  - [ExtendBufferedRelShared](../E/ExtendBufferedRelShared.md)

## Notes and Other Information
- Uses conditional locking to avoid deadlocks when acquiring content locks for dirty buffer writeout
- Implements retry logic (goto again) when victim buffers become unavailable
- Coordinates with buffer access strategies to optimize I/O patterns
- Tracks buffer eviction and reuse statistics for monitoring
- Ensures proper resource cleanup and maintains buffer pool invariants
- Critical for preventing buffer pool exhaustion and maintaining system performance

## Simplified Source

```c
static Buffer GetVictimBuffer(BufferAccessStrategy strategy, IOContext io_context) {
    BufferDesc *buf_hdr;
    Buffer buf;
    uint32 buf_state;
    bool from_ring;

    // Reserve resources before acquiring locks
    ReservePrivateRefCountEntry();
    ResourceOwnerEnlarge(CurrentResourceOwner);

again:
    // Select a victim buffer using the strategy
    buf_hdr = StrategyGetBuffer(strategy, &buf_state, &from_ring);
    buf = BufferDescriptorGetBuffer(buf_hdr);

    Assert(BUF_STATE_GET_REFCOUNT(buf_state) == 0);

    // Pin the buffer and verify single pin
    PinBuffer_Locked(buf_hdr);
    CheckBufferIsPinnedOnce(buf);

    // Handle dirty buffers - write them out first
    if (buf_state & BM_DIRTY) {
        LWLock *content_lock = BufferDescriptorGetContentLock(buf_hdr);

        // Use conditional lock to avoid deadlocks
        if (!LWLockConditionalAcquire(content_lock, LW_SHARED)) {
            UnpinBuffer(buf_hdr);
            goto again;  // Try another buffer
        }

        // Check if strategy wants to reject this buffer due to WAL flush cost
        if (strategy != NULL) {
            buf_state = LockBufHdr(buf_hdr);
            XLogRecPtr lsn = BufferGetLSN(buf_hdr);
            UnlockBufHdr(buf_hdr, buf_state);

            if (XLogNeedsFlush(lsn) && StrategyRejectBuffer(strategy, buf_hdr, from_ring)) {
                LWLockRelease(content_lock);
                UnpinBuffer(buf_hdr);
                goto again;
            }
        }

        // Write the dirty buffer
        FlushBuffer(buf_hdr, NULL, IOOBJECT_RELATION, io_context);
        LWLockRelease(content_lock);

        ScheduleBufferTagForWriteback(&BackendWritebackContext, io_context, &buf_hdr->tag);
    }

    // Track I/O statistics
    if (buf_state & BM_VALID) {
        pgstat_count_io_op(IOOBJECT_RELATION, io_context,
                          from_ring ? IOOP_REUSE : IOOP_EVICT);
    }

    // Remove buffer from mapping table
    if ((buf_state & BM_TAG_VALID) && !InvalidateVictimBuffer(buf_hdr)) {
        UnpinBuffer(buf_hdr);
        goto again;
    }

    // Final safety checks
    Assert(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&buf_hdr->state)) == 1);
    CheckBufferIsPinnedOnce(buf);

    return buf;
}
```