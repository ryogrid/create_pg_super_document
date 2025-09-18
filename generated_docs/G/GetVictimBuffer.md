# GetVictimBuffer

## Location
[src/backend/storage/buffer/bufmgr.c:1938-2103](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L1938-L2103)

## Overview
GetVictimBuffer finds and prepares a buffer to be reused by evicting its current contents, handling dirty buffers, and ensuring proper cleanup for PostgreSQL's buffer pool management.

## Definition


## Detailed Description
GetVictimBuffer is a critical function in PostgreSQL's buffer management that implements the buffer replacement policy. It selects a victim buffer from the buffer pool, handles dirty buffer writeout if necessary, and ensures the buffer is properly prepared for reuse. The function implements several safety mechanisms including deadlock avoidance for content locks, proper resource management, and coordination with buffer access strategies.

The function operates in a loop (with 'again' label) to handle cases where a selected victim buffer becomes unavailable due to concurrent access. It ensures WAL-before-data consistency by flushing dirty buffers before reuse and coordinates with PostgreSQL's I/O statistics tracking.

## Parameters / Member Variables
- : BufferAccessStrategy that guides buffer selection policy (can be NULL for default strategy)
- : IOContext that tracks the type of I/O operation for statistics and optimization

## Dependencies
- Functions called/Symbols referenced:
  - [ReservePrivateRefCountEntry](../R/ReservePrivateRefCountEntry.md)
  - ResourceOwnerEnlarge
  - StrategyGetBuffer
  - [BufferDescriptorGetBuffer](../B/BufferDescriptorGetBuffer.md)
  - PinBuffer_Locked
  - CheckBufferIsPinnedOnce
  - [BufferDescriptorGetContentLock](../B/BufferDescriptorGetContentLock.md)
  - LWLockConditionalAcquire
  - UnpinBuffer
  - LockBufHdr/UnlockBufHdr
  - BufferGetLSN
  - [XLogNeedsFlush](../X/XLogNeedsFlush.md)
  - StrategyRejectBuffer
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