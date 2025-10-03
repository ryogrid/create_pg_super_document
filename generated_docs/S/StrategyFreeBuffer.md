# StrategyFreeBuffer

## Location
[src/backend/storage/buffer/freelist.c:363-393](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/freelist.c#L363-L393)

## Overview
StrategyFreeBuffer adds a buffer to the free list, making it available for immediate reuse by future buffer allocation requests.

## Definition

```c
void
StrategyFreeBuffer(BufferDesc *buf)
```
## Detailed Description
StrategyFreeBuffer puts a buffer onto the front of the free buffer list maintained by StrategyControl. The function operates under the protection of the buffer_strategy_lock spinlock to ensure thread-safe manipulation of the free list data structures.

The function implements a singly-linked list using the freeNext field in buffer descriptors, with special handling to prevent duplicate entries. It checks if the buffer is already in the free list (freeNext != FREENEXT_NOT_IN_LIST) and only adds it if it's not already present.

When adding a buffer to the free list, it becomes the new head (firstFreeBuffer), and the previous head becomes its successor. If the free list was empty before adding this buffer, it also updates lastFreeBuffer to point to this buffer since it becomes both the first and last element.

## Parameters / Member Variables
- `*buf`: Pointer to the BufferDesc structure to be added to the free list
## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire
  - SpinLockRelease
  - FREENEXT_NOT_IN_LIST (constant)
  - StrategyControl (global buffer strategy control structure)
- Called from (representative examples):
  - [BufferAlloc](../B/BufferAlloc.md)
  - [InvalidateBuffer](../I/InvalidateBuffer.md)
  - [ExtendBufferedRelShared](../E/ExtendBufferedRelShared.md)
  - [ResourceOwnerForgetBufferIO](../R/ResourceOwnerForgetBufferIO.md)

## Notes and Other Information
- Uses spinlock protection to ensure atomic updates to the free list structure
- Implements duplicate detection to prevent corruption of the free list
- Buffers added to the free list are immediately available for allocation by StrategyGetBuffer
- The free list is implemented as a LIFO (Last In, First Out) structure for simplicity
- Maintains both firstFreeBuffer and lastFreeBuffer pointers for efficient list management

## Simplified Source

```c
void StrategyFreeBuffer(BufferDesc *buf)
{
    // Acquire lock to protect free list manipulation
    SpinLockAcquire(&StrategyControl->buffer_strategy_lock);

    // Only add buffer if it's not already in the free list
    if (buf->freeNext == FREENEXT_NOT_IN_LIST) {
        // Add buffer to front of free list
        buf->freeNext = StrategyControl->firstFreeBuffer;

        // If list was empty, this buffer becomes the last one too
        if (buf->freeNext < 0)
            StrategyControl->lastFreeBuffer = buf->buf_id;

        // Update first buffer pointer
        StrategyControl->firstFreeBuffer = buf->buf_id;
    }

    SpinLockRelease(&StrategyControl->buffer_strategy_lock);
}
```