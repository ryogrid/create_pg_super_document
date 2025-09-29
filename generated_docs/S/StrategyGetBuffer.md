# StrategyGetBuffer

## Location
[src/backend/storage/buffer/freelist.c:196-362](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/freelist.c#L196-L362)

## Overview
StrategyGetBuffer is the core buffer allocation function that selects the next candidate buffer for use by BufferAlloc(), implementing PostgreSQL's buffer replacement strategy using either a ring buffer strategy or the clock sweep algorithm.

## Definition

```c
BufferDesc *
StrategyGetBuffer(BufferAccessStrategy strategy, uint32 *buf_state, bool *from_ring)
```
## Detailed Description
StrategyGetBuffer implements PostgreSQL's buffer replacement policy, selecting an unused buffer for allocation. The function operates in several phases:

1. **Strategy Ring Check**: If a BufferAccessStrategy is provided, it first attempts to get a buffer from the strategy's ring buffer using GetBufferFromRing().

2. **Background Writer Notification**: If needed, it wakes up the background writer process to help with buffer cleaning by setting the appropriate process latch.

3. **Free List Processing**: It checks for buffers on the free list (firstFreeBuffer) and attempts to use them. Buffers on the free list are immediately usable without needing to evict data.

4. **Clock Sweep Algorithm**: If no free buffers are available, it runs the clock sweep algorithm using ClockSweepTick() to find victim buffers. The algorithm decrements usage counts and selects buffers with zero usage count and reference count.

The function ensures that the selected buffer is returned with its header spinlock held to prevent other processes from using it before the caller can pin it. It also maintains statistics by incrementing numBufferAllocs for bgwriter rate estimation.

## Parameters / Member Variables
- : BufferAccessStrategy object for ring buffer allocation, or NULL for default strategy
- : Output parameter returning the buffer's state value
- : Output parameter indicating whether the buffer came from a strategy ring

## Dependencies
- Functions called/Symbols referenced:
  - [GetBufferFromRing](../G/GetBufferFromRing.md)
  - INT_ACCESS_ONCE
  - [SetLatch](SetLatch.md)
  - [pg_atomic_fetch_add_u32](../p/pg_atomic_fetch_add_u32.md)
  - [GetBufferDescriptor](../G/GetBufferDescriptor.md)
  - [LockBufHdr](../L/LockBufHdr.md)/UnlockBufHdr
  - BUF_STATE_GET_REFCOUNT/BUF_STATE_GET_USAGECOUNT
  - [AddBufferToRing](../A/AddBufferToRing.md)
  - [ClockSweepTick](../C/ClockSweepTick.md)
- Called from (representative examples):
  - [GetVictimBuffer](../G/GetVictimBuffer.md)
  - [ResourceOwnerForgetBufferIO](../R/ResourceOwnerForgetBufferIO.md)

## Notes and Other Information
- The function must return with the buffer header spinlock held to ensure atomicity
- Uses lockless checks where possible to avoid unnecessary spinlock acquisition
- Implements a safety counter (trycounter) to prevent infinite loops when all buffers are pinned
- Background writer wakeup logic uses careful memory ordering to avoid race conditions
- Strategy ring buffers are not counted in numBufferAllocs statistics
- The clock sweep algorithm implements the classical buffer replacement policy with usage count management

## Simplified Source

```c
BufferDesc *StrategyGetBuffer(BufferAccessStrategy strategy, uint32 *buf_state, bool *from_ring)
{
    BufferDesc *buf;
    int bgwprocno;
    int trycounter;
    uint32 local_buf_state;

    *from_ring = false;

    // Try strategy ring buffer first
    if (strategy != NULL) {
        buf = GetBufferFromRing(strategy, buf_state);
        if (buf != NULL) {
            *from_ring = true;
            return buf;
        }
    }

    // Wake background writer if needed
    bgwprocno = INT_ACCESS_ONCE(StrategyControl->bgwprocno);
    if (bgwprocno != -1) {
        StrategyControl->bgwprocno = -1;
        SetLatch(&ProcGlobal->allProcs[bgwprocno].procLatch);
    }

    // Count buffer allocation requests
    pg_atomic_fetch_add_u32(&StrategyControl->numBufferAllocs, 1);

    // Try free list first
    if (StrategyControl->firstFreeBuffer >= 0) {
        while (true) {
            // Get buffer from free list
            SpinLockAcquire(&StrategyControl->buffer_strategy_lock);

            if (StrategyControl->firstFreeBuffer < 0) {
                SpinLockRelease(&StrategyControl->buffer_strategy_lock);
                break;
            }

            buf = GetBufferDescriptor(StrategyControl->firstFreeBuffer);
            StrategyControl->firstFreeBuffer = buf->freeNext;
            buf->freeNext = FREENEXT_NOT_IN_LIST;

            SpinLockRelease(&StrategyControl->buffer_strategy_lock);

            // Check if buffer is usable
            local_buf_state = LockBufHdr(buf);
            if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0 &&
                BUF_STATE_GET_USAGECOUNT(local_buf_state) == 0) {
                if (strategy != NULL)
                    AddBufferToRing(strategy, buf);
                *buf_state = local_buf_state;
                return buf;
            }
            UnlockBufHdr(buf, local_buf_state);
        }
    }

    // Run clock sweep algorithm
    trycounter = NBuffers;
    for (;;) {
        buf = GetBufferDescriptor(ClockSweepTick());

        local_buf_state = LockBufHdr(buf);

        if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0) {
            if (BUF_STATE_GET_USAGECOUNT(local_buf_state) != 0) {
                // Decrement usage count and continue
                local_buf_state -= BUF_USAGECOUNT_ONE;
                trycounter = NBuffers;
            } else {
                // Found usable buffer
                if (strategy != NULL)
                    AddBufferToRing(strategy, buf);
                *buf_state = local_buf_state;
                return buf;
            }
        } else if (--trycounter == 0) {
            // All buffers pinned - error
            UnlockBufHdr(buf, local_buf_state);
            elog(ERROR, "no unpinned buffers available");
        }

        UnlockBufHdr(buf, local_buf_state);
    }
}
```