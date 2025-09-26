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