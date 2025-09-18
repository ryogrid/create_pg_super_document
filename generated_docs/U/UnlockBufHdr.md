# UnlockBufHdr

## Location
src/include/storage/buf_internals.h: 362 - 375

## Overview
Releases the buffer header lock by atomically clearing the BM_LOCKED flag from the buffer's state, ensuring proper memory ordering with a write barrier.

## Definition
```c
static inline void UnlockBufHdr(BufferDesc *desc, uint32 buf_state)
```

## Detailed Description
UnlockBufHdr is a static inline function that releases the lock on a buffer header by atomically updating the buffer's state. It performs two critical operations: first, it executes a write barrier to ensure that all previous memory writes are completed and visible before the unlock operation, and second, it atomically writes the new state with the BM_LOCKED flag cleared.

The function takes the desired buffer state as a parameter and ensures that the BM_LOCKED bit is cleared regardless of its value in the input state. This design allows callers to specify the complete new state while guaranteeing that the lock bit is properly cleared. The use of pg_atomic_write_u32 ensures that the state update is atomic and visible to other processes immediately.

The write barrier (pg_write_barrier) is crucial for ensuring memory ordering - it guarantees that any modifications made while holding the buffer header lock are committed to memory before the lock is released, preventing race conditions and ensuring data consistency in the shared buffer pool.

## Parameters / Member Variables
- `desc`: Pointer to the BufferDesc structure whose header lock is to be released
- `buf_state`: The desired new state for the buffer, with BM_LOCKED flag automatically cleared

## Dependencies
- Functions called/Symbols referenced:
  - [BufferDesc](../B/BufferDesc.md) (structure type)
  - pg_write_barrier (memory barrier function)
  - [pg_atomic_write_u32](../p/pg_atomic_write_u32.md) (atomic write operation)
  - BM_LOCKED (buffer state flag constant)
- Called from (representative examples):
  - [BufferAlloc](../B/BufferAlloc.md)
  - [InvalidateBuffer](../I/InvalidateBuffer.md)
  - [GetVictimBuffer](../G/GetVictimBuffer.md)
  - PinBuffer_Locked
  - UnpinBufferNoOwner
  - SyncOneBuffer
  - [FlushBuffer](../F/FlushBuffer.md)
  - StartBufferIO
  - TerminateBufferIO

## Notes and Other Information
- This function must only be called when the caller actually holds the buffer header lock
- The write barrier ensures proper memory ordering in multiprocessor systems
- The atomic write operation makes the unlock visible to other processes immediately
- This function is complementary to LockBufHdr and is used extensively throughout buffer management operations
- The function is performance-critical and is implemented as a static inline for efficiency
- Used in various buffer lifecycle operations including allocation, invalidation, I/O operations, and cleanup