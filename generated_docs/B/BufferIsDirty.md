# BufferIsDirty

## Location
[src/backend/storage/buffer/bufmgr.c:2488-2519](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L2488-L2519)

## Overview
BufferIsDirty checks whether a buffer has been modified (marked as dirty) and needs to be written to storage during the next checkpoint or when evicted from the buffer pool.

## Definition
```c
bool BufferIsDirty(Buffer buffer)
```

## Detailed Description
This function determines if a buffer contains modified data that has not yet been written to disk. The function works with both shared buffers (managed by the buffer manager) and local buffers (used for temporary tables). It performs an atomic read of the buffer's state flags to check the BM_DIRTY bit.

The function requires that the buffer be pinned (to prevent it from being evicted) and that the caller holds an exclusive content lock on the buffer. These preconditions ensure that the dirty state cannot change while the check is being performed, providing a consistent and reliable result.

## Parameters / Member Variables
- `buffer`: Buffer identifier to check for dirty status. Can be either a shared buffer (positive value) or local buffer (negative value)

## Dependencies
- Functions called/Symbols referenced:
  - BufferIsLocal: Determines if buffer is a local buffer
  - [GetLocalBufferDescriptor](../G/GetLocalBufferDescriptor.md): Gets descriptor for local buffers
  - [GetBufferDescriptor](../G/GetBufferDescriptor.md): Gets descriptor for shared buffers  
  - BufferIsPinned: Assertion to verify buffer is pinned
  - [LWLockHeldByMeInMode](../L/LWLockHeldByMeInMode.md): Assertion to verify exclusive lock is held
  - [BufferDescriptorGetContentLock](BufferDescriptorGetContentLock.md): Gets the content lock for the buffer
  - [pg_atomic_read_u32](../p/pg_atomic_read_u32.md): Atomic read of buffer state
  - BM_DIRTY: Buffer state flag indicating dirty status
- Called from (representative examples):
  - [XLogRegisterBuffer](../X/XLogRegisterBuffer.md): WAL logging system uses this to check buffer state
  - BUFFER_LOCK_EXCLUSIVE: Buffer locking macros

## Notes and Other Information
- The buffer must be pinned before calling this function to prevent race conditions
- An exclusive content lock must be held to ensure the result remains valid
- The function uses atomic operations to read the buffer state, ensuring thread safety
- Local buffers (for temporary tables) and shared buffers are handled differently but use the same state checking mechanism
- This is a lightweight operation that only reads the buffer state without modifying it