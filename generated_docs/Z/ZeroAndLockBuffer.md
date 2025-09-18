# ZeroAndLockBuffer

## Location
[src/backend/storage/buffer/bufmgr.c:1018-1104](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L1018-L1104)

## Overview
A static function that zeros a buffer page if needed and locks it, implementing the core functionality for RBM_ZERO_AND_LOCK and RBM_ZERO_AND_CLEANUP_LOCK buffer reading modes.

## Definition
```c
static void ZeroAndLockBuffer(Buffer buffer, ReadBufferMode mode, bool already_valid)
```

## Detailed Description
ZeroAndLockBuffer is a critical internal function that implements the zero-and-lock semantics for buffer pages. It conditionally zeros a buffer page if it is not already valid and ensures the buffer is properly locked before returning control to the caller. This function is essential for ensuring that newly allocated or extended buffer pages start with clean, predictable content.

The function handles both shared and local buffers with different locking strategies. For shared buffers, it uses the BM_IO_IN_PROGRESS mechanism to coordinate with other backends, while for local buffers it uses simpler atomic operations. The function carefully coordinates between zeroing the page content and marking the buffer as valid to prevent other backends from seeing partially initialized pages.

When a buffer is already valid, the function simply acquires the appropriate lock without zeroing, satisfying the caller's expectation that the buffer will be locked upon return.

## Parameters / Member Variables
- `buffer`: Buffer identifier for the buffer to zero and lock (must already be pinned)
- `mode`: ReadBufferMode specifying the type of lock to acquire (RBM_ZERO_AND_LOCK or RBM_ZERO_AND_CLEANUP_LOCK)
- `already_valid`: Boolean indicating whether the caller knows the buffer is already valid (optimization to skip validity checks)

## Dependencies
- Functions called/Symbols referenced:
  - BufferIsLocal
  - [GetLocalBufferDescriptor](../G/GetLocalBufferDescriptor.md)
  - [GetBufferDescriptor](../G/GetBufferDescriptor.md)
  - [pg_atomic_read_u32](../p/pg_atomic_read_u32.md)
  - [pg_atomic_unlocked_write_u32](../p/pg_atomic_unlocked_write_u32.md)
  - StartBufferIO
  - TerminateBufferIO
  - [BufferGetPage](../B/BufferGetPage.md)
  - [BufferDescriptorGetContentLock](../B/BufferDescriptorGetContentLock.md)
  - LWLockAcquire
  - [LockBuffer](../L/LockBuffer.md)
  - LockBufferForCleanup
  - memset
  - BM_VALID
  - BM_IO_IN_PROGRESS
  - BUFFER_LOCK_EXCLUSIVE
  - LW_EXCLUSIVE
  - BLCKSZ
- Called from (representative examples):
  - [ReadBuffer_common](../R/ReadBuffer_common.md)

## Notes and Other Information
- This is a static function internal to bufmgr.c
- Handles both shared and local buffers with appropriate synchronization mechanisms
- Uses BM_IO_IN_PROGRESS to prevent concurrent access during zeroing operations
- Acquires content locks before marking buffers as valid to ensure atomicity
- The already_valid parameter allows optimization when caller knows buffer state
- Essential for maintaining buffer content consistency during page initialization
- Supports both exclusive locks (RBM_ZERO_AND_LOCK) and cleanup locks (RBM_ZERO_AND_CLEANUP_LOCK)
- Part of the buffer manager's page initialization and locking infrastructure