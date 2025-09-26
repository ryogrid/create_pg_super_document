# UnpinBufferNoOwner

## Location
[src/backend/storage/buffer/bufmgr.c:2804-2882](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L2804-L2882)

## Overview
UnpinBufferNoOwner decrements both the private and shared reference counts of a buffer without resource owner tracking, handling complex pin count management and waiter notification.

## Definition
```c
static void UnpinBufferNoOwner(BufferDesc *buf)
```

## Detailed Description
UnpinBufferNoOwner is the core buffer unpinning function that performs the actual work of decrementing reference counts. It first decrements the private reference count for the current backend, and when that reaches zero, it decrements the shared reference count using atomic operations to ensure thread safety. The function also handles special cases like supporting LockBufferForCleanup() by notifying waiting backends when they become the last pinner. It uses compare-and-swap operations to safely update the shared buffer state and includes Valgrind integration to mark unpinned buffers as non-accessible for debugging.

## Parameters / Member Variables
- `buf`: Pointer to the BufferDesc structure representing the buffer to be unpinned

## Dependencies
- Functions called/Symbols referenced:
  - [BufferDescriptorGetBuffer](../B/BufferDescriptorGetBuffer.md)
  - BufferIsLocal
  - [GetPrivateRefCountEntry](../G/GetPrivateRefCountEntry.md)
  - BufHdrGetBlock
  - VALGRIND_MAKE_MEM_NOACCESS
  - [LWLockHeldByMe](../L/LWLockHeldByMe.md)
  - [BufferDescriptorGetContentLock](../B/BufferDescriptorGetContentLock.md)
  - [pg_atomic_read_u32](../p/pg_atomic_read_u32.md)
  - [WaitBufHdrUnlocked](../W/WaitBufHdrUnlocked.md)
  - [pg_atomic_compare_exchange_u32](../p/pg_atomic_compare_exchange_u32.md)
  - [LockBufHdr](../L/LockBufHdr.md)
  - [UnlockBufHdr](UnlockBufHdr.md)
  - [ProcSendSignal](../P/ProcSendSignal.md)
  - [ForgetPrivateRefCountEntry](../F/ForgetPrivateRefCountEntry.md)
- Called from (representative examples):
  - [UnpinBuffer](UnpinBuffer.md)
  - BufferIsPinned
  - [ResOwnerReleaseBufferPin](../R/ResOwnerReleaseBufferPin.md)

## Notes and Other Information
- Only works with shared buffers (asserts against local buffers)
- Uses atomic compare-and-swap operations for thread-safe shared reference count updates
- Handles BM_PIN_COUNT_WAITER flag to support LockBufferForCleanup() functionality
- Integrates with Valgrind for memory debugging by marking unpinned buffers non-accessible
- Ensures no content locks are held when unpinning buffers
- [Complex](../C/Complex.md) synchronization logic to handle concurrent buffer access safely