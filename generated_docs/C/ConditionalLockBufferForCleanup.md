# ConditionalLockBufferForCleanup

## Location
[src/backend/storage/buffer/bufmgr.c:5373-5428](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L5373-L5428)

## Overview
ConditionalLockBufferForCleanup is a non-blocking version of LockBufferForCleanup that attempts to acquire exclusive buffer lock with pin count verification but returns immediately if conditions are not met.

## Definition

```c
bool
ConditionalLockBufferForCleanup(Buffer buffer)
```
## Detailed Description
This function provides a non-blocking alternative to LockBufferForCleanup for buffer cleanup operations. It performs the same safety checks (ensuring exclusive lock and pin count = 1) but does not wait if the conditions cannot be satisfied immediately. The function is designed for scenarios where the caller cannot afford to block and needs to know immediately whether cleanup is possible.

The function first verifies that the current backend holds exactly one pin on the buffer, then attempts to acquire an exclusive lock conditionally. If successful, it checks the global pin count, and only proceeds if no other backends hold pins. If any step fails, it releases any acquired locks and returns false.

For local buffers, the function only needs to check the local reference count since there's no cross-backend contention.

## Parameters / Member Variables
- `buffer`: The buffer to attempt locking for cleanup operations

## Dependencies
- Functions called/Symbols referenced:
  - [BufferIsValid](../B/BufferIsValid.md)
  - BufferIsLocal
  - [GetPrivateRefCount](../G/GetPrivateRefCount.md)
  - [ConditionalLockBuffer](ConditionalLockBuffer.md)
  - [GetBufferDescriptor](../G/GetBufferDescriptor.md)
  - [LockBufHdr](../L/LockBufHdr.md)/UnlockBufHdr
  - BUF_STATE_GET_REFCOUNT
  - [LockBuffer](../L/LockBuffer.md) (for unlock)
- Called from (representative examples):
  - [_hash_getbuf_with_condlock_cleanup](../h/_hash_getbuf_with_condlock_cleanup.md)
  - [_hash_finish_split](../h/_hash_finish_split.md)
  - [heap_page_prune_opt](../h/heap_page_prune_opt.md)
  - [lazy_scan_heap](../l/lazy_scan_heap.md)

## Notes and Other Information
- Returns true if successfully acquired exclusive lock with pin count = 1
- Returns false if unable to satisfy conditions immediately (no waiting)
- Performs the same safety protocol as LockBufferForCleanup but without blocking
- Useful for opportunistic cleanup operations where blocking is not acceptable
- Properly cleans up any partially acquired locks on failure
- Asserts that the buffer is valid and that the caller holds at least one pin