# IsBufferCleanupOK

## Location
[src/backend/storage/buffer/bufmgr.c:5429-5482](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L5429-L5482)

## Overview
IsBufferCleanupOK determines whether cleanup operations can be safely performed on a buffer that is already locked, by checking if the current exclusive lock can act as a cleanup lock.

## Definition
```c
bool IsBufferCleanupOK(Buffer buffer)
```

## Detailed Description
This function checks whether it's safe to perform cleanup operations on a buffer that the caller has already locked exclusively. The key insight is that if the pin count is exactly 1 (meaning only the current process has the buffer pinned), then an exclusive lock effectively becomes a cleanup lock, allowing cleanup operations to proceed.

The function handles both shared buffers and local buffers differently:
- For local buffers: Checks that LocalRefCount is exactly 1
- For shared buffers: Verifies that both the private reference count and total buffer reference count are exactly 1

This allows callers who initially acquired a regular exclusive lock to upgrade their privileges to cleanup-level access when conditions permit, without needing to release and reacquire a cleanup lock.

## Parameters / Member Variables
- `buffer`: The buffer to check for cleanup eligibility

## Dependencies
- Functions called/Symbols referenced:
  - BufferDesc
  - BufferIsLocal
  - GetPrivateRefCount
  - GetBufferDescriptor
  - LWLockHeldByMeInMode
  - BufferDescriptorGetContentLock
  - LockBufHdr
  - BUF_STATE_GET_REFCOUNT
  - UnlockBufHdr
- Called from (representative examples):
  - hashbucketcleanup
  - _hash_doinsert
  - _hash_expandtable
  - _hash_splitbucket
  - RelationGetNumberOfBlocks

## Notes and Other Information
- The caller must already hold an exclusive lock on the buffer's content
- Returns true only when the buffer has exactly one pin (from the calling process)
- This function is an optimization that allows cleanup operations without lock cycling
- The function includes assertions to verify that the caller holds the required exclusive lock
- Local buffers have simpler logic since they don't involve shared memory coordination