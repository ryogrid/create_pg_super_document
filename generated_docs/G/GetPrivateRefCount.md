# GetPrivateRefCount

## Location
src/backend/storage/buffer/bufmgr.c: 415 - 437

## Overview
GetPrivateRefCount returns the number of times a specified buffer is pinned by the current backend.

## Definition
```c
static inline int32 GetPrivateRefCount(Buffer buffer)
```

## Detailed Description
This inline function provides a simple interface to query how many times a specific buffer is currently pinned by the calling backend. It serves as a read-only accessor to the private reference counting system.

The function leverages GetPrivateRefCountEntry() with do_move set to false, meaning it will not optimize hash table entries by moving them to the array. This makes it suitable for read-only queries where access pattern optimization is not needed.

If no reference count entry exists for the buffer (indicating it's not currently pinned), the function returns 0.

## Parameters / Member Variables
- `buffer`: The Buffer ID to query for reference count information

## Dependencies
- Functions called/Symbols referenced:
  - BufferIsValid
  - BufferIsLocal  
  - GetPrivateRefCountEntry
  - PrivateRefCountEntry (struct type)
- Called from (representative examples):
  - BufferIsPinned
  - ReadRecentBuffer
  - InvalidateBuffer
  - InvalidateVictimBuffer
  - DebugPrintBufferRefcount
  - PrintBufferDescs
  - PrintPinnedBufs
  - MarkBufferDirtyHint
  - CheckBufferIsPinnedOnce
  - HoldingBufferPinThatDelaysRecovery
  - ConditionalLockBufferForCleanup
  - IsBufferCleanupOK

## Notes and Other Information
- Only works for shared memory buffers (not local buffers)
- Returns 0 if the buffer is not currently pinned by this backend
- The inline qualifier suggests this is a frequently called function optimized for performance
- Does not modify the reference count or move entries between storage tiers
- Commonly used in buffer management logic to check pinning status before performing operations
- Essential for debugging and monitoring buffer usage patterns