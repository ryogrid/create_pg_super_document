# ConditionalLockBuffer

## Location
src/backend/storage/buffer/bufmgr.c: 5158 - 5178

## Overview
ConditionalLockBuffer is a non-blocking function that attempts to acquire exclusive content lock on a buffer without waiting if the lock is not immediately available.

## Definition


## Detailed Description
This function provides a non-blocking mechanism to acquire the content_lock for a buffer in BUFFER_LOCK_EXCLUSIVE mode. It's designed for scenarios where the caller cannot afford to wait for a lock to become available. The function assumes the buffer is already pinned by the caller.

For local buffers (those that belong to the current backend), the function always returns true since local buffers don't require locking between processes. For shared buffers, it uses the lightweight lock mechanism to conditionally acquire the buffer's content lock.

## Parameters / Member Variables
- `buffer`: The buffer identifier for which to acquire the exclusive content lock

## Dependencies
- Functions called/Symbols referenced:
  - BufferIsPinned
  - BufferIsLocal  
  - GetBufferDescriptor
  - LWLockConditionalAcquire
  - BufferDescriptorGetContentLock
- Called from (representative examples):
  - GinNewBuffer
  - gistNewBuffer
  - RelationGetBufferForTuple
  - _bt_conditionallockbuf
  - ConditionalLockBufferForCleanup

## Notes and Other Information
- The function asserts that the buffer is pinned before attempting to lock it
- Returns true if the lock was successfully acquired or if dealing with a local buffer
- Returns false if the lock could not be acquired immediately
- This is the conditional (non-blocking) variant of LockBuffer which would block until the lock is available