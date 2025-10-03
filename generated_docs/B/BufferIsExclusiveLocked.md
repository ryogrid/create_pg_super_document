# BufferIsExclusiveLocked

## Location
[src/backend/storage/buffer/bufmgr.c:2459-2487](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L2459-L2487)

## Overview
BufferIsExclusiveLocked checks whether a buffer is currently exclusive-locked by the calling backend, supporting both shared and local buffers.

## Definition

```c
bool
BufferIsExclusiveLocked(Buffer buffer)
```
## Detailed Description
BufferIsExclusiveLocked provides a utility function to determine if the current backend holds an exclusive lock on a buffer's content. It handles both shared buffers (managed by the shared buffer pool) and local buffers (used for temporary relations) by routing to the appropriate buffer descriptor retrieval function. The function requires that the buffer is already pinned by the caller and uses LWLockHeldByMeInMode to check the actual lock state.

This function is critical for WAL logging operations and other scenarios where exclusive access to buffer content must be verified before proceeding with modifications. It ensures that the calling code has proper exclusive access before performing operations that require consistent buffer content.

## Parameters / Member Variables
- `buffer`: Buffer handle to check for exclusive locking (must be pinned)
## Dependencies
- Functions called/Symbols referenced:
  - BufferIsLocal
  - [GetLocalBufferDescriptor](../G/GetLocalBufferDescriptor.md)
  - [GetBufferDescriptor](../G/GetBufferDescriptor.md)
  - BufferIsPinned
  - [LWLockHeldByMeInMode](../L/LWLockHeldByMeInMode.md)
  - [BufferDescriptorGetContentLock](BufferDescriptorGetContentLock.md)
  - LW_EXCLUSIVE
- Called from (representative examples):
  - [XLogRegisterBuffer](../X/XLogRegisterBuffer.md)
  - BUFFER_LOCK_EXCLUSIVE

## Notes and Other Information
- Requires buffer to be pinned before calling (enforced by assertion)
- Handles both shared and local (temporary) buffers transparently
- Essential for WAL logging to ensure buffer modifications are done under exclusive lock
- Part of PostgreSQL's buffer locking verification infrastructure
- Used in critical path operations where exclusive buffer access must be confirmed

## Simplified Source

```c
bool BufferIsExclusiveLocked(Buffer buffer)
{
    BufferDesc *bufHdr;

    // Get buffer descriptor based on buffer type
    if (BufferIsLocal(buffer)) {
        int bufid = -buffer - 1;
        bufHdr = GetLocalBufferDescriptor(bufid);
    } else {
        bufHdr = GetBufferDescriptor(buffer - 1);
    }

    // Verify buffer is pinned
    Assert(BufferIsPinned(buffer));

    // Check if we hold exclusive lock
    return LWLockHeldByMeInMode(BufferDescriptorGetContentLock(bufHdr), LW_EXCLUSIVE);
}
```