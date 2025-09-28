# GetPrivateRefCount

## Location
[src/backend/storage/buffer/bufmgr.c:415-437](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L415-L437)

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
  - [BufferIsValid](../B/BufferIsValid.md)
  - BufferIsLocal  
  - [GetPrivateRefCountEntry](GetPrivateRefCountEntry.md)
  - [PrivateRefCountEntry](../P/PrivateRefCountEntry.md) (struct type)
- Called from (representative examples):
  - BufferIsPinned
  - [ReadRecentBuffer](../R/ReadRecentBuffer.md)
  - [InvalidateBuffer](../I/InvalidateBuffer.md)
  - [InvalidateVictimBuffer](../I/InvalidateVictimBuffer.md)
  - [DebugPrintBufferRefcount](../D/DebugPrintBufferRefcount.md)
  - [PrintBufferDescs](../P/PrintBufferDescs.md)
  - [PrintPinnedBufs](../P/PrintPinnedBufs.md)
  - [MarkBufferDirtyHint](../M/MarkBufferDirtyHint.md)
  - [CheckBufferIsPinnedOnce](../C/CheckBufferIsPinnedOnce.md)
  - [HoldingBufferPinThatDelaysRecovery](../H/HoldingBufferPinThatDelaysRecovery.md)
  - [ConditionalLockBufferForCleanup](../C/ConditionalLockBufferForCleanup.md)
  - [IsBufferCleanupOK](../I/IsBufferCleanupOK.md)

## Notes and Other Information
- Only works for shared memory buffers (not local buffers)
- Returns 0 if the buffer is not currently pinned by this backend
- The inline qualifier suggests this is a frequently called function optimized for performance
- Does not modify the reference count or move entries between storage tiers
- Commonly used in buffer management logic to check pinning status before performing operations
- Essential for debugging and monitoring buffer usage patterns

## Simplified Source

```c
// Simplified version of GetPrivateRefCount
static inline int32 GetPrivateRefCount(Buffer buffer) {
    PrivateRefCountEntry *ref;

    Assert(BufferIsValid(buffer));
    Assert(!BufferIsLocal(buffer));

    // Look up the reference count entry for this buffer
    ref = GetPrivateRefCountEntry(buffer, false);

    // Return the reference count, or 0 if not found
    if (ref == NULL) {
        return 0;
    }
    return ref->refcount;
}
```

Key simplifications made:
- Preserved the assertion checks for validity
- Maintained the core lookup and return logic
- Added clear comments explaining the operation
- Kept the inline performance optimization
- Simple conditional logic for null reference handling