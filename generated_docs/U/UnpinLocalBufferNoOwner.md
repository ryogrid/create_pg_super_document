# UnpinLocalBufferNoOwner

## Location
[src/backend/storage/buffer/localbuf.c:688-703](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/localbuf.c#L688-L703)

## Overview
UnpinLocalBufferNoOwner decrements the reference count of a local buffer without involving resource owner tracking, used for low-level buffer management operations.

## Definition
```c
void UnpinLocalBufferNoOwner(Buffer buffer)
```

## Detailed Description
UnpinLocalBufferNoOwner performs the core unpinning operation for local buffers by decrementing the buffer's reference count in the LocalRefCount array. This function is the lower-level counterpart to UnpinLocalBuffer, focusing solely on the reference counting mechanism without resource owner involvement.

The function converts the buffer identifier to a buffer index, decrements the reference count, and if the count reaches zero, it also decrements the global counter of pinned local buffers (NLocalPinnedBuffers). This ensures accurate tracking of how many local buffers are currently pinned in the system.

The function includes several assertions to validate the buffer state, ensuring that the buffer is indeed a local buffer, has a positive reference count, and that the global pinned buffer count is consistent.

## Parameters / Member Variables
- `buffer`: The Buffer identifier representing the local buffer to be unpinned (negative value for local buffers)

## Dependencies
- Functions called/Symbols referenced:
  - BufferIsLocal (macro for validation)
- Global variables accessed:
  - LocalRefCount (array tracking reference counts)
  - NLocalPinnedBuffers (global counter)
- Called from (representative examples):
  - [ResOwnerReleaseBufferPin](../R/ResOwnerReleaseBufferPin.md)
  - [UnpinLocalBuffer](UnpinLocalBuffer.md)
  - [ResourceOwnerForgetBufferIO](../R/ResourceOwnerForgetBufferIO.md)

## Notes and Other Information
- This function does not interact with the resource owner system, making it suitable for cleanup operations where resource tracking is handled separately
- Local buffer identifiers are negative numbers, and the function converts them to array indices using the formula: buffid = -buffer - 1
- The function maintains the invariant that NLocalPinnedBuffers accurately reflects the number of buffers with non-zero reference counts
- Assertions ensure data integrity and help catch programming errors during development and testing

## Simplified Source

```c
void UnpinLocalBufferNoOwner(Buffer buffer)
{
    int buffid = -buffer - 1;

    Assert(BufferIsLocal(buffer));
    Assert(LocalRefCount[buffid] > 0);
    Assert(NLocalPinnedBuffers > 0);

    if (--LocalRefCount[buffid] == 0)
        NLocalPinnedBuffers--;
}
```