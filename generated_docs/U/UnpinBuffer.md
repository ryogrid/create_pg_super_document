# UnpinBuffer

## Location
[src/backend/storage/buffer/bufmgr.c:2795-2803](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L2795-L2803)

## Overview
UnpinBuffer decrements the pin count of a shared buffer and removes it from the current resource owner's buffer tracking, making the buffer available for potential replacement.

## Definition

```c
static void
UnpinBuffer(BufferDesc *buf)
```
## Detailed Description
UnpinBuffer is a static function that serves as a wrapper around UnpinBufferNoOwner while also handling resource ownership tracking. It specifically deals with shared buffers (never local ones) and always adjusts the CurrentResourceOwner by removing the buffer from its tracked resources before calling UnpinBufferNoOwner to perform the actual unpinning operation. This ensures proper resource management and prevents resource leaks in PostgreSQL's buffer management system.

## Parameters / Member Variables
- : Pointer to the BufferDesc structure representing the buffer to be unpinned

## Dependencies
- Functions called/Symbols referenced:
  - [BufferDescriptorGetBuffer](../B/BufferDescriptorGetBuffer.md)
  - [ResourceOwnerForgetBuffer](../R/ResourceOwnerForgetBuffer.md)
  - [UnpinBufferNoOwner](UnpinBufferNoOwner.md)
- Called from (representative examples):
  - [BufferAlloc](../B/BufferAlloc.md)
  - [GetVictimBuffer](../G/GetVictimBuffer.md)
  - [ReleaseBuffer](../R/ReleaseBuffer.md)
  - [SyncOneBuffer](../S/SyncOneBuffer.md)
  - [FlushRelationBuffers](../F/FlushRelationBuffers.md)

## Notes and Other Information
- This function should only be applied to shared buffers, never local ones
- Always adjusts CurrentResourceOwner to maintain proper resource tracking
- Acts as a resource-aware wrapper around the lower-level UnpinBufferNoOwner function
- Critical for preventing buffer pin leaks in PostgreSQL's memory management system