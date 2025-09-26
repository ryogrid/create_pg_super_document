# UnpinLocalBuffer

## Location
src/backend/storage/buffer/localbuf.c: 681 - 687

## Overview
UnpinLocalBuffer is a function that decrements the pin count of a local buffer and removes it from the current resource owner's tracking list.

## Definition

```c
void
UnpinLocalBuffer(Buffer buffer)
```
## Detailed Description
UnpinLocalBuffer serves as a wrapper function that performs two critical operations for local buffer management:
1. It decrements the buffer's pin count by calling UnpinLocalBufferNoOwner
2. It removes the buffer from the current resource owner's tracking list by calling ResourceOwnerForgetBuffer

This function is part of PostgreSQL's local buffer management system, which handles temporary relations and other local-only data structures. Local buffers are used for temporary tables and other objects that don't need to be shared across processes. The function ensures proper cleanup and resource tracking when a buffer is no longer needed by the current operation.

## Parameters / Member Variables
- : The Buffer identifier representing the local buffer to be unpinned

## Dependencies
- Functions called/Symbols referenced:
  - UnpinLocalBufferNoOwner
  - ResourceOwnerForgetBuffer
  - CurrentResourceOwner (global variable)
- Called from (representative examples):
  - ReleaseAndReadBuffer
  - ReleaseBuffer
  - ExtendBufferedRelLocal
  - ResourceOwnerForgetBufferIO

## Notes and Other Information
- This function is specifically for local buffers, not shared buffers
- The function combines buffer unpinning with resource owner cleanup, ensuring that resource tracking remains consistent
- Local buffers are used for temporary relations that are private to a single backend process
- The resource owner mechanism helps prevent resource leaks by automatically cleaning up resources when transactions or subtransactions end