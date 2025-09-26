# UnlockReleaseBuffer

## Location
[src/backend/storage/buffer/bufmgr.c:4914-4928](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L4914-L4928)

## Overview
UnlockReleaseBuffer is a convenience function that unlocks a buffer's content lock and releases the buffer pin in a single operation.

## Definition

```c
void
UnlockReleaseBuffer(Buffer buffer)
```
## Detailed Description
This function provides a shorthand for the common combination of unlocking a buffer and then releasing it. It sequentially calls LockBuffer() with BUFFER_LOCK_UNLOCK to release the content lock, followed by ReleaseBuffer() to release the pin on the buffer. This pattern is frequently used throughout PostgreSQL's storage layer when code has finished working with a buffer and needs to clean up both the lock and the reference.

## Parameters / Member Variables
- : The Buffer identifier representing the buffer to unlock and release

## Dependencies
- Functions called/Symbols referenced:
  - [LockBuffer](../L/LockBuffer.md) (with BUFFER_LOCK_UNLOCK)
  - [ReleaseBuffer](../R/ReleaseBuffer.md)
  - BUFFER_LOCK_UNLOCK
- Called from (representative examples):
  - Various access method implementations (heap, btree, gin, gist, spgist, brin, hash)
  - Transaction log processing functions
  - Vacuum and visibility map operations
  - Free space management functions

## Notes and Other Information
- This is a convenience function that combines two common buffer management operations
- Widely used across PostgreSQL's storage layer, appearing in over 50 source files
- The function ensures proper cleanup order: unlock first, then release the pin
- Essential for preventing buffer leaks and lock contention in PostgreSQL's buffer management system