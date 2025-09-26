# StartBufferIO

## Location
[src/backend/storage/buffer/bufmgr.c:5532-5588](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L5532-L5588)

## Overview
StartBufferIO initiates I/O operations on a buffer by setting the BM_IO_IN_PROGRESS flag, handling race conditions and ensuring only one process performs I/O on a buffer at a time.

## Definition
```c
static bool StartBufferIO(BufferDesc *buf, bool forInput, bool nowait)
```

## Detailed Description
This function coordinates the start of I/O operations on shared buffers, implementing a sophisticated synchronization mechanism to handle concurrent access. It serves as a critical gatekeeper that ensures I/O operations are performed efficiently without duplication.

The function operates in several phases:
1. **Race condition handling**: Loops until no other I/O is in progress, optionally waiting or returning immediately based on the nowait parameter
2. **Work avoidance**: Checks if the desired I/O operation has already been completed by another process
3. **I/O initiation**: Sets the BM_IO_IN_PROGRESS flag and registers the I/O with the resource owner

The function distinguishes between input and output operations:
- Input operations are only attempted on invalid buffers (not BM_VALID)
- Output operations are only attempted on dirty, valid buffers (BM_VALID and BM_DIRTY)

This design allows the function to detect when the work has already been done and avoid redundant I/O.

## Parameters / Member Variables
- `buf`: Pointer to the BufferDesc on which to start I/O
- `forInput`: True for read operations, false for write operations
- `nowait`: If true, don't wait for other I/O to complete; return false immediately

## Dependencies
- Functions called/Symbols referenced:
  - BufferDesc
  - ResourceOwnerEnlarge
  - LockBufHdr
  - UnlockBufHdr
  - BM_IO_IN_PROGRESS
  - WaitIO
  - BM_VALID
  - BM_DIRTY
  - ResourceOwnerRememberBufferIO
  - BufferDescriptorGetBuffer
- Called from (representative examples):
  - BufferIsPinned
  - ZeroAndLockBuffer
  - WaitReadBuffersCanStartIO
  - ExtendBufferedRelShared
  - FlushBuffer

## Notes and Other Information
- Returns true if I/O was successfully started, false if the work was already done or in progress
- Assumes the calling process is not currently executing any I/O and that the buffer is pinned
- Uses resource owner tracking to ensure proper cleanup of I/O operations
- The nowait parameter enables non-blocking behavior for performance-critical paths
- Critical for preventing duplicate I/O operations in high-concurrency scenarios
- Part of PostgreSQL's buffer I/O coordination infrastructure
- Only applies to shared buffers, not local buffers