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
  - [BufferDesc](../B/BufferDesc.md)
  - [ResourceOwnerEnlarge](../R/ResourceOwnerEnlarge.md)
  - [LockBufHdr](../L/LockBufHdr.md)
  - [UnlockBufHdr](../U/UnlockBufHdr.md)
  - BM_IO_IN_PROGRESS
  - [WaitIO](../W/WaitIO.md)
  - BM_VALID
  - BM_DIRTY
  - [ResourceOwnerRememberBufferIO](../R/ResourceOwnerRememberBufferIO.md)
  - [BufferDescriptorGetBuffer](../B/BufferDescriptorGetBuffer.md)
- Called from (representative examples):
  - BufferIsPinned
  - [ZeroAndLockBuffer](../Z/ZeroAndLockBuffer.md)
  - [WaitReadBuffersCanStartIO](../W/WaitReadBuffersCanStartIO.md)
  - [ExtendBufferedRelShared](../E/ExtendBufferedRelShared.md)
  - [FlushBuffer](../F/FlushBuffer.md)

## Notes and Other Information
- Returns true if I/O was successfully started, false if the work was already done or in progress
- Assumes the calling process is not currently executing any I/O and that the buffer is pinned
- Uses resource owner tracking to ensure proper cleanup of I/O operations
- The nowait parameter enables non-blocking behavior for performance-critical paths
- Critical for preventing duplicate I/O operations in high-concurrency scenarios
- Part of PostgreSQL's buffer I/O coordination infrastructure
- Only applies to shared buffers, not local buffers

## Simplified Source

```c
// Simplified version of StartBufferIO
static bool StartBufferIO(BufferDesc *buf, bool forInput, bool nowait) {
    uint32 buf_state;

    // Prepare resource tracking for this I/O operation
    ResourceOwnerEnlarge(CurrentResourceOwner);

    // Loop until no other I/O is in progress on this buffer
    for (;;) {
        buf_state = LockBufHdr(buf);

        if (!(buf_state & BM_IO_IN_PROGRESS))
            break;  // No I/O in progress, we can proceed

        UnlockBufHdr(buf, buf_state);

        if (nowait)
            return false;  // Don't wait, just return failure

        WaitIO(buf);  // Wait for current I/O to complete
    }

    // Check if the work we wanted to do is already done
    bool work_already_done = forInput ? (buf_state & BM_VALID)
                                      : !(buf_state & BM_DIRTY);

    if (work_already_done) {
        UnlockBufHdr(buf, buf_state);
        return false;  // Someone else already completed the I/O
    }

    // Mark buffer as having I/O in progress
    buf_state |= BM_IO_IN_PROGRESS;
    UnlockBufHdr(buf, buf_state);

    // Register this I/O operation with resource owner for cleanup
    ResourceOwnerRememberBufferIO(CurrentResourceOwner,
                                  BufferDescriptorGetBuffer(buf));

    return true;  // Successfully started I/O
}
```

Key simplifications made:
- Removed detailed comments and replaced with high-level explanations
- Simplified the conditional check for work completion into a clearer boolean variable
- Added descriptive comments explaining the main logic flow
- Consolidated the complex ternary condition into a more readable format
- Maintained all essential logic including race condition handling, work avoidance, and resource tracking
- Preserved the critical synchronization mechanism using buffer state flags