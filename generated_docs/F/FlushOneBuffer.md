# FlushOneBuffer

## Location
src/backend/storage/buffer/bufmgr.c: 4877 - 4896

## Overview
FlushOneBuffer flushes a previously locked and pinned buffer to the operating system, providing a simple interface for flushing a single specific buffer.

## Definition
```c
void FlushOneBuffer(Buffer buffer)
```

## Detailed Description
This function provides a simplified interface for flushing a single buffer that is already properly locked and pinned by the caller. It performs validation checks to ensure the buffer is in the correct state before delegating to FlushBuffer for the actual I/O operation. The function includes:

- Validation that the buffer is not a local buffer (shared buffers only)
- Assertion that the buffer is properly pinned by the caller
- Verification that the caller holds the appropriate content lock
- Direct delegation to FlushBuffer with standard relation I/O parameters

This function is designed for use cases where the caller has already established the necessary buffer state and simply needs to trigger the flush operation.

## Parameters / Member Variables
- `buffer`: Buffer identifier for the buffer to be flushed to disk

## Dependencies
- Functions called/Symbols referenced:
  - BufferIsLocal, BufferIsPinned
  - GetBufferDescriptor
  - LWLockHeldByMe
  - BufferDescriptorGetContentLock
  - FlushBuffer
  - IOOBJECT_RELATION, IOCONTEXT_NORMAL constants
- Called from (representative examples):
  - hash_xlog_init_meta_page
  - hash_xlog_init_bitmap_page
  - XLogReadBufferForRedoExtended

## Notes and Other Information
- Currently only supports shared buffers, not local buffers (though no fundamental reason prevents local buffer support)
- Requires the caller to have already pinned the buffer and acquired the appropriate content lock
- Uses assertions for validation, indicating this is intended for use in contexts where these preconditions are guaranteed
- The function serves as a convenience wrapper around FlushBuffer for single-buffer operations
- Commonly used in WAL replay operations where specific buffers need to be flushed after modification
- Uses standard I/O context parameters (relation object, normal context) for the flush operation