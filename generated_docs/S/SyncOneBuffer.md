# SyncOneBuffer

## Location
[src/backend/storage/buffer/bufmgr.c:3475-3547](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L3475-L3547)

## Overview
SyncOneBuffer processes a single buffer during checkpoint or background writer operations, determining if the buffer needs writing and performing the actual I/O operation while tracking buffer reusability.

## Definition
```c
static int SyncOneBuffer(int buf_id, bool skip_recently_used, WritebackContext *wb_context)
```

## Detailed Description
SyncOneBuffer is the core function for processing individual buffers during sync operations. It first checks if the buffer needs writing by examining its dirty and valid state flags. The function can optionally skip recently-used buffers (those with non-zero pin counts or usage counts) when called from the background writer. If a buffer needs writing, it pins the buffer, acquires a shared content lock, calls FlushBuffer to write it to disk, and then schedules the buffer for writeback batching. The function returns a bitmask indicating whether the buffer was written and whether it's reusable for replacement.

## Parameters / Member Variables
- `buf_id`: The buffer ID to process
- `skip_recently_used`: If true, skip buffers that are currently pinned or marked recently used
- `wb_context`: Writeback context for batching I/O operations
- Returns: Bitmask with BUF_WRITTEN (buffer was written) and/or BUF_REUSABLE (buffer available for replacement) flags

## Dependencies
- Functions called/Symbols referenced:
  - [GetBufferDescriptor](../G/GetBufferDescriptor.md)
  - [ReservePrivateRefCountEntry](../R/ReservePrivateRefCountEntry.md)
  - [ResourceOwnerEnlarge](../R/ResourceOwnerEnlarge.md)
  - [LockBufHdr](../L/LockBufHdr.md)
  - [UnlockBufHdr](../U/UnlockBufHdr.md)
  - BUF_STATE_GET_REFCOUNT
  - BUF_STATE_GET_USAGECOUNT
  - [PinBuffer_Locked](../P/PinBuffer_Locked.md)
  - [BufferDescriptorGetContentLock](../B/BufferDescriptorGetContentLock.md)
  - [FlushBuffer](../F/FlushBuffer.md)
  - [UnpinBuffer](../U/UnpinBuffer.md)
  - [ScheduleBufferTagForWriteback](ScheduleBufferTagForWriteback.md)
- Called from (representative examples):
  - [BufferSync](../B/BufferSync.md)
  - [BgBufferSync](../B/BgBufferSync.md)
  - BufferIsPinned

## Notes and Other Information
- Used by both checkpoint (BufferSync) and background writer (BgBufferSync) processes
- Implements WAL-before-data safety by checking dirty flags before acquiring locks
- Can skip recently-used buffers to focus on better replacement candidates
- Always uses shared content locks to allow concurrent reads during writes
- Integrates with writeback context for efficient I/O batching
- Returns detailed status information to help callers make decisions about buffer management
- Ensures proper resource management through pin/unpin operations and resource owner tracking

## Simplified Source

```c
// Simplified version of SyncOneBuffer
static int SyncOneBuffer(int buf_id, bool skip_recently_used, WritebackContext *wb_context) {
    BufferDesc *buffer = GetBufferDescriptor(buf_id);
    int result = 0;
    uint32 buffer_state;

    // Prepare for buffer operations
    ReservePrivateRefCountEntry();
    ResourceOwnerEnlarge(CurrentResourceOwner);

    // Lock buffer header and check state
    buffer_state = LockBufHdr(buffer);

    // Check if buffer is reusable (no pins, no recent usage)
    if (BUF_STATE_GET_REFCOUNT(buffer_state) == 0 &&
        BUF_STATE_GET_USAGECOUNT(buffer_state) == 0) {
        result |= BUF_REUSABLE;
    }
    else if (skip_recently_used) {
        // Skip recently used buffers if requested
        UnlockBufHdr(buffer, buffer_state);
        return result;
    }

    // Check if buffer needs writing (must be valid and dirty)
    if (!(buffer_state & BM_VALID) || !(buffer_state & BM_DIRTY)) {
        // Buffer is clean, nothing to do
        UnlockBufHdr(buffer, buffer_state);
        return result;
    }

    // Pin buffer and acquire shared content lock for writing
    PinBuffer_Locked(buffer);
    LWLockAcquire(BufferDescriptorGetContentLock(buffer), LW_SHARED);

    // Write the buffer to disk
    FlushBuffer(buffer, NULL, IOOBJECT_RELATION, IOCONTEXT_NORMAL);

    // Release lock and unpin buffer
    LWLockRelease(BufferDescriptorGetContentLock(buffer));
    BufferTag tag = buffer->tag;
    UnpinBuffer(buffer);

    // Schedule for writeback batching
    ScheduleBufferTagForWriteback(wb_context, IOCONTEXT_NORMAL, &tag);

    return result | BUF_WRITTEN;
}
```

Key simplifications made:
- Removed detailed comments and consolidated them into clear step descriptions
- Used more descriptive variable name (`buffer` instead of `bufHdr`)
- Simplified the buffer state checking logic flow
- Consolidated lock acquisition and release operations
- Focused on the main execution path without losing essential functionality
- Maintained the critical WAL-before-data safety logic
- Preserved all return value semantics and error handling paths