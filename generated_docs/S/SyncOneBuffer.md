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
  - GetBufferDescriptor
  - ReservePrivateRefCountEntry
  - ResourceOwnerEnlarge
  - LockBufHdr
  - UnlockBufHdr
  - BUF_STATE_GET_REFCOUNT
  - BUF_STATE_GET_USAGECOUNT
  - PinBuffer_Locked
  - BufferDescriptorGetContentLock
  - FlushBuffer
  - UnpinBuffer
  - ScheduleBufferTagForWriteback
- Called from (representative examples):
  - BufferSync
  - BgBufferSync
  - BufferIsPinned

## Notes and Other Information
- Used by both checkpoint (BufferSync) and background writer (BgBufferSync) processes
- Implements WAL-before-data safety by checking dirty flags before acquiring locks
- Can skip recently-used buffers to focus on better replacement candidates
- Always uses shared content locks to allow concurrent reads during writes
- Integrates with writeback context for efficient I/O batching
- Returns detailed status information to help callers make decisions about buffer management
- Ensures proper resource management through pin/unpin operations and resource owner tracking