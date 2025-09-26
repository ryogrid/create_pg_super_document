# ScheduleBufferTagForWriteback

## Location
[src/backend/storage/buffer/bufmgr.c:5889-5918](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L5889-L5918)

## Overview
Adds a dirty buffer to the pending writeback queue for batched I/O operations, managing the coalescing and scheduling of buffer flushes to optimize storage performance.

## Definition
```c
void ScheduleBufferTagForWriteback(WritebackContext *wb_context, IOContext io_context, BufferTag *tag)
```

## Detailed Description
The ScheduleBufferTagForWriteback function manages the queueing of dirty buffers for writeback operations, implementing an efficient batching mechanism to improve I/O performance. This function is central to PostgreSQL's buffer writeback strategy, which coalesces multiple individual write operations into larger, more efficient batches.

The function operates in several stages:
1. **Direct I/O Check**: Returns early if IO_DIRECT_DATA flag is set, as direct I/O doesn't benefit from writeback batching
2. **Queue Management**: If writeback control is enabled (max_pending > 0), adds the buffer tag to the pending writebacks array
3. **Batch Processing**: When the number of pending writebacks reaches the configured limit, triggers IssuePendingWritebacks to flush the accumulated batch

This batching approach reduces the overhead of individual I/O operations by grouping related writes, improving overall system performance especially under high write loads. The function also handles dynamic configuration changes where writeback control might be disabled after buffers have already been queued.

## Parameters / Member Variables
- `wb_context`: Pointer to WritebackContext managing the writeback state and pending operations
- `io_context`: IOContext providing I/O operation context and metadata  
- `tag`: Pointer to BufferTag identifying the specific buffer to schedule for writeback

## Dependencies
- Functions called/Symbols referenced:
  - [WritebackContext](../W/WritebackContext.md) (type)
  - [IOContext](../I/IOContext.md) (type)
  - BufferTag (type)
  - [PendingWriteback](../P/PendingWriteback.md) (type)
  - IO_DIRECT_DATA (constant)
  - WRITEBACK_MAX_PENDING_FLUSHES (constant)
  - [IssuePendingWritebacks](../I/IssuePendingWritebacks.md) (function)
- Called from (representative examples):
  - [GetVictimBuffer](../G/GetVictimBuffer.md) (buffer replacement operations)
  - [SyncOneBuffer](SyncOneBuffer.md) (single buffer synchronization)
  - [ResourceOwnerForgetBufferIO](../R/ResourceOwnerForgetBufferIO.md) (resource cleanup operations)

## Notes and Other Information
- Skips writeback scheduling when direct I/O is enabled, as it doesn't benefit from batching
- Automatically triggers batch flushes when the pending limit is reached
- Handles dynamic reconfiguration where writeback control can be disabled at runtime
- Critical for I/O performance optimization in high-throughput database operations  
- The batching mechanism reduces system call overhead and improves disk utilization
- Part of PostgreSQL's sophisticated buffer management system for optimal storage performance