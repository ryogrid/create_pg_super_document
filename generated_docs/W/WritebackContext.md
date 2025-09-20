# WritebackContext

## Location
[src/include/storage/buf_internals.h:297-307](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/buf_internals.h#L297-L307)

## Overview
The  structure manages the batching and coordination of pending writeback requests in PostgreSQL's buffer management system.

## Definition

```c
typedef struct WritebackContext
{
	/* pointer to the max number of writeback requests to coalesce */
	int		   *max_pending;

	/* current number of pending writeback requests */
	int			nr_pending;

	/* pending requests */
	PendingWriteback pending_writebacks[WRITEBACK_MAX_PENDING_FLUSHES];
} WritebackContext;
```
## Detailed Description
The  structure serves as the central coordination mechanism for PostgreSQL's writeback optimization system. It manages a collection of pending writeback requests, allowing the system to batch and coalesce multiple flush operations for improved I/O performance.

The context maintains an array of pending writeback requests up to a maximum limit (currently 256 as defined by ). It tracks both the current number of pending requests and provides a configurable maximum limit through a pointer, allowing for dynamic adjustment of writeback behavior.

This batching approach helps reduce the overhead of individual flush operations by grouping related I/O requests together, which can significantly improve overall system performance especially under heavy write workloads.

## Parameters / Member Variables
- : Pointer to the maximum number of writeback requests that can be coalesced/batched together
- : Current number of pending writeback requests waiting in the context
- : Array of PendingWriteback structures containing the actual pending flush requests (size limited by WRITEBACK_MAX_PENDING_FLUSHES = 256)

## Dependencies
- Functions called/Symbols referenced:
  - [PendingWriteback](../P/PendingWriteback.md) (for individual writeback requests)
  - WRITEBACK_MAX_PENDING_FLUSHES (constant defining array size)
- Called from (representative examples):
  - [WritebackContextInit](WritebackContextInit.md) (for context initialization)
  - [ScheduleBufferTagForWriteback](../S/ScheduleBufferTagForWriteback.md) (for adding requests to context)
  - [IssuePendingWritebacks](../I/IssuePendingWritebacks.md) (for processing pending requests)
  - BufferSync (for buffer synchronization operations)
  - BgBufferSync (for background buffer sync)

## Notes and Other Information
- Forward declared in bufmgr.h for use across buffer management modules
- Maximum pending flushes is currently set to 256 (WRITEBACK_MAX_PENDING_FLUSHES)
- The max_pending field is a pointer, allowing dynamic configuration of batching behavior
- Used by background writer and checkpointer processes for optimal I/O patterns
- Helps reduce system call overhead by batching multiple flush requests
- Critical component of PostgreSQL's I/O optimization strategy