# LsnReadQueue

## Location
[src/backend/access/transam/xlogprefetcher.c:103-118](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogprefetcher.c#L103-L118)

## Overview
LsnReadQueue is a simple circular queue data structure used to control the number of potentially inflight WAL prefetch I/O operations in PostgreSQL's transaction log prefetcher.

## Definition

```c
typedef struct LsnReadQueue
{
	LsnReadQueueNextFun next;
	uintptr_t	lrq_private;
	uint32		max_inflight;
	uint32		inflight;
	uint32		completed;
	uint32		head;
	uint32		tail;
	uint32		size;
	struct
	{
		bool		io;
		XLogRecPtr	lsn;
	}			queue[FLEXIBLE_ARRAY_MEMBER];
} LsnReadQueue;
```
## Detailed Description
The LsnReadQueue serves as a circular buffer for managing LSN (Log Sequence Number) read operations in the PostgreSQL WAL prefetcher. It acts as an intermediate IO control mechanism, designed with intentional indirection through function pointers to allow for future extension to more general IO control mechanisms. The structure maintains counters for tracking inflight and completed operations while using a flexible array member to store queue entries containing LSN positions and IO status flags.

## Parameters / Member Variables
- : Function pointer of type LsnReadQueueNextFun that determines which block to prefetch next
- : Private data pointer passed to callback functions for context
- : Maximum number of concurrent inflight IO operations allowed  
- : Current number of inflight IO operations
- : Number of completed IO operations
- : Head position in the circular queue
- : Tail position in the circular queue  
- : Total size of the circular queue buffer
- : Flexible array member containing queue entries with:
  - : Boolean flag indicating whether IO should be performed for this entry
  - : XLogRecPtr containing the Log Sequence Number for this queue entry

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER
  - LsnReadQueueNextFun (callback function pointer type)
  - XLogRecPtr (WAL position type)

- Called from (representative examples):
  - [XLogPrefetcher](../X/XLogPrefetcher.md) (contains LsnReadQueue as member)  
  - [lrq_alloc](../l/lrq_alloc.md) (allocates and initializes LsnReadQueue)
  - [lrq_free](../l/lrq_free.md) (deallocates LsnReadQueue)
  - [lrq_inflight](../l/lrq_inflight.md) (checks inflight count)
  - [lrq_completed](../l/lrq_completed.md) (checks completed count)
  - [lrq_prefetch](../l/lrq_prefetch.md) (performs prefetch operations)
  - [lrq_complete_lsn](../l/lrq_complete_lsn.md) (marks LSN as completed)

## Notes and Other Information
The structure implements a gap-based circular buffer where the full ring buffer maintains a gap between head and tail to distinguish between full and empty states. The design anticipates future expansion to more sophisticated IO control mechanisms, explaining the function pointer indirection. The queue is used specifically in the context of WAL (Write-Ahead Log) prefetching to optimize read performance by predicting and pre-loading blocks that will be needed soon.