# LsnReadQueue

## Location
src/backend/access/transam/xlogprefetcher.c: 103 - 118

## Overview
LsnReadQueue is a simple circular queue data structure used to control the number of potentially inflight WAL prefetch I/O operations in PostgreSQL's transaction log prefetcher.

## Definition


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
  - XLogPrefetcher (contains LsnReadQueue as member)  
  - lrq_alloc (allocates and initializes LsnReadQueue)
  - lrq_free (deallocates LsnReadQueue)
  - lrq_inflight (checks inflight count)
  - lrq_completed (checks completed count)
  - lrq_prefetch (performs prefetch operations)
  - lrq_complete_lsn (marks LSN as completed)

## Notes and Other Information
The structure implements a gap-based circular buffer where the full ring buffer maintains a gap between head and tail to distinguish between full and empty states. The design anticipates future expansion to more sophisticated IO control mechanisms, explaining the function pointer indirection. The queue is used specifically in the context of WAL (Write-Ahead Log) prefetching to optimize read performance by predicting and pre-loading blocks that will be needed soon.