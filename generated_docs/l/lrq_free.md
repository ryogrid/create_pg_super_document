# lrq_free

## Location
[src/backend/access/transam/xlogprefetcher.c:227-232](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogprefetcher.c#L227-L232)

## Overview
Deallocates memory for an LSN read queue structure used by the WAL prefetcher.

## Definition
```c
static inline void lrq_free(LsnReadQueue *lrq)
```

## Detailed Description
The `lrq_free` function is a simple wrapper around PostgreSQL's `pfree` function that deallocates the memory previously allocated for an LSN read queue structure. This function serves as the counterpart to `lrq_alloc` and should be called when the queue is no longer needed to prevent memory leaks. The function is implemented as a static inline function for efficiency since it's a simple one-line wrapper.

## Parameters / Member Variables
- `lrq`: Pointer to the LsnReadQueue structure to be deallocated

## Dependencies
- Functions called/Symbols referenced:
  - [LsnReadQueue](../L/LsnReadQueue.md) (struct type)
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation function)
- Called from (representative examples):
  - [XLogPrefetcherFree](../X/XLogPrefetcherFree.md)
  - [XLogPrefetcherReadRecord](../X/XLogPrefetcherReadRecord.md)

## Notes and Other Information
- Simple wrapper function that ensures consistent memory management for LSN read queues
- Should always be called to clean up queues allocated with lrq_alloc
- Implemented as static inline for minimal overhead
- Does not perform any cleanup of queue contents - caller must ensure queue is properly drained before freeing