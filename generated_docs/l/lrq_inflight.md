# lrq_inflight

## Location
[src/backend/access/transam/xlogprefetcher.c:233-238](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogprefetcher.c#L233-L238)

## Overview
Returns the current number of inflight (pending) I/O operations in an LSN read queue.

## Definition
```c
static inline uint32 lrq_inflight(LsnReadQueue *lrq)
```

## Detailed Description
The `lrq_inflight` function is a simple accessor function that returns the current count of inflight I/O operations in the specified LSN read queue. This value represents the number of asynchronous read operations that have been initiated but have not yet completed. The function provides a clean interface for checking the queue's current load and is used by the WAL prefetcher to make decisions about when to initiate new I/O operations while respecting the maximum inflight limit.

This counter is maintained internally by the queue management functions and reflects the real-time state of pending operations.

## Parameters / Member Variables
- `lrq`: Pointer to the LsnReadQueue structure to query

## Dependencies
- Functions called/Symbols referenced:
  - [LsnReadQueue](../L/LsnReadQueue.md) (struct type)
- Called from (representative examples):
  - [XLogPrefetcherComputeStats](../X/XLogPrefetcherComputeStats.md)
  - [XLogPrefetcherReadRecord](../X/XLogPrefetcherReadRecord.md)

## Notes and Other Information
- Simple accessor function implemented as static inline for efficiency
- Returns a uint32 value representing the current inflight operation count
- Used for monitoring queue load and making scheduling decisions in the WAL prefetcher
- The returned value reflects operations that have been started but not yet completed
- Critical for maintaining the maximum inflight limit specified during queue creation

## Simplified Source

```c
// Get count of pending I/O operations currently in progress
static inline uint32 lrq_inflight(LsnReadQueue *lrq)
{
    return lrq->inflight;
}
```