# lrq_completed

## Location
src/backend/access/transam/xlogprefetcher.c: 239 - 244

## Overview
Returns the current number of completed I/O operations in an LSN read queue that are ready for consumption.

## Definition
```c
static inline uint32 lrq_completed(LsnReadQueue *lrq)
```

## Detailed Description
The `lrq_completed` function is an accessor function that returns the current count of completed I/O operations in the specified LSN read queue. This value represents the number of asynchronous read operations that have finished successfully and have their results available for consumption by the WAL prefetcher. The function provides a clean interface for checking how many completed operations are ready to be processed, which helps the prefetcher determine when data is available without blocking.

This counter is maintained internally by the queue management functions and is incremented as I/O operations complete asynchronously.

## Parameters / Member Variables
- `lrq`: Pointer to the LsnReadQueue structure to query

## Dependencies
- Functions called/Symbols referenced:
  - LsnReadQueue (struct type)
- Called from (representative examples):
  - XLogPrefetcherComputeStats
  - XLogPrefetcherReadRecord

## Notes and Other Information
- Simple accessor function implemented as static inline for efficiency
- Returns a uint32 value representing the current completed operation count
- Used for determining data availability and making scheduling decisions in the WAL prefetcher
- The returned value reflects operations that have finished and have their results ready
- Complementary to lrq_inflight() - together they provide full visibility into queue state
- Critical for non-blocking operation where the prefetcher needs to know when data is ready