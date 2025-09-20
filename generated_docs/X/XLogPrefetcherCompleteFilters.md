# XLogPrefetcherCompleteFilters

## Location
[src/backend/access/transam/xlogprefetcher.c:896-915](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogprefetcher.c#L896-L915)

## Overview
Removes expired prefetch filters that are no longer needed because the corresponding LSN has been replayed, allowing prefetching to resume for those block ranges.

## Definition

```c
static inline void
XLogPrefetcherCompleteFilters(XLogPrefetcher *prefetcher, XLogRecPtr replaying_lsn)
```
## Detailed Description
This function maintains the prefetch filter system by cleaning up filters that have outlived their purpose. It processes the filter queue in order, removing filters whose target LSN has been reached or exceeded by WAL replay.

The function operates on the principle that once a specific LSN has been replayed, any operations that required filtering (such as relation creation, extension, truncation, or database creation) have been completed, making it safe to resume prefetching for those block ranges.

The filter queue is organized with the most recently added filters at the head and older filters at the tail, allowing efficient processing of expired filters by examining from the tail. The function processes filters until it encounters one that hasn't expired yet, at which point all remaining filters in the queue are still active.

## Parameters / Member Variables
- : Pointer to the XLogPrefetcher structure containing the filter infrastructure
- : Current LSN being replayed, used to determine which filters have expired

## Dependencies
- Functions called/Symbols referenced:
  -  - Check if filter queue has any entries
  -  - Get the oldest filter from the queue
  -  - Remove filter from the queue
  -  - Remove filter from hash table using HASH_REMOVE
- Called from (representative examples):
  -  - Called during WAL replay to clean up expired filters

## Notes and Other Information
- Uses  hint for the empty queue check as most calls won't have filters to process
- Processes filters from tail (oldest) to head (newest) for optimal efficiency
- Atomic operation: both list and hash table entries are removed together
- Critical for preventing memory leaks and maintaining filter system performance
- Inline function for performance in the WAL replay path
- Must be called regularly during replay to prevent unbounded filter accumulation