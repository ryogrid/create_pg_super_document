# lrq_complete_lsn

## Location
[src/backend/access/transam/xlogprefetcher.c:272-293](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogprefetcher.c#L272-L293)

## Overview
Completes LSN processing in an LSN read queue by removing entries for LSNs that have been replayed and optionally triggering new prefetch operations.

## Definition

```c
static inline void
lrq_complete_lsn(LsnReadQueue *lrq, XLogRecPtr lsn)
```
## Detailed Description
This function manages the completion of LSN processing in the LSN read queue by advancing the tail pointer past all entries with LSNs less than the specified LSN. It operates under the assumption that LSNs before the given LSN have been replayed, meaning any I/O operations that were started before then have finished and can be safely removed from the queue.

The function performs queue maintenance by:
1. Iterating through queue entries from the tail while LSNs are less than the completion LSN
2. Decrementing the appropriate counter (inflight for active I/O, completed for finished operations)
3. Advancing the tail pointer with wraparound handling
4. Optionally triggering new prefetch operations if recovery prefetch is enabled

## Parameters / Member Variables
- : Pointer to the LsnReadQueue structure being managed
- : The XLogRecPtr representing the LSN up to which processing should be marked as complete

## Dependencies
- Functions called/Symbols referenced:
  - RecoveryPrefetchEnabled
  - [lrq_prefetch](lrq_prefetch.md)
- Called from (representative examples):
  - [XLogPrefetcherReadRecord](../X/XLogPrefetcherReadRecord.md)

## Notes and Other Information
- This is a static inline function for performance optimization
- The function maintains queue integrity by properly handling wraparound when the tail reaches the queue size
- It distinguishes between inflight and completed operations when updating counters
- The conditional call to lrq_prefetch ensures new prefetch operations are initiated when appropriate
- Located in src/backend/access/transam/xlogprefetcher.c:272-293