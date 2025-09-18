# lrq_alloc

## Location
[src/backend/access/transam/xlogprefetcher.c:202-226](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogprefetcher.c#L202-L226)

## Overview
Allocates and initializes a new LSN read queue for managing WAL (Write-Ahead Logging) record prefetching operations in PostgreSQL.

## Definition
```c
static inline LsnReadQueue *lrq_alloc(uint32 max_distance, uint32 max_inflight, uintptr_t lrq_private, LsnReadQueueNextFun next)
```

## Detailed Description
The `lrq_alloc` function creates a new LSN read queue structure used by the WAL prefetcher to manage asynchronous reading of WAL records. It allocates memory for the queue structure including space for a ring buffer that can hold up to `max_distance + 1` entries (the extra slot prevents ambiguity between full and empty states). The function initializes all queue state variables to their starting values and returns a ready-to-use queue structure.

The queue uses a ring buffer design to efficiently manage pending read operations, with separate counters tracking inflight and completed operations. This allows the prefetcher to maintain optimal I/O parallelism while respecting system resource limits.

## Parameters / Member Variables
- `max_distance`: Maximum number of WAL records that can be queued for reading ahead
- `max_inflight`: Maximum number of concurrent I/O operations allowed (must be ≤ max_distance)  
- `lrq_private`: Private data pointer passed through to callback functions
- `next`: Callback function used to determine the next LSN to prefetch

## Dependencies
- Functions called/Symbols referenced:
  - [LsnReadQueue](../L/LsnReadQueue.md) (struct type)
  - [palloc](../p/palloc.md) (memory allocation)
  - offsetof (macro for structure member offset)
- Called from (representative examples):
  - [XLogPrefetcherReadRecord](../X/XLogPrefetcherReadRecord.md)

## Notes and Other Information
- The function includes an assertion that max_distance >= max_inflight to ensure valid queue configuration
- Uses a ring buffer with size max_distance + 1 to distinguish between full and empty states
- Initializes all queue counters (head, tail, inflight, completed) to zero for a clean starting state
- The allocated queue structure includes both the control data and the ring buffer storage in a single allocation