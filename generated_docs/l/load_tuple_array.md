# load_tuple_array

## Location
[src/backend/executor/nodeGatherMerge.c:590-628](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeGatherMerge.c#L590-L628)

## Overview
Fills a worker's tuple buffer array by reading tuples in non-blocking mode from a parallel worker process until the buffer is full or no more tuples are immediately available.

## Definition

```c
static void
load_tuple_array(GatherMergeState *gm_state, int reader)
```
## Detailed Description
This function is part of PostgreSQL's Gather Merge parallel query execution mechanism. It reads tuples from a specific worker process (identified by the reader parameter) and loads them into a pre-allocated tuple buffer array. The function operates in "nowait" mode, meaning it will not block if no tuples are immediately available.

The function implements a buffering strategy where it tries to fill up to MAX_TUPLE_STORE tuples in the worker's tuple buffer. This buffering reduces the overhead of frequent tuple-by-tuple reads from parallel workers by batching multiple tuples at once.

The function includes special handling for the leader process (reader == 0) by doing nothing, as the leader doesn't need to read from itself. It also optimizes buffer management by resetting counters when all buffered tuples have been consumed.

## Parameters / Member Variables
- : Pointer to the GatherMergeState structure containing the overall state of the Gather Merge operation, including tuple buffers for all workers
- : Integer identifier for the worker process to read from (0 represents the leader, 1+ represent worker processes)

## Dependencies
- Functions called/Symbols referenced:
  - [gm_readnext_tuple](../g/gm_readnext_tuple.md)
  - [GatherMergeState](../G/GatherMergeState.md)
  - [GMReaderTupleBuffer](../G/GMReaderTupleBuffer.md)
  - MinimalTuple
  - MAX_TUPLE_STORE
- Called from (representative examples):
  - [gather_merge_init](../g/gather_merge_init.md)
  - [gather_merge_readnext](../g/gather_merge_readnext.md)

## Notes and Other Information
- The function operates only on worker processes (reader > 0), skipping the leader process entirely
- Uses a lazy reset strategy for buffer counters, only resetting when the buffer has been fully consumed
- Integrates with PostgreSQL's parallel query execution framework for efficient tuple processing
- The MAX_TUPLE_STORE constant defines the maximum number of tuples that can be buffered per worker
- Part of the nodeGatherMerge.c module which handles merging sorted results from multiple parallel workers