# gather_merge_readnext

## Location
[src/backend/executor/nodeGatherMerge.c:629-706](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeGatherMerge.c#L629-L706)

## Overview
Retrieves the next tuple for a specified reader (leader or worker) in a Gather Merge operation and stores it in the appropriate tuple slot, handling both local leader execution and remote worker tuple buffering.

## Definition
```c
static bool gather_merge_readnext(GatherMergeState *gm_state, int reader, bool nowait)
```

## Detailed Description
This function is a core component of PostgreSQL's parallel Gather Merge execution strategy. It handles two distinct tuple retrieval paths depending on whether the reader is the leader process (reader == 0) or a worker process (reader > 0).

For the leader process, it directly executes the outer plan using ExecProcNode() to generate tuples locally. It manages the DSA (Dynamic Shared Area) context during execution and tracks completion via the need_to_scan_locally flag.

For worker processes, it manages tuple retrieval through a sophisticated buffering system. It first checks if previously buffered tuples are available, then reads new tuples if needed. When a new tuple is successfully read, it proactively calls load_tuple_array() to prefetch additional tuples in non-blocking mode, optimizing throughput by reducing the frequency of cross-process communication.

The function builds a TupleTableSlot from the retrieved MinimalTuple, making it ready for use in the executor framework.

## Parameters / Member Variables
- `gm_state`: Pointer to the GatherMergeState containing the overall state of the parallel merge operation, including tuple slots, buffers, and execution context
- `reader`: Integer identifier for the source (0 = leader process, 1+ = worker process index)
- `nowait`: Boolean flag indicating whether the function should block waiting for tuples (false) or return immediately if no tuple is available (true)

## Dependencies
- Functions called/Symbols referenced:
  - outerPlanState
  - ExecProcNode
  - TupIsNull
  - [gm_readnext_tuple](gm_readnext_tuple.md)
  - [load_tuple_array](../l/load_tuple_array.md)
  - [ExecStoreMinimalTuple](../E/ExecStoreMinimalTuple.md)
  - [GatherMergeState](../G/GatherMergeState.md)
  - [GMReaderTupleBuffer](../G/GMReaderTupleBuffer.md)
  - MinimalTuple
- Called from (representative examples):
  - [gather_merge_init](gather_merge_init.md)
  - [gather_merge_getnext](gather_merge_getnext.md)

## Notes and Other Information
- Returns true if a tuple was successfully retrieved and stored, false otherwise
- Sets the done flag in the tuple buffer when a worker is exhausted
- Handles DSA area installation/cleanup during local plan execution for proper memory management
- The function implements an important optimization by calling load_tuple_array() after reading a single tuple to prefetch additional tuples
- Part of the parallel query execution framework that merges sorted streams from multiple workers
- Maintains separate handling logic for leader vs worker processes due to their fundamentally different tuple sources