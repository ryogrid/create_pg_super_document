# gather_merge_init

## Location
[src/backend/executor/nodeGatherMerge.c:436-518](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeGatherMerge.c#L436-L518)

## Overview
Initializes the GatherMerge execution by resetting data structures, pulling the first tuple from each source (leader and workers), and building the binary heap for ordered merging.

## Definition
```c
static void gather_merge_init(GatherMergeState *gm_state)
```

## Detailed Description
gather_merge_init performs the critical initialization phase of GatherMerge execution. The function operates in several phases:

1. **Reset Phase**: Clears all data structures to ensure a clean state, including:
   - Setting leader's tuple slot to NULL
   - Resetting each worker's tuple buffer (nTuples=0, readCounter=0, done=false)
   - Clearing all output tuple slots
   - Resetting the binary heap to empty

2. **Initial Tuple Collection**: Attempts to read at least one tuple from each source (leader + workers) using a two-pass approach:
   - First pass: Uses nowait mode to quickly gather tuples from ready sources
   - Second pass: Uses wait mode for remaining sources that didn't provide tuples
   - For sources that already have tuples, calls load_tuple_array to check for additional ready tuples

3. **Heap Construction**: After collecting initial tuples, calls binaryheap_build to establish the heap property for ordered merging.

The function uses a goto-based reread loop to handle the transition from nowait to wait mode, ensuring all available sources are properly initialized before beginning the merge process.

## Parameters / Member Variables
- `gm_state`: The GatherMergeState containing all merge-related data structures, worker information, and heap state

## Dependencies
- Functions called/Symbols referenced:
  - castNode
  - [ExecClearTuple](../E/ExecClearTuple.md)
  - [binaryheap_reset](../b/binaryheap_reset.md)
  - TupIsNull
  - [gather_merge_readnext](gather_merge_readnext.md)
  - [binaryheap_add_unordered](../b/binaryheap_add_unordered.md)
  - [load_tuple_array](../l/load_tuple_array.md)
  - [binaryheap_build](../b/binaryheap_build.md)
  - CHECK_FOR_INTERRUPTS
- Called from (representative examples):
  - [gather_merge_getnext](gather_merge_getnext.md)

## Notes and Other Information
- Uses a two-phase reading strategy (nowait then wait) to optimize for responsive sources while ensuring all sources are checked
- The leader process (index 0) doesn't need rechecking in wait mode since nowait behavior doesn't apply to it
- Sources are added to the heap only if they successfully produce a tuple via gather_merge_readnext
- The function sets gm_initialized to true upon completion, preventing redundant initialization
- Includes CHECK_FOR_INTERRUPTS() to handle query cancellation during initialization
- The reread logic ensures that no worker is left uninitialized, which is crucial for correct merge ordering
- Binary heap is built after all initial tuples are collected to establish proper ordering for the merge process