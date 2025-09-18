# gather_merge_setup

## Location
src/backend/executor/nodeGatherMerge.c: 388 - 435

## Overview
Sets up the essential data structures needed for GatherMerge operations, including tuple slots, tuple buffers, and a binary heap for efficient merging of parallel worker results.

## Definition
```c
static void gather_merge_setup(GatherMergeState *gm_state)
```

## Detailed Description
gather_merge_setup initializes the core data structures required for parallel tuple merging in GatherMerge nodes. The function allocates memory and sets up three key components:

1. **Tuple Slots Array (gm_slots)**: An array of TupleTableSlot pointers with nreaders+1 entries, where index 0 is reserved for the leader process and indexes 1 to n are for worker processes. Each worker slot is initialized using ExecInitExtraTupleSlot with minimal tuple operations.

2. **Tuple Buffers (gm_tuple_buffers)**: An array of GMReaderTupleBuffer structures for worker processes (indexed 0 to n-1, no entry for leader). Each buffer contains an array of MinimalTuple pointers with MAX_TUPLE_STORE capacity for batched tuple storage from workers.

3. **Binary Heap (gm_heap)**: A binary heap structure that manages the merge order of tuples from different sources (leader + workers) using heap_compare_slots as the comparison function.

The allocation is based on gm->num_workers (upper bound), allowing for fewer actual workers during execution while preventing memory leaks across rescans through the reset-and-reuse approach.

## Parameters / Member Variables
- `gm_state`: The GatherMergeState structure that will be populated with the allocated data structures for merge operations

## Dependencies
- Functions called/Symbols referenced:
  - castNode
  - [palloc0](../p/palloc0.md)
  - [ExecInitExtraTupleSlot](../E/ExecInitExtraTupleSlot.md)
  - [binaryheap_allocate](../b/binaryheap_allocate.md)
  - [heap_compare_slots](../h/heap_compare_slots.md)
- Called from (representative examples):
  - [ExecInitGatherMerge](../E/ExecInitGatherMerge.md)

## Notes and Other Information
- The indexing scheme differs between gm_slots (0 to n, includes leader) and gm_tuple_buffers (0 to n-1, workers only)
- Leader process directly stores tuples in gm_slots[0], while workers use the extra tuple slots created by ExecInitExtraTupleSlot
- Memory is allocated based on the maximum number of workers (upper bound) rather than actual workers to simplify rescan handling
- The binary heap enables efficient O(log n) merge operations across all parallel sources
- MAX_TUPLE_STORE defines the batch size for tuple buffering from worker processes
- Uses minimal tuple format (TTSOpsMinimalTuple) for efficient worker communication