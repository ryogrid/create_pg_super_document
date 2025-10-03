# gather_merge_setup

## Location
[src/backend/executor/nodeGatherMerge.c:388-435](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeGatherMerge.c#L388-L435)

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

## Simplified Source

```c
static void
gather_merge_setup(GatherMergeState *gm_state)
{
    GatherMerge *gm = castNode(GatherMerge, gm_state->ps.plan);
    int nreaders = gm->num_workers;

    // Allocate tuple slots: [0] for leader, [1..n] for workers
    gm_state->gm_slots = (TupleTableSlot **)
        palloc0((nreaders + 1) * sizeof(TupleTableSlot *));

    // Allocate tuple buffers for workers (no buffer for leader)
    gm_state->gm_tuple_buffers = (GMReaderTupleBuffer *)
        palloc0(nreaders * sizeof(GMReaderTupleBuffer));

    // Set up each worker's tuple buffer and slot
    for (int i = 0; i < nreaders; i++) {
        // Allocate tuple array for buffering worker results
        gm_state->gm_tuple_buffers[i].tuple =
            (MinimalTuple *) palloc0(sizeof(MinimalTuple) * MAX_TUPLE_STORE);

        // Create tuple slot for worker (index i+1, since 0 is for leader)
        gm_state->gm_slots[i + 1] =
            ExecInitExtraTupleSlot(gm_state->ps.state, gm_state->tupDesc,
                                   &TTSOpsMinimalTuple);
    }

    // Create binary heap for efficient merging
    gm_state->gm_heap = binaryheap_allocate(nreaders + 1,
                                           heap_compare_slots,
                                           gm_state);
}
```