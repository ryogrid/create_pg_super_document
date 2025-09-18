# gather_merge_getnext

## Location
src/backend/executor/nodeGatherMerge.c: 540 - 589

## Overview
Retrieves the next tuple in sorted order from the GatherMerge operation by managing the binary heap and coordinating reads from multiple parallel sources.

## Definition
```c
static TupleTableSlot *gather_merge_getnext(GatherMergeState *gm_state)
```

## Detailed Description
gather_merge_getnext implements the core logic for retrieving sorted tuples from a parallel merge operation. The function operates in two main modes:

1. **Initialization Mode** (first call): When gm_initialized is false, calls gather_merge_init to set up the initial state, including reading the first tuple from each source and building the binary heap.

2. **Streaming Mode** (subsequent calls): 
   - Identifies the source that provided the last returned tuple using binaryheap_first
   - Attempts to read the next tuple from that source using gather_merge_readnext
   - If successful, updates the heap with binaryheap_replace_first to maintain ordering
   - If the source is exhausted, removes it from the heap with binaryheap_remove_first

The function returns the tuple from whichever source currently has the smallest/leading tuple according to the heap's comparison function. When all sources are exhausted (empty heap), it performs cleanup and returns NULL to signal end-of-stream.

## Parameters / Member Variables
- `gm_state`: The GatherMergeState containing heap, tuple slots, and worker management structures

## Dependencies
- Functions called/Symbols referenced:
  - gather_merge_init
  - binaryheap_first
  - DatumGetInt32
  - gather_merge_readnext
  - binaryheap_replace_first
  - binaryheap_remove_first
  - binaryheap_empty
  - gather_merge_clear_tuples
- Called from (representative examples):
  - ExecGatherMerge

## Notes and Other Information
- The function maintains the heap invariant by replacing/removing elements after each tuple consumption
- Uses lazy initialization - the expensive setup only occurs on the first call
- Efficiently handles source exhaustion by removing depleted sources from the heap rather than checking them repeatedly
- The binary heap ensures O(log n) complexity for finding the next minimum tuple across all sources
- Calls gather_merge_clear_tuples when all sources are exhausted to prevent memory leaks
- The heap stores source indices (as Datums) rather than tuples themselves for efficient comparison and management
- Returns TupleTableSlot pointers directly from the gm_slots array, avoiding unnecessary copying
- Critical for maintaining sort order across parallel streams in complex query execution plans