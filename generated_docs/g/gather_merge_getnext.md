# gather_merge_getnext

## Location
[src/backend/executor/nodeGatherMerge.c:540-589](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeGatherMerge.c#L540-L589)

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
  - [gather_merge_init](gather_merge_init.md)
  - [binaryheap_first](../b/binaryheap_first.md)
  - [DatumGetInt32](../D/DatumGetInt32.md)
  - [gather_merge_readnext](gather_merge_readnext.md)
  - [binaryheap_replace_first](../b/binaryheap_replace_first.md)
  - [binaryheap_remove_first](../b/binaryheap_remove_first.md)
  - binaryheap_empty
  - [gather_merge_clear_tuples](gather_merge_clear_tuples.md)
- Called from (representative examples):
  - [ExecGatherMerge](../E/ExecGatherMerge.md)

## Notes and Other Information
- The function maintains the heap invariant by replacing/removing elements after each tuple consumption
- Uses lazy initialization - the expensive setup only occurs on the first call
- Efficiently handles source exhaustion by removing depleted sources from the heap rather than checking them repeatedly
- The binary heap ensures O(log n) complexity for finding the next minimum tuple across all sources
- Calls gather_merge_clear_tuples when all sources are exhausted to prevent memory leaks
- The heap stores source indices (as Datums) rather than tuples themselves for efficient comparison and management
- Returns TupleTableSlot pointers directly from the gm_slots array, avoiding unnecessary copying
- Critical for maintaining sort order across parallel streams in complex query execution plans