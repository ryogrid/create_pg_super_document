# tuplesort_heap_insert

## Location
[src/backend/utils/sort/tuplesort.c:2777-2811](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L2777-L2811)

## Overview
Inserts a new tuple into a heap data structure while maintaining the heap invariant property through a sift-up operation.

## Definition
```c
static void tuplesort_heap_insert(Tuplesortstate *state, SortTuple *tuple)
```

## Detailed Description
This function implements the standard heap insertion algorithm by adding a new element at the end of the heap and then "sifting up" (also known as "bubbling up") to restore the heap property. The implementation follows Knuth's Algorithm 5.2.3 exercise 16, adapted for 0-based array indexing.

The sift-up process works by:
1. Placing the new tuple at the end of the heap (position memtupcount)
2. Comparing it with its parent node
3. If the new tuple should come before its parent (based on the comparison function), swap them
4. Continue this process up the tree until the heap property is satisfied

The function is designed to work with both min-heaps and max-heaps, depending on the comparison function used by the Tuplesortstate. It's commonly used in bounded sorting operations and merge operations where maintaining a heap of candidate tuples is essential.

## Parameters / Member Variables
- `state`: Pointer to the Tuplesortstate containing the heap. The caller must ensure there's sufficient space (memtupcount < memtupsize).
- `tuple`: Pointer to the SortTuple to be inserted into the heap. The tuple data is copied into the heap structure.

## Dependencies
- Functions called/Symbols referenced:
  - COMPARETUP: Macro that performs tuple comparison using the appropriate comparison function
  - CHECK_FOR_INTERRUPTS: PostgreSQL macro to handle query cancellation and other interrupts
  - SortTuple: Structure representing a sortable tuple
  - Tuplesortstate: Main state structure for sorting operations

- Called from:
  - make_bounded_heap: When building initial bounded heaps for top-K operations
  - beginmerge: During external sort merge operations to populate merge heaps
  - LEADER: Referenced by parallel sort leader processes

## Notes and Other Information
- The caller is responsible for ensuring sufficient space in the memtuples array
- The function includes a safety warning about tuple pointer locations to prevent overwriting
- Uses bit shifting (>> 1) for efficient parent index calculation: parent = (child - 1) / 2
- The heap invariant maintained depends on the comparison function (min-heap or max-heap)
- Increments memtupcount as part of the insertion process
- The sift-up algorithm has O(log n) time complexity
- Includes interrupt checking to allow query cancellation during long operations
- Follows Knuth's algorithms but adapted for 0-based indexing instead of 1-based