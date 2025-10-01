# tuplesort_heap_replace_top

## Location
[src/backend/utils/sort/tuplesort.c:2836-2875](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L2836-L2875)

## Overview
Replaces the top element of a heap with a new tuple and restores the heap invariant through a sift-down operation.

## Definition
```c
static void tuplesort_heap_replace_top(Tuplesortstate *state, SortTuple *tuple)
```

## Detailed Description
This function implements the "sift-down" or "sift-up" heap operation as described in Knuth's Algorithm 5.2.3H (Heapsort, steps H3-H8). It replaces the root element of the heap with a new tuple and then restores the heap property by moving the new element down the tree until it finds its proper position.

The algorithm works by:
1. Starting at the root position (index 0)
2. Comparing the new tuple with its children
3. If a child is smaller (in a min-heap), moving that child up to create a "hole"
4. Continuing this process down the tree until the proper position is found
5. Placing the new tuple in the final hole position

The function uses unsigned integers to prevent integer overflow in child index calculations (2 * i + 1), which is a defensive programming practice for large heaps. It maintains the heap invariant regardless of whether it's a min-heap or max-heap, depending on the comparison function.

## Parameters / Member Variables
- `state`: Pointer to the Tuplesortstate containing the heap to modify. Must have at least one element (memtupcount >= 1).
- `tuple`: Pointer to the SortTuple that will replace the current top element. The tuple data is copied into the heap structure.

## Dependencies
- Functions called/Symbols referenced:
  - COMPARETUP: Macro that performs tuple comparison using the appropriate comparison function
  - CHECK_FOR_INTERRUPTS: PostgreSQL macro to handle query cancellation and other interrupts
  - SortTuple: Structure representing a sortable tuple
  - [Tuplesortstate](../T/Tuplesortstate.md): Main state structure for sorting operations

- Called from:
  - [tuplesort_puttuple_common](tuplesort_puttuple_common.md): When adding tuples in bounded sort operations
  - [tuplesort_gettuple_common](tuplesort_gettuple_common.md): During tuple retrieval operations
  - [tuplesort_heap_delete_top](tuplesort_heap_delete_top.md): As part of the heap deletion process
  - [mergeonerun](../m/mergeonerun.md): During external sort merge operations
  - [make_bounded_heap](../m/make_bounded_heap.md): When maintaining bounded heap size constraints
  - LEADER: Referenced by parallel sort leader processes

## Notes and Other Information
- Implements Knuth's Algorithm 5.2.3H (Heapsort) steps H3-H8
- Uses unsigned integers to prevent overflow in child index calculations
- The sift-down operation has O(log n) time complexity
- Includes interrupt checking to allow query cancellation during long operations
- Critical for maintaining heap invariant in priority queue operations
- The "hole" concept simplifies the implementation by avoiding unnecessary swaps
- Works with both min-heaps and max-heaps depending on the comparison function
- Essential for bounded sorting where only top-K elements need to be maintained
- The function assumes the heap has at least one element (checked by assertion)

## Simplified Source

```c
static void tuplesort_heap_replace_top(Tuplesortstate *state, SortTuple *tuple) {
    SortTuple *memtuples = state->memtuples;
    unsigned int i = 0;  // Current "hole" position
    unsigned int n = state->memtupcount;

    // Sift down: find proper position for new tuple
    for (;;) {
        unsigned int j = 2 * i + 1;  // Left child index

        // If no children, we found the position
        if (j >= n)
            break;

        // Choose smaller child (min-heap property)
        if (j + 1 < n && COMPARETUP(state, &memtuples[j], &memtuples[j + 1]) > 0)
            j++;  // Right child is smaller

        // If new tuple is in correct position, stop
        if (COMPARETUP(state, tuple, &memtuples[j]) <= 0)
            break;

        // Move smaller child up, creating hole at child position
        memtuples[i] = memtuples[j];
        i = j;
    }

    // Place new tuple in final hole position
    memtuples[i] = *tuple;
}
```