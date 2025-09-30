# tuplesort_heap_delete_top

## Location
[src/backend/utils/sort/tuplesort.c:2812-2835](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L2812-L2835)

## Overview
Removes the top element from a heap by replacing it with the last element and performing a sift-down operation to restore the heap property.

## Definition
```c
static void tuplesort_heap_delete_top(Tuplesortstate *state)
```

## Detailed Description
This function implements the standard heap deletion algorithm for removing the root (top) element from a heap data structure. The operation follows the classic approach:

1. Decrements the heap size (memtupcount)
2. If the heap becomes empty, simply returns
3. Takes the last element from the heap
4. Uses it to replace the top element via tuplesort_heap_replace_top
5. The replace function handles the sift-down operation to restore heap invariant

This is a fundamental heap operation used in heap sort algorithms, priority queues, and bounded sorting operations. The function assumes that the caller has already freed any memory associated with the tuple being removed, as it only handles the structural heap reorganization.

The time complexity is O(log n) due to the sift-down operation performed by the replace function.

## Parameters / Member Variables
- `state`: Pointer to the Tuplesortstate containing the heap from which to remove the top element. The function modifies memtupcount and reorganizes the heap structure.

## Dependencies
- Functions called/Symbols referenced:
  - [tuplesort_heap_replace_top](tuplesort_heap_replace_top.md): Replaces the top element with a given tuple and restores heap invariant
  - SortTuple: Structure representing a sortable tuple
  - [Tuplesortstate](../T/Tuplesortstate.md): Main state structure for sorting operations

- Called from:
  - [tuplesort_gettuple_common](tuplesort_gettuple_common.md): When retrieving tuples during result reading
  - [mergeonerun](../m/mergeonerun.md): During external sort merge operations
  - [sort_bounded_heap](../s/sort_bounded_heap.md): When converting bounded heap to sorted array
  - LEADER: Referenced by parallel sort leader processes

## Notes and Other Information
- The caller is responsible for freeing the memory of the deleted tuple before calling this function
- Handles edge case of empty heap (memtupcount <= 0) by returning immediately
- Uses efficient pre-decrement operation to reduce heap size and check for empty condition
- The actual sift-down work is delegated to tuplesort_heap_replace_top for code reuse
- This is a safe operation that maintains heap integrity regardless of the heap's current state
- Commonly used in heap-based algorithms like heapsort and in merge operations for external sorting
- The function does not return the deleted element; caller must retrieve it beforehand if needed

## Simplified Source

```c
static void tuplesort_heap_delete_top(Tuplesortstate *state) {
    SortTuple *memtuples = state->memtuples;
    SortTuple *lastTuple;

    // Decrease heap size; return if heap becomes empty
    if (--state->memtupcount <= 0)
        return;

    // Move last element to replace the top element
    lastTuple = &memtuples[state->memtupcount];
    tuplesort_heap_replace_top(state, lastTuple);
}
```