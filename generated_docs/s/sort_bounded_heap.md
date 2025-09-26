# sort_bounded_heap

## Location
src/backend/utils/sort/tuplesort.c: 2674 - 2713

## Overview
Converts a bounded heap to a properly-sorted array by repeatedly extracting the maximum element and placing it in the correct position, effectively performing heapsort on a bounded heap.

## Definition


## Detailed Description
This function performs the final sorting phase for bounded tuple sorting operations. When PostgreSQL uses a bounded sort (top-K sorting), it maintains a min-heap of at most K elements. Once all input has been processed, this function converts the heap into a sorted array by repeatedly extracting the maximum element (which becomes the minimum in a max-heap due to reversed comparison) and placing it at the end of the array.

The function uses an in-place heapsort algorithm that works by:
1. Extracting the top element (maximum) from the heap
2. Storing it in the newly freed slot at the end of the array
3. Repeating until only one element remains
4. Restoring the original sort direction
5. Marking the state as sorted in memory

This approach is memory-efficient as it requires no additional storage beyond the existing tuple array.

## Parameters / Member Variables
- : Pointer to the Tuplesortstate containing the bounded heap to be sorted. Must be in TSS_BOUNDED status with exactly 'bound' number of tuples.

## Dependencies
- Functions called/Symbols referenced:
  - tuplesort_heap_delete_top: Removes the top element from the heap and re-heapifies
  - reversedirection: Restores the original sort direction after processing
  - TSS_BOUNDED: Status indicating bounded heap mode
  - TSS_SORTEDINMEM: Status indicating tuples are sorted in memory
  - SortTuple: Tuple structure used in sorting operations
  - SERIAL: Macro checking if this is a serial (non-parallel) sort operation

- Called from:
  - tuplesort_performsort: Main sorting orchestration function
  - LEADER: Parallel sort leader process

## Notes and Other Information
- Only works with bounded sorts (top-K operations) where K is relatively small
- Requires the sort state to be in TSS_BOUNDED status with exactly 'bound' tuples
- The function assumes serial execution (no parallel processing)
- Uses reversed direction during heap operations, then restores original direction
- Sets boundUsed flag to true to indicate the bound was actually utilized
- The heapsort is performed in-place for memory efficiency