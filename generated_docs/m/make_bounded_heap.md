# make_bounded_heap

## Location
src/backend/utils/sort/tuplesort.c: 2625 - 2673

## Overview
Converts an unordered array of SortTuples into a bounded heap structure, retaining only the smallest N tuples as specified by the sort's bound limit for efficient LIMIT query processing.

## Definition
```c
static void make_bounded_heap(Tuplesortstate *state)
```

## Detailed Description
This function implements a critical optimization for queries with LIMIT clauses by converting a collection of tuples into a bounded heap that maintains only the top N smallest tuples. The algorithm is based on Knuth's Algorithm 5.2.3H for heap manipulation.

The function operates in reverse sort order, keeping the largest qualifying tuple at the root of the heap. This design choice allows for efficient comparison and replacement: when a new tuple is smaller than the current largest tuple in the heap, the largest can be quickly discarded and replaced.

The process works as follows:
1. **Preparation**: Reverses the sort direction so the largest tuple stays at the root
2. **Initial Population**: Inserts the first `bound` tuples directly into the heap
3. **Selective Replacement**: For remaining tuples, compares each against the root and either discards it (if larger) or replaces the root (if smaller)
4. **Finalization**: Sets the state to TSS_BOUNDED with exactly `bound` tuples remaining

This approach is particularly efficient for queries like "SELECT * FROM table ORDER BY column LIMIT 100" where only the smallest 100 rows are needed from potentially millions of rows.

## Parameters / Member Variables
- `state`: Pointer to Tuplesortstate containing the sort operation with bounded=true and the memtuples array to be converted into a heap

## Dependencies
- Functions called/Symbols referenced:
  - reversedirection (to invert comparison logic)
  - tuplesort_heap_insert (to add tuples to heap)
  - tuplesort_heap_replace_top (to replace largest tuple)
  - free_sort_tuple (to deallocate discarded tuples)
  - COMPARETUP (macro for tuple comparison)
  - SERIAL (macro to check if operation is serial)
  - CHECK_FOR_INTERRUPTS (to allow query cancellation)
- Constants referenced:
  - TSS_INITIAL
  - TSS_BOUNDED
- Called from (representative examples):
  - tuplesort_puttuple_common (when memory limit reached in bounded sort)

## Notes and Other Information
- Only called for bounded sorts (queries with LIMIT clauses)
- Requires state->status to be TSS_INITIAL and state->bounded to be true
- The tuple count must be at least as large as the bound limit
- Operates only on serial (non-parallel) sorts as indicated by SERIAL assertion
- After completion, exactly state->bound tuples remain in the heap
- The reversed direction means the heap maintains the largest valid tuple at root for efficient comparisons
- Memory efficiency: discards tuples that don't qualify for the final result set early
- Implements a classic "top-K" algorithm optimized for database query processing