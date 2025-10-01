# free_sort_tuple

## Location
[src/backend/utils/sort/tuplesort.c:3166-3176](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L3166-L3176)

## Overview
A convenience function that safely frees memory allocated for a tuple that was previously loaded into sort memory during tuplesort operations.

## Definition
```c
static void free_sort_tuple(Tuplesortstate *state, SortTuple *stup)
```

## Detailed Description
This function provides a standardized way to deallocate memory for tuple data that was allocated during sorting operations. It performs memory accounting by updating the sort state's memory usage tracking before actually freeing the memory. The function ensures proper cleanup by setting the tuple pointer to NULL after freeing to prevent double-free errors.

The function is designed to be safe to call multiple times on the same SortTuple, as it checks if the tuple pointer is non-NULL before attempting to free it.

## Parameters / Member Variables
- `state`: Pointer to the Tuplesortstate structure that tracks memory usage and sorting state
- `stup`: Pointer to the SortTuple structure containing the tuple to be freed

## Dependencies
- Functions called/Symbols referenced:
  - FREEMEM (macro to update memory accounting in the sort state)
  - [GetMemoryChunkSpace](../G/GetMemoryChunkSpace.md) (function to get the size of allocated memory chunk)
  - [pfree](../p/pfree.md) (PostgreSQL's memory deallocation function)
  - SortTuple (structure representing a tuple in the sort)
  - [Tuplesortstate](../T/Tuplesortstate.md) (main sorting state structure)

- Called from (representative examples):
  - [tuplesort_puttuple_common](../t/tuplesort_puttuple_common.md) (when handling tuple input)
  - [make_bounded_heap](../m/make_bounded_heap.md) (during bounded heap operations)

## Notes and Other Information
- This function is safe to call multiple times on the same SortTuple due to the NULL check
- Memory accounting is properly maintained through FREEMEM macro before actual deallocation
- The function sets the tuple pointer to NULL after freeing to prevent accidental reuse
- Used internally within tuplesort operations for memory management
- Essential for preventing memory leaks during large sort operations that may exceed available memory

## Simplified Source

```c
static void free_sort_tuple(Tuplesortstate *state, SortTuple *stup) {
    // Only free if tuple pointer is valid
    if (stup->tuple) {
        // Update memory accounting before freeing
        FREEMEM(state, GetMemoryChunkSpace(stup->tuple));

        // Free the tuple memory
        pfree(stup->tuple);

        // Clear pointer to prevent double-free
        stup->tuple = NULL;
    }
}
```