# tuplesort_reset

## Location
src/backend/utils/sort/tuplesort.c: 1039 - 1071

## Overview
Resets a tuplesort state to prepare it for a new sorting operation while preserving meta-information and resources, allowing efficient reuse of the sort state for multiple small batches.

## Definition
```c
void tuplesort_reset(Tuplesortstate *state)
```

## Detailed Description
The `tuplesort_reset` function provides an efficient way to reuse an existing tuplesort state for multiple sorting operations. Instead of destroying and recreating the entire tuplesort state (which would involve memory allocation/deallocation overhead), this function resets only the data while preserving the meta-information and configuration.

The function performs a three-phase reset process:
1. **Update and Free**: Updates maximum memory usage statistics and frees per-batch memory
2. **Re-initialization**: Sets up common state for the next batch using `tuplesort_begin_batch`
3. **State Cleanup**: Resets tuple and memory slab pointers to their initial state

This approach is particularly beneficial when sorting multiple small batches of data, as it avoids the overhead of repeatedly creating and destroying tuplesort states.

## Parameters / Member Variables
- `state`: Pointer to the Tuplesortstate structure to be reset

## Dependencies
- Functions called/Symbols referenced:
  - `tuplesort_updatemax`: Updates maximum memory usage statistics
  - `tuplesort_free`: Frees per-batch memory resources
  - `tuplesort_begin_batch`: Initializes state for a new batch
  - `Tuplesortstate`: The tuplesort state structure being operated on

- Called from (representative examples):
  - `switchToPresortedPrefixMode` (src/backend/executor/nodeIncrementalSort.c:323)
  - `ExecIncrementalSort` (src/backend/executor/nodeIncrementalSort.c:626)
  - `ExecReScanIncrementalSort` (src/backend/executor/nodeIncrementalSort.c:1149, 1151)

## Notes and Other Information
- This function is primarily used by incremental sort operations where multiple small batches need to be sorted
- The function preserves the tuplesort configuration (comparison functions, sort keys, etc.) while clearing the actual data
- Memory slab pointers (`slabMemoryBegin`, `slabMemoryEnd`, `slabFreeHead`) are reset to NULL, indicating no allocated slab memory
- The `lastReturnedTuple` pointer is also reset to NULL to prepare for the next sort operation
- This optimization is crucial for performance in scenarios involving many small sorts, as it eliminates repeated setup/teardown costs