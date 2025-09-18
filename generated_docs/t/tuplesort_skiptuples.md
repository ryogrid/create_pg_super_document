# tuplesort_skiptuples

## Location
src/backend/utils/sort/tuplesort.c: 1736 - 1803

## Overview
A function that efficiently advances over a specified number of tuples in the forward direction without returning the actual tuple data, optimized for scenarios where only positioning is needed.

## Definition


## Detailed Description
This function provides an efficient way to skip over tuples during sort result traversal without the overhead of actually retrieving and processing the tuple data. It handles different sorting states with optimized approaches:

For in-memory sorts (TSS_SORTEDINMEM), it simply advances the current position pointer by the requested number of tuples, which is highly efficient. For tape-based sorts (TSS_SORTEDONTAPE and TSS_FINALMERGE), it falls back to repeatedly calling tuplesort_gettuple_common and discarding the results, which could be optimized in the future.

The function currently only supports forward skipping, though the API is designed to accommodate backward skipping in future implementations. It includes proper bounds checking for limited sorts and handles EOF conditions appropriately.

## Parameters / Member Variables
- : The Tuplesortstate containing the sort context and current position information
- : The number of tuples to skip (must be >= 0, with 0 being a no-op)
- : Direction flag (currently must be true, backward skipping not yet implemented)

## Dependencies
- Functions called/Symbols referenced:
  - WORKER (macro to verify non-worker process context)
  - MemoryContextSwitchTo (switches memory context for temporary operations)
  - tuplesort_gettuple_common (called for tape-based and merge scenarios)
  - CHECK_FOR_INTERRUPTS (allows query cancellation during long operations)
- Called from (representative examples):
  - percentile_disc_final
  - percentile_cont_final_common
  - percentile_disc_multi_final
  - percentile_cont_multi_final_common

## Notes and Other Information
- Currently only supports forward skipping; backward skipping would require additional implementation
- The function is optimized for in-memory sorts but uses a less efficient approach for tape-based sorts
- Used primarily by ordered-set aggregate functions like percentile calculations
- Includes bounds checking for bounded sorts to prevent over-fetching beyond the specified limit
- The tape-based implementation switches memory context to ensure proper cleanup of any temporary allocations