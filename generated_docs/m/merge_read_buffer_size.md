# merge_read_buffer_size

## Location
[src/backend/utils/sort/tuplesort.c:1859-1890](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L1859-L1890)

## Overview
A static helper function that calculates the optimal amount of memory to allocate for the read buffer of each input tape during a merge pass, based on available memory and tape configuration.

## Definition

```c
static int64
merge_read_buffer_size(int64 avail_mem, int nInputTapes, int nInputRuns,
					   int maxOutputTapes)
```
## Detailed Description
This function determines how to distribute available memory among input tape buffers during the merge phase of external sorting. It first calculates the number of output tapes needed for the current merge pass by dividing input runs among input tapes (rounded up), then constrains this by the maximum allowed output tapes.

The memory allocation strategy reserves TAPE_BUFFER_OVERHEAD bytes for each output tape, then divides all remaining memory evenly among the input tapes. This ensures balanced I/O performance across all input streams while maintaining sufficient buffering for output operations.

The calculation follows the inverse of the formula used in tuplesort_merge_order, deriving input buffer sizes from available memory rather than determining memory requirements from desired buffer sizes. The function ensures a minimum return value of 0 to handle cases where memory is extremely constrained.

## Parameters / Member Variables
- : Total memory available for all tape buffers (both input and output) in bytes
- : Number of input tapes in the current merge pass
- : Total number of input runs to be processed
- : Maximum number of output tapes that should be produced

## Dependencies
- Functions called/Symbols referenced:
  - TAPE_BUFFER_OVERHEAD (constant defining fixed memory overhead per tape)
  - Min (macro for constraining output tapes to maximum allowed)
  - Max (macro for ensuring non-negative buffer size)
- Called from (representative examples):
  - [mergeruns](mergeruns.md) (during merge pass execution)

## Notes and Other Information
- This is a static internal helper function, not exposed in the public API
- The function implements a balanced memory allocation strategy to optimize I/O performance
- Output tape count is calculated as ceiling division: (nInputRuns + nInputTapes - 1) / nInputTapes
- Returns 0 if memory is too constrained to provide meaningful buffer space after accounting for output tape overhead
- The calculation assumes that equal buffer sizes for all input tapes will provide optimal performance
- Memory distribution is designed to work in conjunction with the merge order calculations from tuplesort_merge_order

## Simplified Source

```c
static int64 merge_read_buffer_size(int64 avail_mem, int nInputTapes, int nInputRuns,
                                   int maxOutputTapes) {
    int nOutputRuns;
    int nOutputTapes;

    // Calculate number of output runs needed (ceiling division)
    nOutputRuns = (nInputRuns + nInputTapes - 1) / nInputTapes;

    // Limit output tapes to maximum allowed
    nOutputTapes = Min(nOutputRuns, maxOutputTapes);

    // Reserve memory for output tape overhead, divide remainder among input tapes
    return Max((avail_mem - TAPE_BUFFER_OVERHEAD * nOutputTapes) / nInputTapes, 0);
}
```