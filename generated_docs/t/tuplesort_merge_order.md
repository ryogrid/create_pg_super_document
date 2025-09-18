# tuplesort_merge_order

## Location
[src/backend/utils/sort/tuplesort.c:1804-1858](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L1804-L1858)

## Overview
Calculates the optimal merge order (number of input tapes) for the merge phase of external sorting based on available memory, balancing memory usage efficiency with CPU cache performance.

## Definition


## Detailed Description
This function determines the number of input tapes to use during the merge phase of external sorting, which is a critical performance parameter. The merge order directly affects both memory usage and I/O efficiency during the balanced merge algorithm.

The calculation considers that each tape requires TAPE_BUFFER_OVERHEAD bytes plus MERGE_BUFFER_SIZE bytes for workspace. The function uses the formula:

where M is the number of input tapes and N is the number of output tapes (typically M = N).

The function enforces bounds with MINORDER (6) as the minimum merge order even in low memory situations, and MAXORDER (500) as the maximum to prevent CPU cache thrashing. The balance between higher merge orders (which reduce I/O passes) and CPU cache efficiency is crucial for optimal performance.

## Parameters / Member Variables
- : The amount of memory available for tape buffers during the merge phase, specified in bytes

## Dependencies
- Functions called/Symbols referenced:
  - TAPE_BUFFER_OVERHEAD (constant defining buffer overhead per tape)
  - MERGE_BUFFER_SIZE (constant defining workspace per input tape)
  - MINORDER (minimum merge order constant, value 6)
  - MAXORDER (maximum merge order constant, value 500)
  - Max/Min (macros for bound enforcement)
- Called from (representative examples):
  - [cost_tuplesort](../c/cost_tuplesort.md) (query planner cost estimation)
  - [inittapes](../i/inittapes.md) (tape initialization during sort setup)

## Notes and Other Information
- The function is exported for use by the query planner to estimate sorting costs
- Higher merge orders reduce the number of merge passes but can hurt CPU cache performance
- The memtuples[] array is considered part of the MERGE_BUFFER_SIZE workspace in calculations
- Even with abundant memory, very high merge orders can be slower than multi-pass merges due to cache effects
- The balance point considers that additional tapes reduce memory available for building initial runs, potentially requiring more runs overall