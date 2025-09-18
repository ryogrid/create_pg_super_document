# inittapes

## Location
[src/backend/utils/sort/tuplesort.c:1891-1941](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L1891-L1941)

## Overview
Initializes the tape-based external sorting infrastructure when the sort cannot be completed in memory, setting up logical tapes and determining the optimal merge order.

## Definition


## Detailed Description
This static function transitions a tuplesort operation from in-memory sorting to external tape-based sorting when available memory is insufficient. It performs several critical initialization steps:

1. **Merge Order Calculation**: If mergeruns is true, it calls tuplesort_merge_order() to determine the optimal number of tapes based on available memory. For worker processes that may produce single runs, it uses MINORDER (6) tapes.

2. **Tape Infrastructure Setup**: Creates the logical tape set using LogicalTapeSetCreate(), which handles the underlying file management for external sorting. The tape set may be shared among parallel workers if this is part of a parallel sort operation.

3. **State Initialization**: Initializes all tape-related arrays and counters, setting up separate tracking for input and output tapes. The output tapes array is pre-allocated based on maxTapes, while input tapes start as NULL since they'll be populated during merge phases.

4. **Status Transition**: Changes the sort status to TSS_BUILDRUNS and selects the first output tape, signaling that the sort is now ready to begin writing runs to external storage.

## Parameters / Member Variables
- : The Tuplesortstate containing all sort context and configuration
- : Boolean indicating whether this sort will need to merge multiple runs (affects tape count calculation)

## Dependencies
- Functions called/Symbols referenced:
  - LEADER/WORKER (macros for checking parallel sort process roles)
  - [tuplesort_merge_order](../t/tuplesort_merge_order.md) (calculates optimal merge order based on available memory)
  - MINORDER (constant defining minimum merge order for simple cases)
  - inittapestate (initializes internal tape state structures)
  - LogicalTapeSetCreate (creates the underlying tape set for file I/O)
  - selectnewtape (selects the initial output tape for run building)
  - TSS_BUILDRUNS (status constant indicating run building phase)
- Called from (representative examples):
  - tuplesort_puttuple_common (when memory limit is exceeded during tuple insertion)
  - tuplesort_performsort (during sort execution planning)

## Notes and Other Information
- This function is only called when in-memory sorting is determined to be impossible
- The function handles both single-worker and parallel sorting scenarios through the shared fileset mechanism
- Includes conditional debug tracing when TRACE_SORT is enabled to track external sort transitions
- The distinction between mergeruns true/false affects tape count: full merge planning vs. minimal worker setup
- Output tape allocation is done upfront while input tapes are allocated dynamically during merge phases
- The function marks a critical transition point where the sort strategy changes from internal to external