# tuplesort_free

## Location
src/backend/utils/sort/tuplesort.c: 902 - 970

## Overview
Internal routine responsible for freeing all resources associated with a tuplesort state, including temporary files, memory contexts, and trace information.

## Definition
```c
static void tuplesort_free(Tuplesortstate *state)
```

## Detailed Description
This function performs comprehensive cleanup of a tuplesort operation, releasing all allocated resources. It handles both internal (memory-only) and external (disk-based) sorts, properly closing temporary tape files and resetting memory contexts.

The function switches to the sort's memory context during cleanup to ensure proper memory management, then systematically releases resources including logical tape sets for external sorts. It also handles trace logging when enabled, providing performance statistics about disk usage and execution time.

The cleanup process includes closing any temporary "tape" files used for external sorting, freeing sort-specific state via FREESTATE macro, and finally resetting the entire sort memory context to release all working memory.

## Parameters / Member Variables
- `state`: Pointer to the Tuplesortstate structure to be freed

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextSwitchTo (memory context management)
  - LogicalTapeSetBlocks (get disk usage statistics) 
  - LogicalTapeSetClose (close temporary tape files)
  - SERIAL (macro to check if sort is serial vs parallel)
  - pg_rusage_show (display resource usage statistics)
  - FREESTATE (macro to free sort state structure)
  - MemoryContextReset (reset sort memory context)
  - TRACE_POSTGRESQL_SORT_DONE (tracing probe point)

- Called from (representative examples):
  - tuplesort_end (normal sort completion)
  - tuplesort_reset (sort state reset)
  - LEADER (parallel sort leader cleanup)

## Notes and Other Information
- This is a static internal function not exposed in the public API
- Handles both memory-only and disk-based sort cleanup appropriately
- Includes comprehensive tracing and logging support when TRACE_SORT is enabled
- Individual tapes are not destroyed explicitly as they are cleaned up with the memory context
- Memory context switching ensures cleanup happens in the correct memory context
- Supports both serial and parallel sort cleanup scenarios
- Performance statistics are logged including disk blocks used and execution time
- The function is designed to be safe even if called multiple times on the same state