# dead_items_cleanup

## Location
src/backend/access/heap/vacuumlazy.c: 2930 - 2954

## Overview
`dead_items_cleanup` is a static cleanup function that performs resource deallocation for vacuum operations, specifically handling parallel vacuum mode termination and cleanup.

## Definition
```c
static void dead_items_cleanup(LVRelState *vacrel)
```

## Detailed Description
This function serves as the cleanup counterpart to resource allocation functions used during VACUUM operations. It primarily handles the termination of parallel vacuum mode when it's active. For non-parallel vacuum operations, the function returns early without performing any cleanup operations since PostgreSQL's memory context system will handle automatic cleanup. When parallel vacuum is active, it properly terminates the parallel vacuum session and cleans up associated resources including index statistics.

The function is designed to be called at the end of vacuum operations to ensure proper resource cleanup and orderly shutdown of parallel processing.

## Parameters / Member Variables
- `vacrel`: Pointer to LVRelState structure containing vacuum operation state and configuration

## Dependencies
- Functions called/Symbols referenced:
  - ParallelVacuumIsActive
  - [parallel_vacuum_end](../p/parallel_vacuum_end.md)
- Called from (representative examples):
  - [heap_vacuum_rel](../h/heap_vacuum_rel.md)

## Notes and Other Information
- This is a static function, only accessible within vacuumlazy.c
- The function explicitly avoids using pfree for non-parallel cases, relying on memory context cleanup
- In parallel mode, it properly terminates parallel workers and cleans up shared state
- Sets `vacrel->pvs` to NULL after cleanup to prevent accidental reuse
- The `vacrel->indstats` parameter passed to parallel_vacuum_end contains index statistics gathered during the vacuum operation