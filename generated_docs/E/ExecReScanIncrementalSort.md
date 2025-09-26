# ExecReScanIncrementalSort

## Location
[src/backend/executor/nodeIncrementalSort.c:1107-1172](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeIncrementalSort.c#L1107-L1172)

## Overview
Resets an incremental sort node to its initial state, clearing all cached tuples and sort states to prepare for a fresh execution scan.

## Definition

```c
void
ExecReScanIncrementalSort(IncrementalSortState *node)
```
## Detailed Description
ExecReScanIncrementalSort resets an incremental sort node for re-execution, which is necessary when query parameters change or when a rescan is explicitly requested. Unlike regular sort nodes that can efficiently rewind through stored results, incremental sort must perform a complete reset because it processes data in batches rather than storing the complete result set.

The function performs a comprehensive reset:
1. Clears all cached tuple slots (result, pivot, and transfer slots)
2. Resets execution state variables to initial values
3. Sets execution status back to INCSORT_LOADFULLSORT (initial loading phase)
4. Resets both fullsort and prefixsort tuplesort states without deallocating them
5. Conditionally rescans the outer child node if parameters haven't changed

The approach of resetting rather than deallocating tuplesort states avoids the overhead of recreating them while ensuring proper cleanup of internal state.

## Parameters / Member Variables
- : The IncrementalSortState to be reset for re-execution

## Dependencies
- Functions called/Symbols referenced:
  - outerPlanState (gets outer child plan state)
  - [ExecClearTuple](ExecClearTuple.md) (clears tuple slots)
  - [tuplesort_reset](../t/tuplesort_reset.md) (resets tuplesort states)
  - [ExecReScan](ExecReScan.md) (rescans outer child node)
- Called from (representative examples):
  - [ExecReScan](ExecReScan.md) (main rescan dispatcher)

## Notes and Other Information
- Incremental sort cannot efficiently support rescan operations because it doesn't store complete result sets like regular sort
- The function resets tuplesort states rather than ending them to avoid setup costs on subsequent execution
- Tuplesort states are only reset if they were previously initialized (non-NULL)
- The outer child node is only rescanned if its parameters haven't changed (chgParam == NULL)
- Even with EXEC_FLAG_REWIND, incremental sort must perform a complete reset rather than efficient rewinding
- The narrow case where efficient rewind might be possible (single batch in full sort) is not currently optimized