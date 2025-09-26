# ExecReScanSort

## Location
[src/backend/executor/nodeSort.c:362-415](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSort.c#L362-L415)

## Overview
Resets and rescans a Sort node's execution state, either by rewinding to the beginning of previously sorted results or by invalidating the sort and forcing a complete re-sort on the next execution.

## Definition
```c
void ExecReScanSort(SortState *node)
```

## Detailed Description
ExecReScanSort handles the rescan operation for Sort executor nodes. The function intelligently determines whether it can simply rewind to the beginning of previously sorted tuples or whether it needs to invalidate the entire sort state and re-sort from scratch.

The function performs different actions based on the current state:
- If sorting hasn't been completed yet (`!node->sort_Done`), it returns immediately
- If the outer plan has changed parameters, bounded-sort parameters have changed, or random access wasn't selected, it invalidates the sort state and prepares for a complete re-sort
- Otherwise, it simply rewinds to the beginning of the sorted tuple stream using `tuplesort_rescan`

This optimization allows Sort nodes to avoid expensive re-sorting operations when the underlying data hasn't changed and the sort parameters remain the same.

## Parameters / Member Variables
- `node`: Pointer to the SortState containing the Sort node's execution state, including sort completion status, bounded sort parameters, and tuplesort state

## Dependencies
- Functions called/Symbols referenced:
  - outerPlanState (macro to get outer plan state)
  - [ExecClearTuple](ExecClearTuple.md) (clears result tuple slot)
  - [tuplesort_end](../t/tuplesort_end.md) (terminates tuplesort state)
  - [ExecReScan](ExecReScan.md) (rescans outer plan if needed)
  - [tuplesort_rescan](../t/tuplesort_rescan.md) (rewinds tuplesort to beginning)
  - [Tuplesortstate](../T/Tuplesortstate.md) (tuplesort state structure)
- Called from (representative examples):
  - [ExecReScan](ExecReScan.md) (generic rescan dispatcher)

## Notes and Other Information
- The function optimizes performance by avoiding unnecessary re-sorting when possible
- Bounded sort parameter changes (`bounded`, `bound`) force a complete re-sort since the sort criteria may have changed
- The function properly manages memory by ending the previous tuplesort state before invalidating
- Only rescans the outer plan if its change parameters are NULL, otherwise the outer plan will be rescanned automatically on next execution