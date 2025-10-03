# ExecSortRestrPos

## Location
[src/backend/executor/nodeSort.c:347-361](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSort.c#L347-L361)

## Overview
Restores the reading position in a sorted result set to a previously marked location set by ExecSortMarkPos.

## Definition

```c
void
ExecSortRestrPos(SortState *node)
```
## Detailed Description
ExecSortRestrPos implements the restore functionality for Sort plan nodes by returning the current reading position to a previously saved location in the sorted tuple stream. This function:

1. **Validation Check**: Verifies that the sort operation has been completed ( is true). If sorting hasn't been performed yet, the function returns early without taking any action.

2. **Position Restoration**: Delegates to the tuplesort module's  function to restore the reading position to the location previously saved by . This allows the sort node to re-read tuples from the marked position onward.

The restore functionality complements ExecSortMarkPos to provide complete mark/restore semantics, enabling query operations that require multiple passes over portions of sorted data. This is particularly useful in nested loop joins where the inner relation needs to be rescanned from a specific position multiple times.

## Parameters / Member Variables
- `*node`: The SortState containing the tuplesort state and sort completion status
## Dependencies
- Functions called/Symbols referenced:
  - : Restore to previously marked position in tuplesort state
- Called from (representative examples):
  - : Generic position restoration interface in the executor

## Notes and Other Information
- Only functional after the initial sort operation has completed (sort_Done == true)
- Requires that the Sort node was initialized with randomAccess enabled (EXEC_FLAG_MARK flag)
- Must be preceded by a call to ExecSortMarkPos to establish a valid restore point
- Works in conjunction with ExecSortMarkPos to provide complete mark/restore semantics
- The underlying tuplesort module manages the actual position restoration mechanism
- No-op if called before sorting is complete, ensuring safe usage regardless of execution state

## Simplified Source

```c
void ExecSortRestrPos(SortState *node)
{
    // Skip if sorting hasn't completed yet
    if (!node->sort_Done)
        return;

    // Restore scan to previously marked position
    tuplesort_restorepos((Tuplesortstate *) node->tuplesortstate);
}
```