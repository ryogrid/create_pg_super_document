# ExecSortMarkPos

## Location
[src/backend/executor/nodeSort.c:329-346](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSort.c#L329-L346)

## Overview
Marks the current position in a sorted result set, allowing for later restoration to this position via ExecSortRestrPos.

## Definition

```c
void
ExecSortMarkPos(SortState *node)
```
## Detailed Description
ExecSortMarkPos implements the mark/restore functionality for Sort plan nodes by saving the current position in the sorted tuple stream. This function:

1. **Validation Check**: Verifies that the sort operation has been completed ( is true). If sorting hasn't been performed yet, the function returns early without taking any action.

2. **Position Marking**: Delegates to the tuplesort module's  function to save the current reading position in the sorted data. This position can later be restored using  via ExecSortRestrPos.

The mark/restore functionality is essential for certain query operations that need to revisit previously read portions of sorted data, such as nested loop joins where the inner relation needs to be rescanned multiple times from a specific position.

## Parameters / Member Variables
- : The SortState containing the tuplesort state and sort completion status

## Dependencies
- Functions called/Symbols referenced:
  - : Save current position in the tuplesort state
- Called from (representative examples):
  - : Generic position marking interface in the executor

## Notes and Other Information
- Only functional after the initial sort operation has completed (sort_Done == true)
- Requires that the Sort node was initialized with randomAccess enabled (EXEC_FLAG_MARK flag)
- Works in conjunction with ExecSortRestrPos to provide mark/restore semantics
- The underlying tuplesort module handles the actual position tracking mechanism
- No-op if called before sorting is complete, ensuring safe usage in all execution scenarios