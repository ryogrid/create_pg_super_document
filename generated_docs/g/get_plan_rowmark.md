# get_plan_rowmark

## Location
[src/backend/optimizer/prep/preptlist.c:526-538](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/preptlist.c#L526-L538)

## Overview
Utility function that locates a PlanRowMark structure for a given range table index in a list of row marks, or returns NULL if not found.

## Definition
```c
PlanRowMark *get_plan_rowmark(List *rowmarks, Index rtindex)
```

## Detailed Description
The `get_plan_rowmark` function is a simple utility that searches through a list of PlanRowMark structures to find one that matches a specific range table index (rtindex). PlanRowMark structures are used in PostgreSQL to manage row locking behavior for SELECT FOR UPDATE/SHARE queries and to support EvalPlanQual rechecking in concurrent environments.

The function performs a linear search through the provided list of rowmarks, comparing each PlanRowMark's `rti` (range table index) field against the target `rtindex`. When a match is found, it returns a pointer to that PlanRowMark structure. If no matching entry is found after searching the entire list, the function returns NULL.

This is a helper function that could logically belong in other parts of the codebase, but is placed here due to its current usage patterns in the optimizer.

## Parameters / Member Variables
- `rowmarks`: List of PlanRowMark structures to search through
- `rtindex`: Target range table index to locate

## Dependencies
- Functions called/Symbols referenced:
  - PlanRowMark (structure type)
  - foreach macro (for list iteration)
  - lfirst (list cell access)
- Called from (representative examples):
  - [check_index_predicates](../c/check_index_predicates.md) (src/backend/optimizer/path/indxpath.c:3331)
  - [expand_inherited_rtentry](../e/expand_inherited_rtentry.md) (src/backend/optimizer/util/inherit.c:129)

## Notes and Other Information
This function is located in src/backend/optimizer/prep/preptlist.c:526-538. Despite being a simple utility function, it plays an important role in PostgreSQL's concurrency control and query planning infrastructure. The comment in the source acknowledges that this function "probably ought to be elsewhere", suggesting it might be moved to a more appropriate location in future versions. The function's simplicity makes it efficient for the typical use cases where the rowmarks list is small.