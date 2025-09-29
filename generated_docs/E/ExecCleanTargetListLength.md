# ExecCleanTargetListLength

## Location
[src/backend/executor/execUtils.c:1119-1137](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execUtils.c#L1119-L1137)

## Overview
Counts the number of items in a target list, excluding any resjunk items that are used for internal executor purposes.

## Definition

```c
int
ExecCleanTargetListLength(List *targetlist)
```
## Detailed Description
This utility function iterates through a PostgreSQL target list and counts only the "clean" target entries - those that represent actual result columns visible to the user. It specifically excludes target entries marked with , which are internal entries used by the executor for various purposes like storing join keys, sort keys, or other intermediate values that should not appear in the final result set.

The function is essential for determining the actual width (number of columns) of query results, which is needed for tuple descriptor creation, result formatting, and various executor operations that need to know the true output schema.

## Parameters / Member Variables
- : A PostgreSQL List containing TargetEntry nodes representing the columns or expressions in a query's target list

## Dependencies
- Functions called/Symbols referenced:
  -  (list iteration macro)
  -  (node type for target list entries)
- Called from (representative examples):
  -  (src/backend/executor/execTuples.c:2051)
  -  (src/backend/executor/functions.c:1717)
  -  (src/backend/rewrite/rewriteHandler.c:1858)
  -  (src/include/executor/executor.h:612)

## Notes and Other Information
- The function only counts TargetEntry nodes where  is false
- Resjunk entries are commonly used for storing intermediate values like join keys, sort keys, or system columns that should not appear in query results
- This count is crucial for creating proper tuple descriptors and determining the actual output width of queries
- The function is located in execUtils.c, which contains various executor utility functions

## Simplified Source

```c
int ExecCleanTargetListLength(List *targetlist)
{
    int len = 0;
    ListCell *tl;

    foreach(tl, targetlist)
    {
        TargetEntry *curTle = lfirst_node(TargetEntry, tl);

        if (!curTle->resjunk)
            len++;
    }
    return len;
}
```