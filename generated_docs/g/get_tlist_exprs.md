# get_tlist_exprs

## Location
[src/backend/optimizer/util/tlist.c:163-185](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/tlist.c#L163-L185)

## Overview
Extracts just the expression subtrees from a target list, optionally filtering out resjunk columns.

## Definition

```c
List *
get_tlist_exprs(List *tlist, bool includeJunk)
```
## Detailed Description
The `get_tlist_exprs` function takes a target list and extracts only the expression components from each TargetEntry, returning them as a simple list of expressions. It provides an option to include or exclude resjunk columns (auxiliary columns used internally by PostgreSQL but not part of the final result). This utility is useful when you need to work with the expressions themselves rather than the full TargetEntry structures.

## Parameters / Member Variables
- `tlist`: The target list from which to extract expressions
- `includeJunk`: Boolean flag indicating whether to include resjunk columns in the result

## Dependencies
- Functions called/Symbols referenced:
  - [lappend](../l/lappend.md) (implicitly used for list building)
- Called from (representative examples):
  - [build_setop_child_paths](../b/build_setop_child_paths.md)
  - Referenced in optimizer/tlist.h header

## Notes and Other Information
- Returns a new list containing only the expression nodes
- Resjunk columns are skipped by default unless explicitly included
- Useful for transforming target lists into expression lists for further processing
- Does not copy the expressions, so the returned list shares pointers with the original target list
- Common utility in query optimization when working with expression analysis
- Part of the target list manipulation utilities in the optimizer

## Simplified Source

```c
List *
get_tlist_exprs(List *tlist, bool includeJunk)
{
    List *result = NIL;
    ListCell *l;

    // Extract expressions from each target entry
    foreach(l, tlist)
    {
        TargetEntry *tle = (TargetEntry *) lfirst(l);

        // Skip resjunk columns unless explicitly included
        if (tle->resjunk && !includeJunk)
            continue;

        result = lappend(result, tle->expr);
    }
    return result;
}
```