# extract_update_targetlist_colnos

## Location
[src/backend/optimizer/prep/preptlist.c:348-381](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/preptlist.c#L348-L381)

## Overview
Extracts target table column numbers from an UPDATE targetlist and renumbers the targetlist entries to use sequential numbering convention.

## Definition
```c
List *extract_update_targetlist_colnos(List *tlist)
```

## Detailed Description
The `extract_update_targetlist_colnos` function processes an UPDATE statement's targetlist to extract the column numbers that need to be updated. In PostgreSQL's parser and rewriter, UPDATE targetlist entries use the target table's actual column numbers as resnos. However, the rest of the query planning system expects sequential numbering starting from 1.

This function serves two purposes:
1. Creates a separate list (`update_colnos`) containing the original column numbers that identify which columns are being updated
2. Renumbers all non-resjunk targetlist entries to use sequential numbering (1, 2, 3, etc.)

The function is also used for INSERT ... ON CONFLICT ... UPDATE statements, though this happens later in the planning process. Only non-resjunk (non-auxiliary) targetlist entries are processed, as these represent the actual columns being assigned values.

## Parameters / Member Variables
- `tlist`: List of TargetEntry nodes representing the UPDATE targetlist

## Dependencies
- Functions called/Symbols referenced:
  - [lappend_int](../l/lappend_int.md)
- Called from (representative examples):
  - [preprocess_targetlist](../p/preprocess_targetlist.md) (src/backend/optimizer/prep/preptlist.c:109, 158)
  - [make_modifytable](../m/make_modifytable.md) (src/backend/optimizer/plan/createplan.c:7091)

## Notes and Other Information
This function is located in src/backend/optimizer/prep/preptlist.c:348-381. It's a utility function that handles the conversion between two different numbering conventions used in PostgreSQL: the parser/rewriter convention (using actual column numbers) and the planner/executor convention (using sequential numbers). The returned list of column numbers is essential for the executor to know which table columns to update.

## Simplified Source

```c
List *extract_update_targetlist_colnos(List *tlist)
{
    List *update_colnos = NIL;
    AttrNumber nextresno = 1;
    ListCell *lc;

    // Process each targetlist entry
    foreach(lc, tlist)
    {
        TargetEntry *tle = (TargetEntry *) lfirst(lc);

        // For non-resjunk entries, extract the original column number
        if (!tle->resjunk)
            update_colnos = lappend_int(update_colnos, tle->resno);

        // Renumber to sequential convention (1, 2, 3, ...)
        tle->resno = nextresno++;
    }

    return update_colnos;  // List of target table column numbers
}
```