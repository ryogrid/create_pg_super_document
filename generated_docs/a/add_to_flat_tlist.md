# add_to_flat_tlist

## Location
[src/backend/optimizer/util/tlist.c:132-162](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/tlist.c#L132-L162)

## Overview
Extends a flattened target list by adding new expressions, avoiding duplicates by checking for existing entries.

## Definition

```c
List *
add_to_flat_tlist(List *tlist, List *exprs)
```
## Detailed Description
The `add_to_flat_tlist` function takes a flattened target list and a list of expressions, then adds each expression to the target list if it doesn't already exist. It uses `tlist_member` to check for duplicates and creates new TargetEntry nodes for unique expressions. Each new entry is assigned a sequential resource number starting from the length of the existing list plus one. The expressions are copied using `copyObject` to ensure proper memory management.

## Parameters / Member Variables
- `tlist`: The existing flattened target list to extend
- `exprs`: A list of expressions to potentially add to the target list

## Dependencies
- Functions called/Symbols referenced:
  - [tlist_member](../t/tlist_member.md) (to check for existing entries)
  - [makeTargetEntry](../m/makeTargetEntry.md) (to create new TargetEntry nodes)
  - copyObject (to copy expressions)
- Called from (representative examples):
  - Referenced in optimizer/tlist.h header

## Notes and Other Information
- Returns the extended target list with new entries appended
- Maintains sequential resource numbering for new entries
- Avoids duplicate expressions by using tlist_member for existence checks
- Creates deep copies of expressions to prevent memory sharing issues
- Part of the target list flattening utilities used in query optimization
- The comment suggests uncertainty about whether copying is needed, indicating potential for optimization

## Simplified Source

```c
List *
add_to_flat_tlist(List *tlist, List *exprs)
{
    int next_resno = list_length(tlist) + 1;
    ListCell *lc;

    // Add each expression to target list if not already present
    foreach(lc, exprs)
    {
        Expr *expr = (Expr *) lfirst(lc);

        // Check for duplicates using tlist_member
        if (!tlist_member(expr, tlist))
        {
            // Create new TargetEntry with sequential resource number
            TargetEntry *tle = makeTargetEntry(copyObject(expr),
                                               next_resno++,
                                               NULL,
                                               false);
            tlist = lappend(tlist, tle);
        }
    }
    return tlist;
}
```