# search_indexed_tlist_for_sortgroupref

## Location
[src/backend/optimizer/plan/setrefs.c:2955-3032](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L2955-L3032)

## Overview
Searches for a sort/group expression in an indexed target list by matching both the expression and its sortgroupref, returning a Var constructed to reference the matching target list item.

## Definition

```c
union.c assigns ressortgroupref equal to the
		 * column resno without regard to whether that matches the topmost
		 * level's sortgrouprefs and without regard to whether any implicit
		 * coercions are added in the setop tree.  We might have to clean that
		 * up someday;
```
## Detailed Description
This function searches through an indexed target list to find a target entry that matches both the provided expression node and sortgroupref. The matching is performed by comparing the sortgroupref values and using equal() to compare the expressions. This dual matching is essential for ensuring that the correct subplan target list entry is selected in cases where there are multiple textually-equal but volatile sort expressions.

The function is optimized for sort/group operations and is faster than search_indexed_tlist_for_non_var because it uses the sortgroupref as an additional filtering criterion. The equal() check is sometimes redundant but necessary in setop plans where prepunion.c assigns ressortgroupref values that may not match the topmost level's sortgrouprefs.

## Parameters / Member Variables
- : The expression node to search for in the indexed target list
- : The sort/group reference number to match against ressortgroupref
- : The indexed target list structure to search within
- : The varno value to assign to the constructed Var if a match is found

## Dependencies
- Functions called/Symbols referenced:
  - lfirst
  - [equal](../e/equal.md)
  - [makeVarFromTargetEntry](../m/makeVarFromTargetEntry.md)
- Data types used:
  - [Expr](../E/Expr.md)
  - Index
  - [indexed_tlist](../i/indexed_tlist.md)
  - ListCell
  - [TargetEntry](../T/TargetEntry.md)
- Called from (representative examples):
  - fix_scan_list
  - [set_upper_references](set_upper_references.md)

## Notes and Other Information
- Returns NULL if no matching expression with the correct sortgroupref is found
- The returned Var has varnosyn and varattnosyn set to 0, indicating it was never a plain Var
- The equal() check handles cases in setop plans where ressortgroupref assignment may not perfectly match topmost level sortgrouprefs
- More efficient than search_indexed_tlist_for_non_var for sort/group expressions due to the additional sortgroupref filtering
- Essential for correctly handling multiple textually-equal but volatile sort expressions
- Part of PostgreSQL's plan tree reference fixing mechanism during query optimization
- Located in src/backend/optimizer/plan/setrefs.c at lines 2955-3032

## Simplified Source

```c
static Var *
search_indexed_tlist_for_sortgroupref(Expr *node,
                                     Index sortgroupref,
                                     indexed_tlist *itlist,
                                     int newvarno)
{
    ListCell *lc;

    // Search through target list for matching entry
    foreach(lc, itlist->tlist)
    {
        TargetEntry *tle = lfirst(lc);

        // Match both sortgroupref and expression
        // Equal check handles setop plans where ressortgroupref
        // assignment may not perfectly match topmost level
        if (tle->ressortgroupref == sortgroupref &&
            equal(node, tle->expr))
        {
            // Create Var to reference the matching target entry
            Var *newvar = makeVarFromTargetEntry(newvarno, tle);

            // Mark as not originally a plain Var
            newvar->varnosyn = 0;
            newvar->varattnosyn = 0;

            return newvar;
        }
    }

    return NULL;  // No match found
}
```