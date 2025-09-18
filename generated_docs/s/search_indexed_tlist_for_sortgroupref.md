# search_indexed_tlist_for_sortgroupref

## Location
src/backend/optimizer/plan/setrefs.c: 2955 - 3032

## Overview
Searches for a sort/group expression in an indexed target list by matching both the expression and its sortgroupref, returning a Var constructed to reference the matching target list item.

## Definition


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
  - equal
  - makeVarFromTargetEntry
- Data types used:
  - Expr
  - Index
  - indexed_tlist
  - ListCell
  - TargetEntry
- Called from (representative examples):
  - fix_scan_list
  - set_upper_references

## Notes and Other Information
- Returns NULL if no matching expression with the correct sortgroupref is found
- The returned Var has varnosyn and varattnosyn set to 0, indicating it was never a plain Var
- The equal() check handles cases in setop plans where ressortgroupref assignment may not perfectly match topmost level sortgrouprefs
- More efficient than search_indexed_tlist_for_non_var for sort/group expressions due to the additional sortgroupref filtering
- Essential for correctly handling multiple textually-equal but volatile sort expressions
- Part of PostgreSQL's plan tree reference fixing mechanism during query optimization
- Located in src/backend/optimizer/plan/setrefs.c at lines 2955-3032