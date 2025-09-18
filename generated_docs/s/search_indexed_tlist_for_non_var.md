# search_indexed_tlist_for_non_var

## Location
src/backend/optimizer/plan/setrefs.c: 2915 - 2954

## Overview
Searches for a non-Var/non-PlaceHolderVar expression in an indexed target list and returns a Var constructed to reference the matching target list item.

## Definition


## Detailed Description
This function searches through an indexed target list to find a non-Var, non-PlaceHolderVar expression that matches the provided node. When a match is found, it constructs and returns a new Var node that references the target list item containing the matching expression. The function uses tlist_member() to perform the matching.

The function includes an optimization where it avoids replacing simple Const nodes with Vars, since a Var is more expensive to execute than a Const. Additionally, replacing Consts could confuse executor components that expect to see simple Consts for specific purposes like dropped columns.

## Parameters / Member Variables
- : The expression node to search for in the indexed target list
- : The indexed target list structure to search within  
- : The varno value to assign to the constructed Var if a match is found

## Dependencies
- Functions called/Symbols referenced:
  - IsA
  - tlist_member
  - makeVarFromTargetEntry
- Data types used:
  - Expr
  - indexed_tlist
  - TargetEntry
  - Const
- Called from (representative examples):
  - fix_scan_list
  - fix_join_expr_mutator
  - fix_upper_expr_mutator
  - fix_windowagg_condition_expr_mutator

## Notes and Other Information
- Returns NULL if no matching expression is found in the indexed target list
- Returns NULL immediately if the input node is a simple Const, to avoid inefficient replacements
- The returned Var has varnosyn and varattnosyn set to 0, indicating it was never a plain Var
- It's recommended to check itlist->has_non_vars before calling this function, as it's a waste of time to call it otherwise
- Part of PostgreSQL's plan tree reference fixing mechanism during query optimization
- Located in src/backend/optimizer/plan/setrefs.c at lines 2915-2954