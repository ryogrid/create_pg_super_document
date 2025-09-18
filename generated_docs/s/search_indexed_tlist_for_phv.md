# search_indexed_tlist_for_phv

## Location
src/backend/optimizer/plan/setrefs.c: 2862 - 2914

## Overview
Searches for a PlaceHolderVar in an indexed target list and returns a Var constructed to reference the matching target list item.

## Definition


## Detailed Description
This function searches through an indexed target list to find a PlaceHolderVar that matches the provided PlaceHolderVar by phid (placeholder ID). When a match is found, it constructs and returns a new Var node that references the target list item containing the matching PlaceHolderVar. The matching is performed based on phid only, not using complete equality checks, both for performance reasons and because phnullingrels might not be exactly equal.

The function includes validation of phnullingrels between the input PlaceHolderVar and the subplan output PlaceHolderVar based on the nrm_match parameter, similar to the corresponding validation in search_indexed_tlist_for_var.

## Parameters / Member Variables
- : The PlaceHolderVar to search for in the indexed target list
- : The indexed target list structure to search within
- : The varno value to assign to the constructed Var if a match is found
- : Controls how phnullingrels are compared (NRM_EQUAL for exact match, NRM_SUBSET/NRM_SUPERSET for partial matches)

## Dependencies
- Functions called/Symbols referenced:
  - lfirst
  - IsA
  - [bms_is_subset](../b/bms_is_subset.md)
  - [bms_equal](../b/bms_equal.md)
  - [bmsToString](../b/bmsToString.md)
  - elog
  - makeVarFromTargetEntry
- Data types used:
  - [PlaceHolderVar](../P/PlaceHolderVar.md)
  - [indexed_tlist](../i/indexed_tlist.md)
  - [TargetEntry](../T/TargetEntry.md)
  - NullingRelsMatch
  - ListCell
- Called from (representative examples):
  - fix_scan_list
  - [fix_join_expr_mutator](../f/fix_join_expr_mutator.md)
  - [fix_upper_expr_mutator](../f/fix_upper_expr_mutator.md)

## Notes and Other Information
- Returns NULL if no matching PlaceHolderVar is found in the indexed target list
- The returned Var has varnosyn and varattnosyn set to 0, indicating it was never a plain Var
- It's recommended to check itlist->has_ph_vars before calling this function, as it's a waste of time to call it otherwise
- Matching is performed on phid only, not complete equality, for both performance and correctness reasons
- Part of PostgreSQL's plan tree reference fixing mechanism during query optimization
- Located in src/backend/optimizer/plan/setrefs.c at lines 2862-2914